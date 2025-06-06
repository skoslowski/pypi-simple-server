import asyncio
import hashlib
import logging
import os
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import KW_ONLY, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from tarfile import TarFile
from time import time
from zipfile import ZipFile

import watchfiles
from packaging.metadata import parse_email
from packaging.utils import (
    NormalizedName,
    canonicalize_name,
    canonicalize_version,
    parse_sdist_filename,
    parse_wheel_filename,
)

from .models import Hashes, ProjectFile

logger = logging.getLogger(__name__)


class ProjectReaderError(Exception):
    pass


class UnhandledFileTypeError(ProjectReaderError):
    pass


class InvalidFileError(ValueError):
    pass


def read_project_metadata(file: Path) -> bytes:
    if file.suffix == ".whl":
        parse_wheel_filename(file.name)
        # https://packaging.python.org/en/latest/specifications/binary-distribution-format/
        distribution, version, _ = file.name.split("-", 2)
        subdir = f"{distribution}-{version}.dist-info"
        with ZipFile(file) as zip, zip.open(f"{subdir}/METADATA") as fp:
            return fp.read()

    elif file.name.endswith(".tar.gz"):
        parse_sdist_filename(file.name)
        # https://packaging.python.org/en/latest/specifications/source-distribution-format/
        subdir = file.name.removesuffix(".tar.gz")
        with TarFile.open(file) as tar_file:
            pkg_info = tar_file.extractfile(f"{subdir}/PKG-INFO")
            assert pkg_info
            with pkg_info as fp:
                return fp.read()

    raise UnhandledFileTypeError(f"Can't handle type {file.name}")


def _get_file_hashes(filename: Path, blocksize: int = 2 << 13) -> Hashes:
    hash_obj = hashlib.sha256()
    with open(filename, "rb") as fp:
        while fb := fp.read(blocksize):
            hash_obj.update(fb)
    return Hashes(sha256=hash_obj.hexdigest())


@dataclass(frozen=True, slots=True)
class FileResult:
    project: NormalizedName
    version: str
    hash: str
    dist: ProjectFile
    metadata: bytes


@dataclass
class ProjectFileReader:
    base_dir: Path
    _: KW_ONLY
    ignore_dirs: set[Path] = field(default_factory=set)

    def __iter__(self) -> Iterator[tuple[str, Path]]:
        for root, _, files in os.walk(self.base_dir):
            root_dir = Path(root)
            if not self.ignore_dirs.isdisjoint(root_dir.parents):
                continue
            index = f"{root_dir.relative_to(self.base_dir).as_posix()}/".lstrip(".")
            for file in files:
                yield index, root_dir / file

    def read(self, file: Path) -> FileResult:
        try:
            metadata_content = read_project_metadata(file)
            metadata, _ = parse_email(metadata_content)
            project = canonicalize_name(metadata["name"])  # type: ignore
            version = canonicalize_version(metadata["version"])  # type: ignore
        except ProjectReaderError:
            raise
        except Exception as e:
            raise InvalidFileError(file) from e

        hashes = _get_file_hashes(file)

        dist = ProjectFile(
            filename=file.name,
            size=file.stat().st_size,
            url="",  # filled later
            hashes=hashes,
            requires_python=metadata.get("requires_python"),
            core_metadata=Hashes(sha256=hashlib.sha256(metadata_content).hexdigest()),
            upload_time=_format_time(file.stat().st_mtime),
        )
        return FileResult(project, version, hashes.sha256, dist, metadata_content)


def _format_time(timestamp: float) -> str:
    """generate a ISO 8601 / RFC 3339 from timestamp"""
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class FileWatcher:
    watch_dir: Path
    callback: Callable[[set[Path]], Awaitable[None]]
    _: KW_ONLY
    ignore: set[Path] = field(default_factory=set)
    quiet_time: int = 10

    def __post_init__(self) -> None:
        self._next_callback_time: float | None = None
        self._files_changed: set[Path] = set()
        self._watch_task = asyncio.create_task(self._run_watch())

    async def _run_watch(self) -> None:
        async for changes in watchfiles.awatch(self.watch_dir.absolute(), watch_filter=self._watch_filter):
            if not self._next_callback_time:
                logger.info("File watch detected changes (quiet time %ss)", self.quiet_time)
                asyncio.create_task(self._run_callback())
            self._next_callback_time = time() + self.quiet_time
            self._files_changed.update(Path(s) for c, s in changes)

    def _watch_filter(self, change: watchfiles.Change, file: str) -> bool:
        return self.ignore.isdisjoint(Path(file).parents)

    async def _run_callback(self) -> None:
        try:
            while not self._next_callback_time or time() < self._next_callback_time:
                await asyncio.sleep(1)
                continue

            files_changed = set(self._files_changed)
            self._files_changed.clear()

            logger.info("File watch reporting %d changed files", len(files_changed))
            try:
                await self.callback(files_changed)
            except Exception as e:
                logger.exception("File watch callback failed: %s", e)
        finally:
            self._next_callback_time = None
