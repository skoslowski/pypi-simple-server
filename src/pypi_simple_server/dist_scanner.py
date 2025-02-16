import hashlib
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from tarfile import TarFile
from zipfile import ZipFile

from packaging.metadata import parse_email
from packaging.utils import (
    NormalizedName,
    canonicalize_name,
    canonicalize_version,
    parse_sdist_filename,
    parse_wheel_filename,
)

from .models import ProjectFile

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


def _get_file_hashes(filename: Path, blocksize: int = 2 << 13) -> dict[str, str]:
    hash_obj = hashlib.sha256()
    with open(filename, "rb") as fp:
        while fb := fp.read(blocksize):
            hash_obj.update(fb)
    return {hash_obj.name: hash_obj.hexdigest()}


@dataclass
class ProjectFileReader:
    files_dir: Path
    cache_dir: Path

    def iter_files(self) -> Iterator[tuple[str, Path]]:
        for root, _, files in os.walk(self.files_dir):
            root_dir = Path(root)
            if root_dir == self.cache_dir:
                continue
            index = f"{root_dir.relative_to(self.files_dir).as_posix()}/".lstrip(".")
            for file in files:
                yield index, root_dir / file

    def read(self, file: Path) -> tuple[NormalizedName, str, ProjectFile]:
        try:
            metadata_content = read_project_metadata(file)
            metadata, _ = parse_email(metadata_content)
            name = canonicalize_name(metadata["name"])  # type: ignore
            version = canonicalize_version(metadata["version"])  # type: ignore
        except ProjectReaderError:
            raise
        except Exception as e:
            raise InvalidFileError from e

        dist = ProjectFile(
            filename=file.name,
            size=file.stat().st_size,
            url=file.relative_to(self.files_dir).as_posix(),
            hashes=_get_file_hashes(file),
            requires_python=metadata.get("requires_python"),
            core_metadata={"sha256": hashlib.sha256(metadata_content).hexdigest()},
        )

        self.save_metadata(file, metadata_content)
        return name, version, dist

    def save_metadata(self, file: Path, metadata_content: bytes) -> None:
        metadata_file = self.cache_dir.joinpath(file.relative_to(self.files_dir))
        metadata_file = metadata_file.with_name(file.name + ".metadata")
        metadata_file.parent.mkdir(parents=True, exist_ok=True)
        metadata_file.write_bytes(metadata_content)
        file_stat = file.stat()
        os.utime(metadata_file, (file_stat.st_atime, file_stat.st_mtime))
