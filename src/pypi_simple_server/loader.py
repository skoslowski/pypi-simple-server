import hashlib
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from tarfile import TarFile
from zipfile import ZipFile

from packaging.metadata import parse_email
from packaging.utils import (
    canonicalize_name,
    canonicalize_version,
    parse_sdist_filename,
    parse_wheel_filename,
)
from sqlmodel import Session, select

from .database import get_one_or_create
from .models import ProjectDB, ProjectFileDB

logger = logging.getLogger(__name__)


class LoaderError(Exception):
    pass


class UnhandledFileTypeError(LoaderError):
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

    elif file.suffixes[-2:] == [".tar", ".gz"]:
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
        for file in self.files_dir.rglob("*.*"):
            index = file.relative_to(self.files_dir).parent.as_posix().removeprefix(".")
            yield index, file

    def read(self, file: Path, index: str) -> tuple[str, ProjectFileDB]:
        metadata_content = read_project_metadata(file)

        try:
            metadata, _ = parse_email(metadata_content)
            name = canonicalize_name(metadata["name"])  # type: ignore
            version = canonicalize_version(metadata["version"])  # type: ignore
        except Exception as e:
            raise InvalidFileError from e

        dist = ProjectFileDB(
            project_version=version,
            filename=file.name,
            size=file.stat().st_size,
            url=f"{index}/{file.name}",
            hashes=_get_file_hashes(file),
            requires_python=metadata.get("requires_python"),
            core_metadata={"sha256": hashlib.sha256(metadata_content).hexdigest()},
        )

        self.save_metadata(file, metadata_content)
        return name, dist

    def save_metadata(self, file: Path, metadata_content: bytes) -> None:
        metadata_file = self.cache_dir.joinpath(file.relative_to(self.files_dir))
        metadata_file = metadata_file.with_name(file.name + ".metadata")
        metadata_file.parent.mkdir(parents=True, exist_ok=True)
        metadata_file.write_bytes(metadata_content)
        file_stat = file.stat()
        os.utime(metadata_file, (file_stat.st_atime, file_stat.st_mtime))


def update_db(session: Session, files_dir: Path, cache_dir: Path) -> None:
    project_file_reader = ProjectFileReader(files_dir, cache_dir)

    @lru_cache(maxsize=512)
    def project_id(name: str, index: str) -> int:
        project = get_one_or_create(
            session,
            query=select(ProjectDB).where(ProjectDB.index == index).where(ProjectDB.name == name),
            factory=lambda: ProjectDB(index=index, name=name),
        )
        assert project.id is not None
        return project.id

    def create_project_and_distribution() -> ProjectFileDB:
        project_name, distribution = project_file_reader.read(file, index)
        distribution.project_id = project_id(project_name, index)
        return distribution

    for index, file in project_file_reader.iter_files():
        try:
            get_one_or_create(
                session,
                query=(
                    select(ProjectFileDB.id)
                    .where(ProjectDB.id == ProjectFileDB.project_id)
                    .where(ProjectDB.index == index)
                    .where(ProjectFileDB.filename == file.name)
                ),
                factory=create_project_and_distribution,
            )
        except UnhandledFileTypeError:
            continue
        except InvalidFileError as e:
            logger.error(e)
            continue
