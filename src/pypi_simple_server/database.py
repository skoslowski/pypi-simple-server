import hashlib
import logging
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from tarfile import TarFile
from typing import Self
from zipfile import ZipFile

import msgspec
from packaging.metadata import parse_email
from packaging.utils import (
    NormalizedName,
    canonicalize_name,
    canonicalize_version,
    parse_sdist_filename,
    parse_wheel_filename,
)

from .models import Project, ProjectDetail, ProjectFile, ProjectList

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

    def read(self, file: Path, index: str) -> tuple[NormalizedName, str, ProjectFile]:
        metadata_content = read_project_metadata(file)

        try:
            metadata, _ = parse_email(metadata_content)
            name = canonicalize_name(metadata["name"])  # type: ignore
            version = canonicalize_version(metadata["version"])  # type: ignore
        except Exception as e:
            raise InvalidFileError from e

        dist = ProjectFile(
            filename=file.name,
            size=file.stat().st_size,
            url=f"{index}/{file.name}",
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


BUILD_TABLE = """
    CREATE TABLE IF NOT EXISTS Distribution (
        "index" TEXT NOT NULL,
        "project" TEXT NOT NULL,
        "version" TEXT NOT NULL,
        "file" BLOB NOT NULL
    )
"""

LOOKUP_ROOT_PROJECT = """
    SELECT DISTINCT "project"
    FROM Distribution
    ORDER BY project
"""

LOOKUP_INDEX_PROJECT = """
    SELECT DISTINCT "project"
    FROM Distribution
    WHERE "index" = ?
    ORDER BY project
"""

LOOKUP_ROOT_DETAIL = """
    SELECT version, file
    FROM Distribution
    WHERE "project" = ?
"""

LOOKUP_INDEX_DETAIL = """
    SELECT version, file
    FROM Distribution
    WHERE "project" = ? AND "index" = ?
"""

STORE_DIST = """
    REPLACE INTO Distribution VALUES (?, ?, ?, ?)
"""


@dataclass
class Database:
    files_dir: Path
    cache_dir: Path
    database_url: str = ":memory:"

    def __enter__(self) -> Self:
        self._con = sqlite3.connect(self.database_url)
        closing(self._con.execute(BUILD_TABLE))
        self.update()
        return self

    def __exit__(self, a, b, c):
        self._con.close()

    def update(self) -> None:
        project_file_reader = ProjectFileReader(self.files_dir, self.cache_dir)

        for index, file in project_file_reader.iter_files():
            try:
                name, version, dist = project_file_reader.read(file, index)
                data = msgspec.json.encode(dist)
                with self._con as cur:
                    cur.execute(STORE_DIST, (index, name, version, data))
                    cur.commit()

            except UnhandledFileTypeError:
                continue
            except InvalidFileError as e:
                logger.error(e)
                continue

    def get_project_list(self, index: str) -> ProjectList:
        with self._con as con:
            result = (
                con.execute(LOOKUP_INDEX_PROJECT, (index,)) if index else con.execute(LOOKUP_ROOT_PROJECT)
            )
            return ProjectList(projects=[Project(name) for (name,) in result])

    def get_project_detail(self, project: NormalizedName, index: str) -> ProjectDetail:
        detail = ProjectDetail(name=project)

        with self._con as con:
            result = (
                con.execute(LOOKUP_INDEX_DETAIL, (project, index))
                if index
                else con.execute(LOOKUP_ROOT_DETAIL, (project,))
            )
            for version, data in result:
                detail.versions.add(version)
                file = msgspec.json.decode(data, type=ProjectFile)
                detail.files.add(file)

        return detail
