import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import msgspec
from packaging.utils import NormalizedName

from .dist_scanner import InvalidFileError, ProjectFileReader, UnhandledFileTypeError
from .models import Project, ProjectDetail, ProjectFile, ProjectList

logger = logging.getLogger(__name__)

BUILD_TABLE = """
    CREATE TABLE IF NOT EXISTS Distribution (
        "index" TEXT NOT NULL,
        "filename" TEXT NOT NULL,
        "project" TEXT NOT NULL,
        "version" TEXT NOT NULL,
        "file" ProjectFile NOT NULL,
        "metadata" BLOB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS project_lookup ON Distribution("project", "index");
    CREATE UNIQUE INDEX IF NOT EXISTS file_lookup ON Distribution("filename", "index");
"""

GET_STATS = """
    SELECT COUNT(*), COUNT(DISTINCT project), COUNT(DISTINCT "index")
    FROM Distribution
"""

LOOKUP_PROJECT_LIST = """
    SELECT DISTINCT "project"
    FROM Distribution
    WHERE "index" GLOB ?
    ORDER BY "project"
"""

LOOKUP_PROJECT_DETAIL = """
    SELECT "version", "file" AS "file [ProjectFile]"
    FROM Distribution
    WHERE "project" = ? AND "index" GLOB ?
    GROUP BY "filename"
    HAVING ROWID = MIN(ROWID)
    ORDER BY "filename"
"""

LOOKUP_METADATA = """
    SELECT "metadata"
    FROM Distribution
    WHERE "filename" = ? AND "index" GLOB ?
"""

CHECK_DIST = """
    SELECT COUNT(*)
    FROM Distribution
    WHERE "filename" = ? AND "index" = ?
"""

STORE_DIST = """
    INSERT INTO Distribution VALUES (?, ?, ?, ?, ?, ?)
"""

sqlite3.register_adapter(ProjectFile, msgspec.msgpack.Encoder().encode)
sqlite3.register_converter("ProjectFile", msgspec.msgpack.Decoder(ProjectFile).decode)


class Stats(msgspec.Struct, frozen=True):
    distributions: int
    projects: int
    indexes: int

    def __getitem__(self, index: int | slice) -> int | tuple[int, ...]:
        return msgspec.structs.astuple(self)[index]

    # def __len__(self) -> int:
    #     return len(msgspec.structs.fields(self))


@dataclass
class Database:
    files_dir: Path
    database_file: Path

    def __enter__(self) -> Self:
        self.database_file.parent.mkdir(exist_ok=True, parents=True)
        self._connection = sqlite3.connect(
            self.database_file,
            detect_types=sqlite3.PARSE_COLNAMES,
            autocommit=False,
            check_same_thread=False,
        )
        self._connection.executescript(BUILD_TABLE).close()
        return self

    def __exit__(self, *exc_info):
        self._connection.close()

    def stats(self) -> Stats:
        with self._connection as cur:
            return Stats(*cur.execute(GET_STATS).fetchone())

    def update(self) -> None:
        project_file_reader = ProjectFileReader(self.files_dir)

        for index, file in project_file_reader.iter_files():
            with self._connection as cursor:
                if cursor.execute(CHECK_DIST, (file.name, index)).fetchone()[0]:
                    continue
                try:
                    project, version, dist, metadata = project_file_reader.read(file)
                except UnhandledFileTypeError:
                    continue
                except InvalidFileError as e:
                    logger.error(e)
                    continue

                cursor.execute(STORE_DIST, (index, file.name, project, version, dist, metadata))

    def get_project_list(self, index: str) -> ProjectList:
        with self._connection as con:
            result = con.execute(LOOKUP_PROJECT_LIST, (_path_pattern(index),))
            return ProjectList(projects=[Project(name) for (name,) in result])

    def get_project_detail(self, project: NormalizedName, index: str) -> ProjectDetail:
        detail = ProjectDetail(name=project)
        with self._connection as cursor:
            result = cursor.execute(LOOKUP_PROJECT_DETAIL, (project, _path_pattern(index)))
            for version, dist in result:
                detail.versions.append(version)
                detail.files.append(dist)
        detail.versions = sorted(set(detail.versions))
        return detail

    def get_metadata(self, filename: str, index: str) -> bytes:
        with self._connection as cursor:
            result = cursor.execute(LOOKUP_METADATA, (filename, _path_pattern(index))).fetchone()
        return result[0]


def _path_pattern(prefix: str) -> str:
    return f"{prefix}/*" if prefix else "*"
