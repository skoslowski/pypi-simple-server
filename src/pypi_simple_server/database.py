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
        "project" TEXT NOT NULL,
        "filename" TEXT NOT NULL,
        "version" TEXT NOT NULL,
        "file" ProjectFile NOT NULL
    );
    CREATE INDEX IF NOT EXISTS project_lookup ON Distribution(project, "index");
    CREATE UNIQUE INDEX IF NOT EXISTS file_lookup ON Distribution("index", "file");
"""

GET_STATS = """
    SELECT COUNT(*), COUNT(DISTINCT project), COUNT(DISTINCT "index")
    FROM Distribution
"""

LOOKUP_PROJECT_LIST = """
    SELECT DISTINCT "project"
    FROM Distribution
    WHERE "index" GLOB ?
    ORDER BY project
"""

LOOKUP_PROJECT_DETAIL = """
    SELECT version, file AS "file [ProjectFile]"
    FROM Distribution
    WHERE "project" = ? AND "index" GLOB ?
    GROUP BY filename
    HAVING ROWID = MIN(ROWID)
    ORDER BY filename
"""

CHECK_DIST = """
    SELECT COUNT(*)
    FROM Distribution
    WHERE "index" = ? AND "filename" = ?
"""

STORE_DIST = """
    INSERT INTO Distribution VALUES (?, ?, ?, ?, ?)
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
    cache_dir: Path

    @property
    def database_file(self) -> Path:
        return self.cache_dir / "db.sqlite"

    def __enter__(self) -> Self:
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
        project_file_reader = ProjectFileReader(self.files_dir, self.cache_dir)

        for index, file in project_file_reader.iter_files():
            with self._connection as cursor:
                if cursor.execute(CHECK_DIST, (index, file.name)).fetchone()[0]:
                    continue
                try:
                    name, version, dist = project_file_reader.read(file)
                except UnhandledFileTypeError:
                    continue
                except InvalidFileError as e:
                    logger.error(e)
                    continue

                cursor.execute(STORE_DIST, (index, name, file.name, version, dist))

    def get_project_list(self, index: str) -> ProjectList:
        with self._connection as con:
            search_index = f"{index}/*" if index else "*"
            result = con.execute(LOOKUP_PROJECT_LIST, (search_index,))
            return ProjectList(projects=[Project(name) for (name,) in result])

    def get_project_detail(self, project: NormalizedName, index: str) -> ProjectDetail:
        detail = ProjectDetail(name=project)
        with self._connection as con:
            search_index = f"{index}/*" if index else "*"
            result = con.execute(LOOKUP_PROJECT_DETAIL, (project, search_index))
            for version, dist in result:
                detail.versions.append(version)
                detail.files.append(dist)
        detail.versions = sorted(set(detail.versions))
        return detail
