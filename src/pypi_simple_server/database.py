import logging
import queue
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import msgspec
from anyio import CapacityLimiter, to_thread
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
    filepath: Path
    read_only: bool = True
    max_num_connections: int = 4

    def __post_init__(self):
        self._connections = queue.Queue[sqlite3.Connection]()
        self._limiter = CapacityLimiter(self.max_num_connections)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info):
        self._connections.shutdown()
        try:
            while con := self._connections.get():
                con.close()
        except queue.ShutDown:
            pass  # queue empty

    def _create_connection(self) -> sqlite3.Connection:
        mode = "?mode=ro" if self.read_only else ""
        con = sqlite3.connect(
            f"file://{self.filepath.absolute()}{mode}",
            uri=True,
            detect_types=sqlite3.PARSE_COLNAMES,
            autocommit=False,
            check_same_thread=False,
        )
        if not self.read_only:
            con.executescript(BUILD_TABLE).close()
        return con

    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        con = None
        try:
            try:
                con = self._connections.get_nowait()
            except queue.Empty:
                con = self._create_connection()
            with con:
                yield con
        finally:
            if con is not None:
                self._connections.put(con)

    def stats(self) -> Stats:
        with self._get_connection() as con:
            return Stats(*con.execute(GET_STATS).fetchone())

    def update(self, project_file_reader: ProjectFileReader) -> None:
        with self._get_connection() as con:
            for index, file in project_file_reader:
                if con.execute(CHECK_DIST, (file.name, index)).fetchone()[0]:
                    continue
                try:
                    project, version, dist, metadata = project_file_reader.read(file)
                except UnhandledFileTypeError:
                    continue
                except InvalidFileError as e:
                    logger.error(e)
                    continue

                con.execute(STORE_DIST, (index, file.name, project, version, dist, metadata))

    async def get_project_list(self, index: str) -> ProjectList:

        def run() -> ProjectList:
            with self._get_connection() as con:
                cursor = con.execute(LOOKUP_PROJECT_LIST, (_path_pattern(index),))
                return ProjectList(projects=[Project(name) for (name,) in cursor])

        return await to_thread.run_sync(run, limiter=self._limiter)

    async def get_project_detail(self, project: NormalizedName, index: str) -> ProjectDetail:

        def run() -> ProjectDetail:
            detail = ProjectDetail(name=project)
            with self._get_connection() as con:
                cursor = con.execute(LOOKUP_PROJECT_DETAIL, (project, _path_pattern(index)))
                for version, dist in cursor:
                    detail.versions.append(version)
                    detail.files.append(dist)
            detail.versions = sorted(set(detail.versions))
            return detail

        return await to_thread.run_sync(run, limiter=self._limiter)

    async def get_metadata(self, filename: str, index: str) -> bytes | None:
        def run() -> bytes | None:
            with self._get_connection() as con:
                result = con.execute(LOOKUP_METADATA, (filename, _path_pattern(index))).fetchone()
            return result[0] if result else None

        return await to_thread.run_sync(run, limiter=self._limiter)


def _path_pattern(prefix: str) -> str:
    return f"{prefix}/*" if prefix else "*"
