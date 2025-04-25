import logging
import queue
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import KW_ONLY, dataclass
from pathlib import Path
from typing import Self

import msgspec
from anyio import CapacityLimiter, to_thread
from packaging.utils import NormalizedName

from .dist_scanner import InvalidFileError, ProjectFileReader, UnhandledFileTypeError
from .models import Project, ProjectDetail, ProjectFile, ProjectList
from .static_files_gen import StaticFilesDirGenerator

logger = logging.getLogger(__name__)

BUILD_TABLE = """
    CREATE TABLE IF NOT EXISTS Distribution (
        "index" TEXT NOT NULL,
        "filename" TEXT NOT NULL,
        "sha256" TEXT NOT NULL,
        "project" TEXT NOT NULL,
        "version" TEXT NOT NULL,
        "file" ProjectFile NOT NULL
    );
    CREATE INDEX IF NOT EXISTS project_lookup ON Distribution("project", "index");
    CREATE UNIQUE INDEX IF NOT EXISTS file_lookup ON Distribution("filename", "index");
"""

GET_STATS = """
    SELECT COUNT(*), COUNT(DISTINCT project), COUNT(DISTINCT "index")
    FROM Distribution
"""

GET_STATS_PER_INDEX = """
    SELECT "index", COUNT(*) as distributions, COUNT(DISTINCT project) as projects
    FROM Distribution
    GROUP BY "index"
"""

GET_PROJECT_LIST = """
    SELECT DISTINCT "project"
    FROM Distribution
    WHERE "index" GLOB ?
    ORDER BY "project"
"""

GET_PROJECT_DETAIL = """
    SELECT "version", "file" AS "file [ProjectFile]"
    FROM Distribution
    WHERE "project" = ? AND "index" GLOB ?
    GROUP BY "filename"
    HAVING ROWID = MIN(ROWID)
    ORDER BY "filename"
"""

CHECK_DIST = """
    SELECT "index", "sha256"
    FROM Distribution
    WHERE "filename" = ?
"""

LIST_DISTS = """
    SELECT "filename", "index", "sha256"
    FROM Distribution
"""

STORE_DIST = """
    INSERT INTO Distribution VALUES (?, ?, ?, ?, ?, ?)
"""

REMOVE_DIST = """
    DELETE FROM Distribution
    WHERE "filename" = ? AND "index" = ?
"""

sqlite3.register_adapter(ProjectFile, msgspec.msgpack.Encoder().encode)
sqlite3.register_converter("ProjectFile", msgspec.msgpack.Decoder(ProjectFile).decode)


class Stats(msgspec.Struct, frozen=True):
    distributions: int
    projects: int
    indexes: int


@dataclass
class Database:
    filepath: Path
    _: KW_ONLY
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

    def stats_per_index(self):
        with self._get_connection() as con:
            cursor = con.execute(GET_STATS_PER_INDEX)
            fields = [column[0] for column in cursor.description]
            return [{key: value for key, value in zip(fields, row)} for row in cursor]

    async def update(
        self, project_file_reader: ProjectFileReader, static_files: StaticFilesDirGenerator
    ) -> None:
        def run() -> None:
            with self._get_connection() as con:
                _add_new(con, project_file_reader, static_files)
                _remove_missing(con, project_file_reader.base_dir, static_files)

            self.filepath.touch(exist_ok=True)

        return await to_thread.run_sync(run, limiter=self._limiter)

    async def get_project_list(self, index: str) -> ProjectList:
        def run() -> ProjectList:
            with self._get_connection() as con:
                cursor = con.execute(GET_PROJECT_LIST, (_path_pattern(index),))
                return ProjectList(projects=[Project(name) for (name,) in cursor])

        return await to_thread.run_sync(run, limiter=self._limiter)

    async def get_project_detail(self, project: NormalizedName, index: str) -> ProjectDetail:
        def run() -> ProjectDetail:
            detail = ProjectDetail(name=project)
            with self._get_connection() as con:
                cursor = con.execute(GET_PROJECT_DETAIL, (project, _path_pattern(index)))
                for version, dist in cursor:
                    detail.versions.append(version)
                    detail.files.append(dist)
            detail.versions = sorted(set(detail.versions))
            return detail

        return await to_thread.run_sync(run, limiter=self._limiter)


def _path_pattern(prefix: str) -> str:
    return f"{prefix}/*" if prefix else "*"


def _add_new(
    con: sqlite3.Connection,
    project_file_reader: ProjectFileReader,
    static_files: StaticFilesDirGenerator,
) -> None:
    for index, file in project_file_reader:
        known: list[tuple[str, str]] = con.execute(CHECK_DIST, (file.name,)).fetchall()
        if any(i == index for i, _ in known):
            continue
        try:
            file_info = project_file_reader.read(file)
        except UnhandledFileTypeError:
            logger.debug("Ignoring %s", file)
            continue
        except InvalidFileError as e:
            logger.exception("Invalid distribution %s: %s", file, e)
            continue

        conflicts = (f"{i}{file.name}" for i, h in known if h != file_info.hash)
        if other := next(conflicts, None):
            logger.error("Conflicting distribution %s: hash conflict with %s", file, other)
            continue

        logger.info("Adding %s", file)
        file_info.dist.url = static_files.add(file, file_info.hash, file_info.metadata)
        parameters = (
            index,
            file.name,
            file_info.hash,
            file_info.project,
            file_info.version,
            file_info.dist,
        )
        con.execute(STORE_DIST, parameters)


def _remove_missing(con: sqlite3.Connection, base_dir: Path, static_files: StaticFilesDirGenerator) -> None:
    remove_dist_parameters = []
    files_to_check = []

    filename: str
    index: str
    hash: str
    for filename, index, hash in con.execute(LIST_DISTS):
        file = base_dir.joinpath(index.rstrip("/"), filename)
        if not file.exists():
            logger.info("Removing %s", file)
            remove_dist_parameters.append((filename, index))
            files_to_check.append((filename, hash))

    con.executemany(REMOVE_DIST, remove_dist_parameters)

    for filename, hash in files_to_check:
        if index_hash := con.execute(CHECK_DIST, (filename,)).fetchone():
            # use file from other index
            file = base_dir.joinpath(index_hash[0].rstrip("/"), filename)
            static_files.update_link(file, hash)
        else:
            static_files.remove(static_files.url_path(filename, hash))
