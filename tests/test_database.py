import asyncio
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

import msgspec
import pytest

from pypi_simple_server.database import Database, StaticFilesDirGenerator, Stats
from pypi_simple_server.dist_scanner import ProjectFileReader


@dataclass
class Context:
    database: Database
    reader: ProjectFileReader
    files: StaticFilesDirGenerator

    def update(self) -> Stats:
        asyncio.run(self.database.update(self.reader, self.files))
        return self.database.stats()

    @property
    def base_dir(self) -> Path:
        return self.reader.base_dir

    def find_dists(self, pattern: str) -> list[Path]:
        return [f for f in self.reader.base_dir.rglob(pattern) if self.files.directory not in f.parents]

    def list_files(self) -> set[str]:
        return {f.relative_to(self.files.directory).as_posix() for f in self.files.directory.rglob("*.*")}


@pytest.fixture
def context(downloads: Path, tmp_path: Path) -> Iterator[Context]:
    shutil.copytree(downloads, tmp_path, dirs_exist_ok=True)

    files = StaticFilesDirGenerator(tmp_path / "files")
    reader = ProjectFileReader(tmp_path, ignore_dirs={files.directory})

    db_file = tmp_path / ".cache.sqlite"
    with Database(db_file, read_only=False) as db:
        assert db.stats().distributions == 0
        yield Context(db, reader, files)


DEFAULT_STATS = Stats(11, 4, 2)
DEFAULT_FILES = {
    "09/packaging-24.2-py3-none-any.whl.metadata",
    "09/packaging-24.2-py3-none-any.whl",
    "2c/pluggy-1.5.0.tar.gz.metadata",
    "2c/pluggy-1.5.0.tar.gz",
    "2d/iniconfig-2.0.0.tar.gz.metadata",
    "2d/iniconfig-2.0.0.tar.gz",
    "44/pluggy-1.5.0-py3-none-any.whl.metadata",
    "44/pluggy-1.5.0-py3-none-any.whl",
    "50/pytest-8.3.4-py3-none-any.whl.metadata",
    "50/pytest-8.3.4-py3-none-any.whl",
    "96/pytest-8.3.4.tar.gz.metadata",
    "96/pytest-8.3.4.tar.gz",
    "a1/pytest-8.3.0-py3-none-any.whl.metadata",
    "a1/pytest-8.3.0-py3-none-any.whl",
    "b6/iniconfig-2.0.0-py3-none-any.whl.metadata",
    "b6/iniconfig-2.0.0-py3-none-any.whl",
    "c2/packaging-24.2.tar.gz.metadata",
    "c2/packaging-24.2.tar.gz",
}


@contextmanager
def rename_files(files: list[Path]) -> Iterator[None]:
    renamed = [file.with_name(file.name + "~") for file in files]
    for file, backup in zip(files, renamed):
        file.rename(backup)
    yield
    for backup, file in zip(renamed, files):
        backup.rename(file)


def test_static_files(context: Context):
    assert context.update() == DEFAULT_STATS
    assert context.list_files() == DEFAULT_FILES


def test_new_index(context: Context):
    rename = list(context.base_dir.rglob("ext/*"))
    with rename_files(rename):
        assert context.update() == Stats(7, 3, 1)

    assert context.update() == DEFAULT_STATS


def test_new_project(context: Context):
    add_on_2nd_update = context.find_dists("iniconfig*")
    assert len(add_on_2nd_update) == 3

    with rename_files(add_on_2nd_update):
        assert context.update() == Stats(8, 3, 2)

    stats = context.update()
    print(context.database.stats_per_index())
    assert stats == DEFAULT_STATS


def test_removed_dist(context: Context):
    assert context.update() == DEFAULT_STATS

    to_remove = context.find_dists("iniconfig*")
    assert len(to_remove) == 3
    for file in to_remove:
        file.unlink()

    stats = context.update()

    print(context.database.stats_per_index())
    assert stats == Stats(8, 3, 2)
    assert not {f for f in context.list_files() if "iniconfig" in f}


def test_removed_dist_with_subindex(context: Context):
    assert context.update() == DEFAULT_STATS

    links = list(context.files.directory.rglob("iniconfig*.whl"))
    assert len(links) == 1
    initial_target = links[0].resolve()
    initial_target.unlink()

    stats = context.update()

    print(context.database.stats_per_index())
    assert stats == Stats(10, 4, 2)
    assert context.list_files() == DEFAULT_FILES

    links = list(context.files.directory.rglob("iniconfig*.whl"))
    assert len(links) == 1
    assert links[0].readlink() != initial_target


def test_conflicting_dist(context: Context, caplog: pytest.LogCaptureFixture):
    dist_to_patch = context.find_dists("ext/ini*whl")[0]
    expected_stats = msgspec.structs.replace(DEFAULT_STATS, distributions=10)

    with rename_files([dist_to_patch]):
        assert context.update() == expected_stats

    with ZipFile(dist_to_patch, "a") as zip:
        zip.comment += b"XXXX"

    caplog.clear()

    assert context.update() == expected_stats
    assert f"Conflicting distribution {dist_to_patch}" in caplog.text
