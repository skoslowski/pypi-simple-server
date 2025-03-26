import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from zipfile import ZipFile

import msgspec
import pytest

from pypi_simple_server.database import Database, Stats
from pypi_simple_server.dist_scanner import ProjectFileReader


@pytest.fixture
def project_file_reader(file_path: Path, tmp_path: Path) -> ProjectFileReader:
    shutil.copytree(file_path, tmp_path, dirs_exist_ok=True)
    return ProjectFileReader(tmp_path)


@pytest.fixture
def database(project_file_reader: ProjectFileReader) -> Iterator[Database]:
    db_file = project_file_reader.files_dir / ".cache.sqlite"
    with Database(db_file, read_only=False) as db:
        assert db.stats().distributions == 0
        yield db


DEFAULT_STATS = Stats(11, 4, 2)


@contextmanager
def rename_files(files: list[Path]) -> Iterator[None]:
    renamed = [file.with_name(file.name + "~") for file in files]
    for file, backup in zip(files, renamed):
        file.rename(backup)
    yield
    for backup, file in zip(renamed, files):
        backup.rename(file)


def test_new_index(project_file_reader: ProjectFileReader, database: Database):
    rename = list(project_file_reader.files_dir.rglob("ext/*"))
    with rename_files(rename):
        database._update(project_file_reader)
        assert database.stats() == Stats(7, 3, 1)

    database._update(project_file_reader)
    assert database.stats() == DEFAULT_STATS


def test_new_project(project_file_reader: ProjectFileReader, database: Database):
    add_on_2nd_update = list(project_file_reader.files_dir.rglob("iniconfig*"))
    assert len(add_on_2nd_update) == 3

    with rename_files(add_on_2nd_update):
        database._update(project_file_reader)
        assert database.stats() == Stats(8, 3, 2)

    database._update(project_file_reader)
    print(database.stats_per_index())
    assert database.stats() == DEFAULT_STATS


def test_removed_dist(project_file_reader: ProjectFileReader, database: Database):
    to_remove = list(project_file_reader.files_dir.rglob("iniconfig*"))
    assert len(to_remove) == 3

    database._update(project_file_reader)
    assert database.stats() == DEFAULT_STATS

    for file in to_remove:
        file.unlink()

    database._update(project_file_reader)
    print(database.stats_per_index())
    assert database.stats() == Stats(8, 3, 2)


def test_conflicting_dist(
    project_file_reader: ProjectFileReader, database: Database, caplog: pytest.LogCaptureFixture
):
    dist_to_patch = next(project_file_reader.files_dir.rglob("ext/ini*whl"))
    expected_stats = msgspec.structs.replace(DEFAULT_STATS, distributions=10)

    with rename_files([dist_to_patch]):
        database._update(project_file_reader)
        assert database.stats() == expected_stats

    with ZipFile(dist_to_patch, "a") as zip:
        zip.comment += b"XXXX"

    caplog.clear()

    database._update(project_file_reader)

    assert database.stats() == expected_stats
    assert f"Conflicting distribution {dist_to_patch}" in caplog.text
