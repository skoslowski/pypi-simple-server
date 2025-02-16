import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from pypi_simple_server.database import Database


@pytest.fixture
def database(file_path: Path, tmp_path: Path) -> Iterator[Database]:
    shutil.copytree(file_path, tmp_path, dirs_exist_ok=True)
    tmp_cache_path = tmp_path / ".cache"
    tmp_cache_path.mkdir()
    with Database(tmp_path, tmp_cache_path) as db:
        assert db.stats().distributions == 0
        yield db


@contextmanager
def rename_files(files: list[Path]) -> Iterator[None]:
    renamed = [file.with_name(file.name + "~") for file in files]
    for file, backup in zip(files, renamed):
        file.rename(backup)
    yield
    for backup, file in zip(renamed, files):
        backup.rename(file)


def test_new_index(database: Database, tmp_path: Path):
    rename = list(database.files_dir.rglob("ext/*"))
    with rename_files(rename):
        database.update()
        assert database.stats()[:] == (7, 3, 1)

    database.update()
    assert database.stats()[:] == (11, 4, 2)


def test_new_project(database: Database):
    add_on_2nd_update = list(database.files_dir.rglob("iniconfig*"))
    assert len(add_on_2nd_update) == 3

    with rename_files(add_on_2nd_update):
        database.update()
        assert database.stats()[:] == (8, 3, 2)

    database.update()
    assert database.stats()[:] == (11, 4, 2)
