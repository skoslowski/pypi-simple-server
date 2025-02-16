from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from pypi_simple import PyPISimple
from starlette.testclient import TestClient

FILES_REQUIRED = [
    "pytest-8.3.4-py3-none-any.whl",
    "pytest-8.3.4.tar.gz",
    "pytest-8.3.0-py3-none-any.whl",
    "iniconfig-2.0.0-py3-none-any.whl",
    "iniconfig-2.0.0.tar.gz",
    "packaging-24.2-py3-none-any.whl",
    "packaging-24.2.tar.gz",
    "ext/pytest-8.3.0-py3-none-any.whl",
    "ext/iniconfig-2.0.0-py3-none-any.whl",
    "ext/pluggy-1.5.0-py3-none-any.whl",
    "ext/pluggy-1.5.0.tar.gz",
]


@pytest.fixture(scope="session")
def file_path() -> Path:
    download_dir = Path(__file__).with_name("files")
    files_missing = {}
    for entry in FILES_REQUIRED:
        file = download_dir / entry
        if file.exists():
            continue
        project = file.name.partition("-")[0]
        files_missing.setdefault(project, []).append(download_dir / file)
    if files_missing:
        download(files_missing)
    download_dir.joinpath("not-a-dist.txt").touch()
    download_dir.joinpath("invalid-dist.tar.gz").touch()
    return download_dir


def download(files_missing: dict[str, list[Path]]) -> None:
    with PyPISimple() as client:
        for project, files in files_missing.items():
            page = client.get_project_page(project)
            packages = {package.filename: package for package in page.packages}
            for file in files:
                if file.exists():
                    continue
                print(f"Downloading {file.name}")
                client.download_package(packages[file.name], path=file)


@pytest.fixture(scope="session")
def client(file_path: Path, tmp_path_factory: pytest.TempPathFactory) -> Iterator[TestClient]:
    from pypi_simple_server import config

    patch_config = mock.patch.object(config, "CACHE_DIR", tmp_path_factory.mktemp("cache"))

    from pypi_simple_server.main import app

    with patch_config, TestClient(app, root_path="/pypi") as client:
        yield client
