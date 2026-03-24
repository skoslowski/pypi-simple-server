import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest

from . import support

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
JWT_SECRET = "upload-secret-0123456789abcdef01"


@pytest.fixture(scope="session")
def downloads() -> Path:
    download_dir = Path(__file__).with_name("data")
    files_missing: dict[str, list[Path]] = {}
    for entry in FILES_REQUIRED:
        file = download_dir / entry
        if file.exists():
            continue
        project = file.name.partition("-")[0]
        files_missing.setdefault(project, []).append(download_dir / file)
    if files_missing:
        support.download_files(files_missing)

    ts = datetime(1111, 11, 11, 11, 11, 11, tzinfo=UTC).timestamp()
    for entry in FILES_REQUIRED:
        if "pytest" in entry:
            os.utime(download_dir / entry, (ts, ts))
    download_dir.joinpath("not-a-dist.txt").touch()
    download_dir.joinpath("invalid-dist.tar.gz").touch()

    return download_dir


@pytest.fixture(scope="session")
def client(downloads: Path, tmp_path_factory: pytest.TempPathFactory) -> Iterator[support.AppClient]:
    from pypi_simple_server import auth, config

    with (
        mock.patch.object(config, "CACHE_FILE", tmp_path_factory.mktemp("cache") / "db.sqlite"),
        mock.patch.object(auth.config, "UPLOAD_JWT_SECRET", JWT_SECRET),
    ):
        from pypi_simple_server.main import app

        with support.AppClient(app, root_path="/pypi") as client:
            client.upload_jwt_secret = JWT_SECRET
            yield client
