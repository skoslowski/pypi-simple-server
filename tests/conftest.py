import base64
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest
from pypi_simple import PyPISimple
from starlette.testclient import TestClient

from pypi_simple_server.auth import create_jwt

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


class AppClient(TestClient):
    upload_jwt_secret: str

    def make_upload_auth(
        self,
        *,
        scope: list[str],
        sub: str = "ci",
        expires_in: int | None = None,
        expired: bool = False,
        max_upload_size: int | None = None,
    ) -> str:
        if expired:
            expires_in = -300
        token = create_jwt(
            user=sub,
            scope=scope,
            secret=self.upload_jwt_secret,
            expires_in=expires_in,
            max_upload_size=max_upload_size,
        )
        raw = f"__token__:{token}".encode()
        return f"Basic {base64.b64encode(raw).decode()}"


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
        _download(files_missing)

    ts = datetime(1111, 11, 11, 11, 11, 11, tzinfo=UTC).timestamp()
    for entry in FILES_REQUIRED:
        if "pytest" in entry:
            os.utime(download_dir / entry, (ts, ts))
    download_dir.joinpath("not-a-dist.txt").touch()
    download_dir.joinpath("invalid-dist.tar.gz").touch()

    return download_dir


def _download(files_missing: dict[str, list[Path]]) -> None:
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
def client(downloads: Path, tmp_path_factory: pytest.TempPathFactory) -> Iterator[AppClient]:
    from pypi_simple_server import auth, config

    with (
        mock.patch.object(config, "CACHE_FILE", tmp_path_factory.mktemp("cache") / "db.sqlite"),
        mock.patch.object(auth.config, "UPLOAD_JWT_SECRET", JWT_SECRET),
    ):
        from pypi_simple_server.main import app

        with AppClient(app, root_path="/pypi") as client:
            client.upload_jwt_secret = JWT_SECRET
            yield client
