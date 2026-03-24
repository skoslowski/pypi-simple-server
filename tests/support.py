import base64
from pathlib import Path

from pypi_simple import PyPISimple
from starlette.testclient import TestClient

from pypi_simple_server.auth import create_jwt


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
        token, _ = create_jwt(
            user=sub,
            scope=scope,
            secret=self.upload_jwt_secret,
            expires_in=expires_in,
            max_upload_size=max_upload_size,
        )
        raw = f"__token__:{token}".encode()
        return f"Basic {base64.b64encode(raw).decode()}"


def download_files(dist_files: dict[str, list[Path]]) -> None:
    with PyPISimple() as client:
        for project, files in dist_files.items():
            page = client.get_project_page(project)
            packages = {package.filename: package for package in page.packages}
            for file in files:
                if file.exists():
                    continue
                print(f"Downloading {file.name}")
                client.download_package(packages[file.name], path=file)
