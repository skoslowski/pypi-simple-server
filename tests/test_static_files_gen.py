from pathlib import Path

import pytest
from starlette.status import HTTP_200_OK, HTTP_404_NOT_FOUND
from starlette.testclient import TestClient

from pypi_simple_server.static_files_gen import _relative_to

CASES = [
    ("/a.py", "/", "a.py"),
    ("/a/b.py", "/", "a/b.py"),
    ("/b.py", "/a", "../b.py"),
    ("/a/b.py", "/c", "../a/b.py"),
    ("a/b.py", "c", "../a/b.py"),
]


@pytest.mark.parametrize(("file", "dir", "expected"), CASES)
def test_relative_to(file: str, dir: str, expected: str):
    assert _relative_to(Path(file), Path(dir)) == Path(expected)


def test_file(client: TestClient):
    # static_files sub-mount need full path
    response = client.get("/pypi/files/09/packaging-24.2-py3-none-any.whl")
    assert response.status_code == HTTP_200_OK
    assert response.content[:4] == b"PK\x03\x04"


def test_file_metadata(client: TestClient):
    response = client.get("/pypi/files/09/packaging-24.2-py3-none-any.whl.metadata")
    assert response.status_code == HTTP_200_OK
    assert response.text.startswith("Metadata-Version: 2.3\n")


def test_file_metadata_missing(client: TestClient):
    response = client.get("/pypi/files/09/packaging-00.0-py3-none-any.whl.metadata")
    assert response.status_code == HTTP_404_NOT_FOUND
