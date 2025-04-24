from starlette.status import HTTP_200_OK, HTTP_404_NOT_FOUND
from starlette.testclient import TestClient


def test_ping(client: TestClient):
    response = client.get("/ping")
    assert response.status_code == HTTP_200_OK


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


def test_status(client: TestClient):
    response = client.get("/")
    assert response.status_code == HTTP_200_OK
