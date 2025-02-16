import pytest
from starlette.status import (
    HTTP_200_OK,
    HTTP_301_MOVED_PERMANENTLY,
    HTTP_404_NOT_FOUND,
    HTTP_406_NOT_ACCEPTABLE,
)
from starlette.testclient import TestClient

from pypi_simple_server.endpoint_utils import MediaType


def test_redirect_url(client: TestClient):
    r = client.head("/simple", follow_redirects=False)
    assert r.is_redirect


def test_redirect_name(client: TestClient):
    r = client.head("/simple/PyTest/", follow_redirects=False)
    assert r.status_code, r.headers["Location"] == (HTTP_301_MOVED_PERMANENTLY, "/simple/pytest/")


@pytest.mark.parametrize("content_type", {MediaType.JSON_LATEST, MediaType.JSON_V1})
def test_content_type(client: TestClient, content_type: str):
    r = client.head("/simple/", headers={"Accept": content_type})
    assert r.status_code, r.headers.get("content-type") == (HTTP_200_OK, MediaType.JSON_V1)


def test_content_type_invalid(client: TestClient):
    r = client.head("/simple/", headers={"Accept": MediaType.JSON_V1.value.replace("v1", "v0")})
    assert r.status_code == HTTP_406_NOT_ACCEPTABLE


def test_root_index(client: TestClient):
    r = client.get("/simple/", headers={"Accept": MediaType.JSON_V1})
    assert r.status_code == HTTP_200_OK
    expected = {
        "meta": {"api_version": "1.1"},
        "projects": [
            {"name": "iniconfig"},
            {"name": "packaging"},
            {"name": "pluggy"},
            {"name": "pytest"},
        ],
    }
    assert r.json() == expected


def test_root_project(client: TestClient):
    r = client.get("/simple/pytest/", headers={"Accept": MediaType.JSON_V1})
    assert r.status_code == HTTP_200_OK
    expected = {
        "meta": {"api_version": "1.1"},
        "name": "pytest",
        "versions": ["8.3", "8.3.4"],
        "files": [
            {
                "filename": "pytest-8.3.0-py3-none-any.whl",
                "size": 341630,
                "url": "http://testserver/pypi/files/pytest-8.3.0-py3-none-any.whl",
                "hashes": {"sha256": "a1b30492f2676b476266a87f6551345fb25c0484fb6d09c86aa2eb07b5f71c2f"},
                "requires_python": ">=3.8",
                "core_metadata": {
                    "sha256": "cdd29a47b9142b3a3d662c4fa4870139d0c213d3f3853406efc90775b09d06af"
                },
            },
            {
                "filename": "pytest-8.3.4-py3-none-any.whl",
                "size": 343083,
                "url": "http://testserver/pypi/files/pytest-8.3.4-py3-none-any.whl",
                "hashes": {"sha256": "50e16d954148559c9a74109af1eaf0c945ba2d8f30f0a3d3335edde19788b6f6"},
                "requires_python": ">=3.8",
                "core_metadata": {
                    "sha256": "7f9bf63bf3c20dd4fc7552a8b4708b887cd728c4d2f614ced98b0a43afcfde28"
                },
            },
            {
                "filename": "pytest-8.3.4.tar.gz",
                "size": 1445919,
                "url": "http://testserver/pypi/files/pytest-8.3.4.tar.gz",
                "hashes": {"sha256": "965370d062bce11e73868e0335abac31b4d3de0e82f4007408d242b4f8610761"},
                "requires_python": ">=3.8",
                "core_metadata": {
                    "sha256": "7f9bf63bf3c20dd4fc7552a8b4708b887cd728c4d2f614ced98b0a43afcfde28"
                },
            },
        ],
    }
    assert r.json() == expected


def test_sub_index(client: TestClient):
    r = client.get("/ext/simple/", headers={"Accept": MediaType.JSON_V1})
    assert r.status_code == HTTP_200_OK

    projects = {p["name"] for p in r.json()["projects"]}
    assert projects == {"iniconfig", "pluggy", "pytest"}


def test_sub_project(client: TestClient):
    r = client.get("/ext/simple/pytest/", headers={"Accept": MediaType.JSON_V1})
    assert r.status_code == HTTP_200_OK

    files = {f["filename"] for f in r.json()["files"]}
    assert files == {"pytest-8.3.0-py3-none-any.whl"}


@pytest.mark.parametrize("index", ["ex", "ext/foo"])
def test_missing_sub_index(client: TestClient, index: str):
    r = client.get("/{index}/simple/")
    assert r.status_code == HTTP_404_NOT_FOUND


def test_missing_project(client: TestClient):
    r = client.get("/simple/uv/")
    assert r.status_code == HTTP_404_NOT_FOUND
