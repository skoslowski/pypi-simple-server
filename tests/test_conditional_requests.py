from email.utils import formatdate

import pytest
from starlette.status import (
    HTTP_200_OK,
    HTTP_304_NOT_MODIFIED,
    HTTP_412_PRECONDITION_FAILED,
)
from starlette.testclient import TestClient

from pypi_simple_server.endpoint_utils import MediaType


@pytest.fixture(scope="module")
def current_headers(client: TestClient):
    r = client.head("/simple/", headers={"Accept": MediaType.JSON_V1})
    assert r.status_code == HTTP_200_OK
    assert "etag" in r.headers
    assert "last-modified" in r.headers
    return r.headers


def test_etag_if_none_match(client: TestClient, current_headers: dict[str, str]):
    r = client.head(
        "/simple/", headers={"Accept": MediaType.JSON_V1, "If-None-Match": current_headers["etag"]}
    )
    assert r.status_code == HTTP_304_NOT_MODIFIED


def test_etag_if_none_match_outdated(client: TestClient):
    r = client.head("/simple/", headers={"Accept": MediaType.JSON_V1, "If-None-Match": "XXX"})
    assert r.status_code == HTTP_200_OK


def test_etag_if_match(client: TestClient, current_headers: dict[str, str]):
    r = client.head("/simple/", headers={"Accept": MediaType.JSON_V1, "If-Match": current_headers["etag"]})
    assert r.status_code == HTTP_200_OK


def test_etag_if_match_outdated(client: TestClient):
    r = client.head("/simple/", headers={"Accept": MediaType.JSON_V1, "If-Match": "XXX"})
    assert r.status_code == HTTP_412_PRECONDITION_FAILED


def test_if_modified_since(client: TestClient, current_headers: dict[str, str]):
    r = client.head(
        "/simple/",
        headers={"Accept": MediaType.JSON_V1, "If-Modified-Since": current_headers["last-modified"]},
    )
    assert r.status_code == HTTP_304_NOT_MODIFIED


def test_if_modified_since_outdated(client: TestClient, current_headers: dict[str, str]):
    r = client.head("/simple/", headers={"Accept": MediaType.JSON_V1, "If-Modified-Since": formatdate(0)})
    assert r.status_code == HTTP_200_OK
