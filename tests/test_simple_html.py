from textwrap import dedent

import pytest
from starlette.status import HTTP_200_OK
from starlette.testclient import TestClient

from pypi_simple_server.endpoint_utils import MediaType


@pytest.mark.parametrize("content_type", {"", "*/*", "text/html", MediaType.HTML_LATEST, MediaType.HTML_V1})
def test_content_type(client: TestClient, content_type: str):
    headers = {"Accept": content_type} if content_type else {}
    response = client.head("/simple/", headers=headers)
    assert response.headers.get("content-type") == MediaType.HTML_V1
    assert response.status_code == HTTP_200_OK


def test_root_index_html(client: TestClient):
    response = client.get("/simple/")
    assert response.status_code == HTTP_200_OK
    expected = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8" />
            <meta name="pypi:repository-version" content="1.1" />
            <title>Simple Index</title>
        </head>
        <body>
            <a href="iniconfig/">iniconfig</a>
            <a href="packaging/">packaging</a>
            <a href="pluggy/">pluggy</a>
            <a href="pytest/">pytest</a>
        </body>
        </html>
    """
    assert response.text == dedent(expected).strip()


def test_root_project_pytest_html(client: TestClient):
    response = client.get("/simple/pytest/")
    assert response.status_code == HTTP_200_OK
    expected = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8" />
            <meta name="pypi:repository-version" content="1.1" />
            <title>Links for pytest</title>
        </head>
        <body>
            <h1>Links for pytest</h1>
            <a href="http://testserver/pypi/files/pytest-8.3.0-py3-none-any.whl#sha256=a1b30492f2676b476266a87f6551345fb25c0484fb6d09c86aa2eb07b5f71c2f" data-requires-python="&gt;=3.8" data-core_metadata="sha256=cdd29a47b9142b3a3d662c4fa4870139d0c213d3f3853406efc90775b09d06af">pytest-8.3.0-py3-none-any.whl</a><br />
            <a href="http://testserver/pypi/files/pytest-8.3.4-py3-none-any.whl#sha256=50e16d954148559c9a74109af1eaf0c945ba2d8f30f0a3d3335edde19788b6f6" data-requires-python="&gt;=3.8" data-core_metadata="sha256=7f9bf63bf3c20dd4fc7552a8b4708b887cd728c4d2f614ced98b0a43afcfde28">pytest-8.3.4-py3-none-any.whl</a><br />
            <a href="http://testserver/pypi/files/pytest-8.3.4.tar.gz#sha256=965370d062bce11e73868e0335abac31b4d3de0e82f4007408d242b4f8610761" data-requires-python="&gt;=3.8" data-core_metadata="sha256=7f9bf63bf3c20dd4fc7552a8b4708b887cd728c4d2f614ced98b0a43afcfde28">pytest-8.3.4.tar.gz</a><br />
        </body>
        </html>
    """
    assert response.text == dedent(expected).strip()
