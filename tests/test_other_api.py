import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from urllib.request import Request as UrlRequest

import pytest
from starlette.status import HTTP_200_OK
from starlette.testclient import TestClient


def test_ping(client: TestClient):
    response = client.get("/ping")
    assert response.status_code == HTTP_200_OK


def test_status(client: TestClient):
    response = client.get("/")
    assert response.status_code == HTTP_200_OK


@pytest.mark.parametrize("fail_request", [False, True], ids=("pass", "fail"))
def test_file_change_purges_after_database_update(fail_request: bool, monkeypatch: pytest.MonkeyPatch):
    from pypi_simple_server import main

    calls = []

    @contextmanager
    def urlopen(request: UrlRequest, timeout: int) -> Generator[None]:
        calls.append((request.full_url, request.method, timeout))
        yield
        if fail_request:
            raise ValueError

    monkeypatch.setattr(main, "INDEX_UPDATED_HOOK_URL", "http://test")
    monkeypatch.setattr(main, "urlopen", urlopen)

    asyncio.run(main._handle_file_change({main.CACHE_FILE}))

    assert calls == [("http://test", "PURGE", main.INDEX_UPDATED_HOOK_TIMEOUT)]
