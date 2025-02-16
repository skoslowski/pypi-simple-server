from starlette.status import HTTP_200_OK
from starlette.testclient import TestClient


def test_ping(client: TestClient):
    response = client.get("/ping")
    assert response.status_code == HTTP_200_OK
