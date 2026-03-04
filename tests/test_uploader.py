import base64
import hashlib
from http import HTTPStatus
from io import BytesIO
from pathlib import Path

import pytest
from starlette.datastructures import UploadFile
from starlette.status import (
    HTTP_200_OK,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_415_UNSUPPORTED_MEDIA_TYPE,
)
from starlette.testclient import TestClient

from pypi_simple_server.uploader import UploadError, UploadForm


@pytest.fixture
def upload_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    from pypi_simple_server import uploader

    upload_dir = tmp_path / "uploads"
    monkeypatch.setattr(uploader, "UPLOAD_DIR", upload_dir)
    return upload_dir


def _upload_payload(downloads: Path) -> tuple[dict[str, str], dict[str, tuple[str, bytes, str]], bytes]:
    content = downloads.joinpath("pytest-8.3.4-py3-none-any.whl").read_bytes()
    fields = {
        ":action": "file_upload",
        "protocol_version": "1",
        "filetype": "bdist_wheel",
        "pyversion": "py3",
        "metadata_version": "2.4",
        "name": "pytest",
        "version": "8.3.4",
        "sha256_digest": hashlib.sha256(content).hexdigest(),
    }
    files = {
        "content": (
            "pytest-8.3.4-py3-none-any.whl",
            content,
            "application/octet-stream",
        )
    }
    return fields, files, content


def _upload_sdist_payload(downloads: Path) -> tuple[dict[str, str], dict[str, tuple[str, bytes, str]], bytes]:
    content = downloads.joinpath("pytest-8.3.4.tar.gz").read_bytes()
    fields = {
        ":action": "file_upload",
        "protocol_version": "1",
        "filetype": "sdist",
        "pyversion": "source",
        "metadata_version": "2.4",
        "name": "pytest",
        "version": "8.3.4",
        "sha256_digest": hashlib.sha256(content).hexdigest(),
    }
    files = {
        "content": (
            "pytest-8.3.4.tar.gz",
            content,
            "application/gzip",
        )
    }
    return fields, files, content


def test_legacy_upload_rejects_non_multipart(client: TestClient):
    response = client.post(
        "/legacy/",
        content=b"not multipart",
        headers={"Authorization": client.make_upload_auth(scope=["pytest"])},
    )

    assert response.status_code == HTTP_415_UNSUPPORTED_MEDIA_TYPE
    assert response.json() == {"error": "Expected multipart/form-data"}


def test_legacy_upload_rejects_unauthorized(client: TestClient, downloads: Path, upload_dir: Path):
    fields, files, _ = _upload_payload(downloads)

    response = client.post("/legacy/", data=fields, files=files)

    assert response.status_code == HTTP_401_UNAUTHORIZED
    assert response.json() == {"error": "Unauthorized"}
    assert not upload_dir.exists()


def test_legacy_upload_saves_wheel_and_returns_hashes(client: TestClient, downloads: Path, upload_dir: Path):
    fields, files, content = _upload_payload(downloads)

    response = client.post(
        "/legacy/",
        data=fields,
        files=files,
        headers={"Authorization": client.make_upload_auth(scope=["pytest"])},
    )

    assert response.status_code == HTTP_200_OK
    assert response.json() == {
        "ok": True,
        "name": "pytest",
        "version": "8.3.4",
        "metadata_version": "2.4",
        "filename": "pytest-8.3.4-py3-none-any.whl",
        "bytes": len(content),
        "computed": {
            "sha256_digest": hashlib.sha256(content).hexdigest(),
            "blake2_256_digest": hashlib.blake2b(content, digest_size=32).hexdigest(),
            "md5_digest_urlsafeb64_nopad": base64.urlsafe_b64encode(hashlib.md5(content).digest())
            .decode("ascii")
            .rstrip("="),
        },
    }
    assert upload_dir.joinpath("pytest-8.3.4-py3-none-any.whl").read_bytes() == content


def test_legacy_upload_rejects_project_outside_scope(client: TestClient, downloads: Path, upload_dir: Path):
    fields, files, _ = _upload_payload(downloads)

    response = client.post(
        "/legacy/",
        data=fields,
        files=files,
        headers={"Authorization": client.make_upload_auth(scope=["pluggy"])},
    )

    assert response.status_code == HTTP_400_BAD_REQUEST
    assert response.json() == {
        "ok": False,
        "error": "Not authorized to upload files for this project",
    }
    assert not upload_dir.exists()


def test_legacy_upload_rejects_bad_digest_without_persisting_file(
    client: TestClient, downloads: Path, upload_dir: Path
):
    fields, files, _ = _upload_payload(downloads)
    fields["sha256_digest"] = "0" * 64

    response = client.post(
        "/legacy/",
        data=fields,
        files=files,
        headers={"Authorization": client.make_upload_auth(scope=["pytest"])},
    )

    assert response.status_code == HTTP_400_BAD_REQUEST
    assert response.json() == {
        "ok": False,
        "error": "sha256_hex does not match uploaded file",
    }
    assert not upload_dir.joinpath("pytest-8.3.4-py3-none-any.whl").exists()
    assert not upload_dir.joinpath("pytest-8.3.4-py3-none-any.whl.part").exists()


def test_legacy_upload_rejects_token_max_upload_size(client: TestClient, downloads: Path, upload_dir: Path):
    fields, files, content = _upload_payload(downloads)

    response = client.post(
        "/legacy/",
        data=fields,
        files=files,
        headers={
            "Authorization": client.make_upload_auth(scope=["pytest"], max_upload_size=len(content) - 1)
        },
    )

    assert response.status_code == HTTPStatus.CONTENT_TOO_LARGE
    assert response.json() == {
        "ok": False,
        "error": f"File too large (>{len(content) - 1} bytes)",
    }
    assert not upload_dir.joinpath("pytest-8.3.4-py3-none-any.whl").exists()
    assert not upload_dir.joinpath("pytest-8.3.4-py3-none-any.whl.part").exists()


def test_legacy_upload_saves_sdist_and_returns_hashes(client: TestClient, downloads: Path, upload_dir: Path):
    fields, files, content = _upload_sdist_payload(downloads)

    response = client.post(
        "/legacy/",
        data=fields,
        files=files,
        headers={"Authorization": client.make_upload_auth(scope=["pytest"])},
    )

    assert response.status_code == HTTP_200_OK
    assert response.json() == {
        "ok": True,
        "name": "pytest",
        "version": "8.3.4",
        "metadata_version": "2.4",
        "filename": "pytest-8.3.4.tar.gz",
        "bytes": len(content),
        "computed": {
            "sha256_digest": hashlib.sha256(content).hexdigest(),
            "blake2_256_digest": hashlib.blake2b(content, digest_size=32).hexdigest(),
            "md5_digest_urlsafeb64_nopad": base64.urlsafe_b64encode(hashlib.md5(content).digest())
            .decode("ascii")
            .rstrip("="),
        },
    }
    assert upload_dir.joinpath("pytest-8.3.4.tar.gz").read_bytes() == content


def test_upload_form_prefers_sha256_over_md5(downloads: Path):
    content = downloads.joinpath("pytest-8.3.4-py3-none-any.whl").read_bytes()
    form = UploadForm(
        action="file_upload",
        protocol_version="1",
        filetype="bdist_wheel",
        pyversion="py3",
        metadata_version="2.4",
        name="pytest",
        version="8.3.4",
        content=UploadFile(file=BytesIO(content), filename="pytest-8.3.4-py3-none-any.whl"),
        sha256_digest=hashlib.sha256(content).hexdigest().upper(),
        md5_digest=base64.urlsafe_b64encode(hashlib.md5(content).digest()).decode("ascii").rstrip("="),
    )

    assert form.preferred_digests() == ("sha256_hex", hashlib.sha256(content).hexdigest())


def test_upload_form_rejects_invalid_md5_digest(downloads: Path):
    content = downloads.joinpath("pytest-8.3.4-py3-none-any.whl").read_bytes()
    form = UploadForm(
        action="file_upload",
        protocol_version="1",
        filetype="bdist_wheel",
        pyversion="py3",
        metadata_version="2.4",
        name="pytest",
        version="8.3.4",
        content=UploadFile(file=BytesIO(content), filename="pytest-8.3.4-py3-none-any.whl"),
        md5_digest="abc",
    )

    with pytest.raises(UploadError, match="md5_digest must decode to 16 bytes"):
        form.preferred_digests()


def test_upload_form_rejects_sdist_without_source_pyversion(downloads: Path):
    content = downloads.joinpath("pytest-8.3.4.tar.gz").read_bytes()
    form = UploadForm(
        action="file_upload",
        protocol_version="1",
        filetype="sdist",
        pyversion="py3",
        metadata_version="2.4",
        name="pytest",
        version="8.3.4",
        content=UploadFile(file=BytesIO(content), filename="pytest-8.3.4.tar.gz"),
        sha256_digest=hashlib.sha256(content).hexdigest(),
    )

    with pytest.raises(UploadError, match="pyversion must be 'source' for sdists"):
        form._validate_legacy_fields()


def test_legacy_upload_rejects_expired_jwt(client: TestClient, downloads: Path, upload_dir: Path):
    fields, files, _ = _upload_payload(downloads)

    response = client.post(
        "/legacy/",
        data=fields,
        files=files,
        headers={"Authorization": client.make_upload_auth(scope=["pytest"], expired=True)},
    )

    assert response.status_code == HTTP_401_UNAUTHORIZED
    assert response.json() == {"error": "Unauthorized"}
