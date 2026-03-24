import base64
import binascii
import hashlib
import logging
from collections.abc import Mapping
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Self

import msgspec
from packaging.utils import canonicalize_name, parse_sdist_filename, parse_wheel_filename
from packaging.version import InvalidVersion, Version
from starlette.datastructures import FormData, UploadFile
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.status import (
    HTTP_200_OK,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_413_CONTENT_TOO_LARGE,
    HTTP_415_UNSUPPORTED_MEDIA_TYPE,
)

from .auth import AuthContext
from .config import UPLOAD_DIR, UPLOAD_MAX_BYTES

logger = logging.getLogger(__name__)


async def legacy_upload(request: Request) -> Response:
    ctype = request.headers.get("content-type", "")
    if "multipart/form-data" not in ctype.lower():
        logger.warning("Upload rejected: expected multipart/form-data")
        return JSONResponse(
            {"error": "Expected multipart/form-data"},
            status_code=HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        )

    token = _authenticate(request)
    if token:
        logger.info("Upload authenticated: user=%s token_id=%s", token.user, token.token_id)
    else:
        logger.warning("Upload rejected: unauthorized")
        return JSONResponse({"error": "Unauthorized"}, status_code=HTTP_401_UNAUTHORIZED)

    try:
        m = UploadForm.from_form_data(await request.form())
        hash_type, hash_value = m.preferred_digests()
        logger.info(
            "Upload started: filename=%s name=%s version=%s hash_type=%s",
            m.filename,
            m.name,
            m.version,
            hash_type,
        )

        _require(
            any(fnmatch(m.name, pat) for pat in token.scope),
            "Not authorized to upload files for this project",
        )

        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        dest = UPLOAD_DIR / m.filename
        dest_tmp = dest.with_suffix(dest.suffix + ".part")

        try:
            hashes = await _stream_to_disk_and_hash(
                m.content,
                dest_tmp,
                max_upload_bytes=token.max_upload_size or UPLOAD_MAX_BYTES,
            )
            _require(hashes[hash_type] == hash_value, f"{hash_type} does not match uploaded file")
            dest_tmp.replace(dest)
        finally:
            dest_tmp.unlink(missing_ok=True)

        return JSONResponse(
            {
                "ok": True,
                "name": m.name,
                "version": m.version,
                "metadata_version": m.metadata_version,
                "filename": m.filename,
                "bytes": hashes["bytes"],
                "computed": {
                    "sha256_digest": hashes["sha256_hex"],
                    "blake2_256_digest": hashes["blake2_256_hex"],
                    "md5_digest_urlsafeb64_nopad": base64.urlsafe_b64encode(hashes["md5_raw"])
                    .decode("ascii")
                    .rstrip("="),
                },
            },
            status_code=HTTP_200_OK,
        )

    except UploadError as e:
        logger.warning("Upload failed: %s", e.message)
        return JSONResponse({"ok": False, "error": e.message}, status_code=e.status_code)

    except Exception:
        logger.exception("Upload failed with unexpected error")
        return JSONResponse(
            {"ok": False, "error": "Unexpected error"},
            status_code=HTTP_400_BAD_REQUEST,
        )

    else:
        logger.info("Upload completed: filename=%s bytes=%s", m.filename, hashes["bytes"])


class UploadError(Exception):
    def __init__(self, message: str, status_code: int = HTTP_400_BAD_REQUEST):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _require(cond: bool, msg: str, status: int = HTTP_400_BAD_REQUEST) -> None:
    if not cond:
        raise UploadError(msg, status)


def _authenticate(request: Request) -> AuthContext | None:
    auth = request.headers.get("authorization") or ""
    prefix = "basic "
    if not auth.lower().startswith(prefix):
        return None

    b64 = auth[len(prefix) :].strip()
    try:
        raw = base64.b64decode(b64).decode("utf-8")
    except Exception:
        return None

    username, sep, password = raw.partition(":")
    if not sep or username != "__token__":
        return None

    return AuthContext.from_jwt(token=password)


class UploadForm(msgspec.Struct, frozen=True):
    action: str
    protocol_version: str
    filetype: str
    pyversion: str
    metadata_version: str
    name: str
    version: str

    content: UploadFile

    sha256_digest: str | None = None
    blake2_256_digest: str | None = None
    md5_digest: str | None = None

    @property
    def filename(self) -> str:
        return self.content.filename or ""

    @classmethod
    def from_form_data(cls, form: FormData) -> Self:
        self = cls(
            action=_get_required_str(form, ":action"),
            protocol_version=_get_required_str(form, "protocol_version"),
            filetype=_get_required_str(form, "filetype"),
            pyversion=_get_required_str(form, "pyversion"),
            metadata_version=_get_required_str(form, "metadata_version"),
            name=_get_required_str(form, "name"),
            version=_get_required_str(form, "version"),
            sha256_digest=_get_str(form, "sha256_digest", required=False),
            blake2_256_digest=_get_str(form, "blake2_256_digest", required=False),
            md5_digest=_get_str(form, "md5_digest", required=False),
            content=_get_file(form, "content"),
        )
        self._validate_legacy_fields()
        self._validate_against_filename()
        return self

    def _validate_legacy_fields(self) -> None:
        _require(self.action == "file_upload", "':action' must be 'file_upload'")
        _require(self.protocol_version == "1", "'protocol_version' must be '1'")
        if self.filetype == "bdist_wheel":
            _require(self.pyversion != "source", "pyversion must be a wheel Python tag")
        elif self.filetype == "sdist":
            _require(self.pyversion == "source", "pyversion must be 'source' for sdists")
            _require(self.filename.endswith(".tar.gz"), "sdist uploads must use a .tar.gz filename")
        else:
            raise UploadError("filetype must be 'bdist_wheel' or 'sdist'")

        _require(
            any([self.sha256_digest, self.blake2_256_digest, self.md5_digest]),
            "Provide one of: sha256_digest, blake2_256_digest, md5_digest",
        )

    def _validate_against_filename(self) -> None:
        try:
            if self.filetype == "bdist_wheel":
                dist_name, dist_version, *_ = parse_wheel_filename(self.filename)
                artifact_type = "wheel"
            else:
                dist_name, dist_version = parse_sdist_filename(self.filename)
                artifact_type = "sdist"
        except Exception as e:
            raise UploadError(str(e)) from None

        if canonicalize_name(self.name) != canonicalize_name(dist_name):
            raise UploadError(f"name mismatch: form={self.name!r} {artifact_type}={dist_name!r}")

        try:
            if Version(self.version) != dist_version:
                raise UploadError(
                    f"version mismatch: form={self.version!r} {artifact_type}={str(dist_version)!r}"
                )
        except InvalidVersion:
            raise UploadError(f"invalid version in form field: {self.version!r}")

    def preferred_digests(self) -> tuple[str, str | bytes]:
        """
        Return strongest provided digest as a single-entry dict.

        Priority:
        sha256 > blake2 > md5
        """

        if self.sha256_digest:
            try:
                bytes.fromhex(self.sha256_digest)
            except ValueError:
                raise UploadError("sha256_digest must be valid hex")

            return "sha256_hex", self.sha256_digest.lower()

        if self.blake2_256_digest:
            try:
                bytes.fromhex(self.blake2_256_digest)
            except ValueError:
                raise UploadError("blake2_256_digest must be valid hex")

            return "blake2_256_hex", self.blake2_256_digest.lower()

        if self.md5_digest:
            md5_raw = _urlsafe_b64_no_pad_to_bytes(self.md5_digest)

            _require(len(md5_raw) == 16, "md5_digest must decode to 16 bytes")

            return "md5_raw", md5_raw

        raise UploadError("No digest provided")


def _get_str(form: FormData, key: str, *, required: bool = True) -> str | None:
    v = form.get(key)

    if v is None:
        if required:
            raise UploadError(f"Missing form field: {key}")
        return None

    if isinstance(v, UploadFile):
        raise UploadError(f"Field {key} must be a text field, not a file")

    return str(v)


def _get_required_str(form: FormData, key: str) -> str:
    v = _get_str(form, key, required=True)
    assert v is not None
    return v


def _get_file(form: FormData, key: str) -> UploadFile:
    v = form.get(key)

    if not isinstance(v, UploadFile):
        raise UploadError(f"Missing file field: {key}")

    return v


def _urlsafe_b64_no_pad_to_bytes(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)

    try:
        return base64.urlsafe_b64decode((s + pad).encode("ascii"))
    except (binascii.Error, UnicodeEncodeError) as e:
        raise UploadError(f"Invalid md5_digest encoding: {e}") from None


async def _stream_to_disk_and_hash(
    upload: UploadFile, dest_tmp: Path, *, max_upload_bytes: int
) -> Mapping[str, Any]:
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    blake2 = hashlib.blake2b(digest_size=32)

    total = 0
    with dest_tmp.open("wb") as f:
        while chunk := await upload.read(1024 * 1024):
            total += len(chunk)

            if total > max_upload_bytes:
                raise UploadError(
                    f"File too large (>{max_upload_bytes} bytes)",
                    413 or HTTP_413_CONTENT_TOO_LARGE,
                )

            f.write(chunk)
            md5.update(chunk)
            sha256.update(chunk)
            blake2.update(chunk)

    return {
        "bytes": total,
        "md5_raw": md5.digest(),
        "sha256_hex": sha256.hexdigest(),
        "blake2_256_hex": blake2.hexdigest(),
    }
