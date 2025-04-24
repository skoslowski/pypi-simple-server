from collections.abc import Mapping
from email.utils import formatdate, parsedate
from enum import StrEnum
from functools import lru_cache
from hashlib import md5
from pathlib import Path

import msgspec
from starlette.datastructures import Headers, MutableHeaders
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response
from starlette.status import (
    HTTP_304_NOT_MODIFIED,
    HTTP_406_NOT_ACCEPTABLE,
    HTTP_412_PRECONDITION_FAILED,
)
from starlette.templating import Jinja2Templates

templates = Jinja2Templates(Path(__file__).with_name("templates"))


class MediaType(StrEnum):
    JSON_V1 = "application/vnd.pypi.simple.v1+json"
    HTML_V1 = "application/vnd.pypi.simple.v1+html"

    JSON_LATEST = "application/vnd.pypi.simple.latest+json"
    HTML_LATEST = "application/vnd.pypi.simple.latest+html"


_ACCEPTABLE: dict[str, MediaType] = {
    MediaType.JSON_LATEST: MediaType.JSON_V1,
    MediaType.JSON_V1: MediaType.JSON_V1,
    MediaType.HTML_LATEST: MediaType.HTML_V1,
    MediaType.HTML_V1: MediaType.HTML_V1,
    "text/html": MediaType.HTML_V1,
    "text/*": MediaType.HTML_V1,
    "*/*": MediaType.HTML_V1,
}


def _parse_accept_entry(value: str) -> tuple[float, int, str]:
    type_, _, q_factor = value.strip().partition(";q=")
    try:
        priority = max(0.0, min(1.0, float(q_factor)))
    except Exception:
        priority = 1.0
    specificity = 0 if type_ == "*/*" else 1 if type_.endswith("/*") else 0
    return priority, specificity, type_


@lru_cache
def get_response_media_type(accept_header: str | None) -> MediaType:
    """https://packaging.python.org/en/latest/specifications/simple-repository-api/#version-format-selection"""
    accepts = list(_parse_accept_entry(mt) for mt in (accept_header or "*/*").split(","))
    for *_, accept in sorted(accepts, reverse=True):
        if media_type := _ACCEPTABLE.get(accept):
            return media_type
    raise HTTPException(HTTP_406_NOT_ACCEPTABLE)


class ResponseHeaders(MutableHeaders):
    def update_changed(self, mtime: float) -> None:
        self["last-modified"] = formatdate(mtime, usegmt=True)
        etag = f'"{md5(str(mtime).encode(), usedforsecurity=False).hexdigest()}"'
        self["etag"] = f"W/{etag}"


def handle_conditional_request(request_headers: Headers, response_headers: Headers) -> None:
    try:
        if_none_match = request_headers["if-none-match"]
        etag = response_headers["etag"].removeprefix("W/")
        if etag in [tag.strip(" W/") for tag in if_none_match.split(",")]:
            raise HTTPException(HTTP_304_NOT_MODIFIED, headers=response_headers)
    except KeyError:
        pass

    try:
        if_modified_since = parsedate(request_headers["if-modified-since"])
        last_modified = parsedate(response_headers["last-modified"])
        if if_modified_since is not None and last_modified is not None and if_modified_since >= last_modified:
            raise HTTPException(HTTP_304_NOT_MODIFIED, headers=response_headers)
    except KeyError:
        pass

    try:
        if_match = request_headers["if-match"]
        etag = response_headers["etag"].removeprefix("W/")
        if etag != if_match.strip("W/"):
            raise HTTPException(HTTP_412_PRECONDITION_FAILED, headers=response_headers)
    except KeyError:
        pass


def get_response(
    request: Request,
    headers: Mapping[str, str],
    model: msgspec.Struct,
    template: str,
) -> Response:
    media_type = get_response_media_type(request.headers.get("accept"))
    if media_type == MediaType.HTML_V1:
        return templates.TemplateResponse(
            request,
            template,
            context={"model": model},
            headers=headers,
            media_type=MediaType.HTML_V1,
        )
    else:
        return Response(
            msgspec.json.encode(model),
            headers=headers,
            media_type=MediaType.JSON_V1,
        )
