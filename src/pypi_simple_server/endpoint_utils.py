from collections.abc import Mapping
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

import msgspec
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


def handle_etag(request: Request, etag: str, weak: bool = True) -> dict[str, str]:
    if etag and weak:
        etag = f'W/"{etag}"'

    headers = {"etag": etag} if etag else {}

    if (client_etag := request.headers.get("if-none-match")) and etag == client_etag:
        raise HTTPException(HTTP_304_NOT_MODIFIED, headers=headers)

    elif (client_etag := request.headers.get("if-match")) and etag != client_etag:
        raise HTTPException(HTTP_412_PRECONDITION_FAILED, headers=headers)

    return headers


def get_response(
    request: Request,
    headers: Mapping[str, str],
    model: msgspec.Struct,
    template: str,
) -> Response:
    media_type = get_response_media_type(request.headers.get("accept"))
    headers = {
        **headers,
        "Cache-Control": "max-age=600, public",
        "Vary": "Accept, Accept-Encoding",
    }
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
