from collections.abc import Mapping
from enum import StrEnum
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


_ACCEPTABLE: dict[MediaType, set[str]] = {
    MediaType.JSON_V1: {
        MediaType.JSON_LATEST,
        MediaType.JSON_V1,
    },
    MediaType.HTML_V1: {
        MediaType.HTML_LATEST,
        MediaType.HTML_V1,
        "text/html",
        "*/*",
    },
}


def get_response_media_type(request: Request) -> MediaType:
    accepts = set(request.headers.get("accept", "*/*").split(","))
    for media_type, acceptable in _ACCEPTABLE.items():
        if acceptable & accepts:
            return media_type
    raise HTTPException(HTTP_406_NOT_ACCEPTABLE)


def handle_etag(request: Request, etag: str, weak: bool = True) -> dict[str, str]:
    headers = {"etag": ("W/" if weak else "") + f'"{etag}"'} if etag else {}

    if (client_etags := request.headers.get("if-none-match")) and etag:
        if any(etag == client_etag.strip(' W/"') for client_etag in client_etags.split(",")):
            raise HTTPException(HTTP_304_NOT_MODIFIED, headers=headers)

    elif client_etag := request.headers.get("if-match"):
        if etag != client_etag.strip(' W/"'):
            raise HTTPException(HTTP_412_PRECONDITION_FAILED, headers=headers)

    return headers


def get_response(
    request: Request,
    headers: Mapping[str, str],
    model: msgspec.Struct,
    template: str,
) -> Response:
    media_type = get_response_media_type(request)
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
