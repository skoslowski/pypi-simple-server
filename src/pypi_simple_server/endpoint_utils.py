import asyncio
from collections.abc import Mapping
from enum import StrEnum
from hashlib import md5
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


class ETagProvider:
    def __init__(self, file: Path) -> None:
        self.file = file
        self.last_changed = 0.0
        self.interval = 1  # second
        self._check_file()
        asyncio.create_task(self.updater())

    async def updater(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            self._check_file()

    def _check_file(self) -> None:
        try:
            self.last_changed = self.file.stat().st_mtime
        except FileNotFoundError:
            pass

    def __str__(self) -> str:
        return md5(str(self.last_changed).encode()).hexdigest()


def handle_etag(request: Request, etag: str | None, weak: bool = True) -> dict[str, str]:
    etag = etag or getattr(request.state, "etag", None)
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
