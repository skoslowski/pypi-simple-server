from enum import StrEnum

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.status import HTTP_406_NOT_ACCEPTABLE


class MediaType(StrEnum):
    JSON_V1 = "application/vnd.pypi.simple.v1+json"
    HTML_V1 = "application/vnd.pypi.simple.v1+html"


_ACCEPTABLE = {
    MediaType.JSON_V1: {
        MediaType.JSON_V1,
        "application/vnd.pypi.simple.latest+json",
    },
    MediaType.HTML_V1: {
        MediaType.HTML_V1,
        "application/vnd.pypi.simple.latest+html",
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
