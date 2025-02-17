from pathlib import Path

import msgspec
from starlette.requests import Request
from starlette.responses import Response
from starlette.templating import Jinja2Templates

from .requests import MediaType

templates = Jinja2Templates(Path(__file__).with_name("templates"))


class SimpleV1JSONResponse(Response):
    media_type = MediaType.JSON_V1

    def render(self, content: msgspec.Struct) -> bytes:
        return msgspec.json.encode(content)




def SimpleV1HTMLResponse(request: Request, name: str, model: msgspec.Struct) -> Response:
    return templates.TemplateResponse(
        request, name=name, context={"model": model}, media_type=MediaType.HTML_V1
    )
