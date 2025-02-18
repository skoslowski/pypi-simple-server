import logging
from contextlib import asynccontextmanager
from pathlib import Path

from packaging.utils import canonicalize_name
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import PlainTextResponse, RedirectResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.status import HTTP_404_NOT_FOUND
from starlette.templating import Jinja2Templates

from .config import BASE_DIR, CACHE_DIR
from .database import Database
from .requests import MediaType, get_response_media_type
from .responses import SimpleV1HTMLResponse, SimpleV1JSONResponse

logger = logging.getLogger(__name__)
templates = Jinja2Templates(Path(__file__).with_name("templates"))
database = Database(BASE_DIR, CACHE_DIR)


async def index(request: Request) -> Response:
    media_type = get_response_media_type(request)
    index: str = request.path_params.get("index", "")

    project_list = database.get_project_list(index)
    if not project_list.projects:
        raise HTTPException(HTTP_404_NOT_FOUND)

    if media_type == MediaType.HTML_V1:
        return SimpleV1HTMLResponse(request, "index.html", model=project_list)
    return SimpleV1JSONResponse(project_list)


async def detail(request: Request) -> Response:
    media_type = get_response_media_type(request)
    index: str = request.path_params.get("index", "")
    project: str = request.path_params.get("project", "")

    project_canonical = canonicalize_name(project)
    if project != project_canonical:
        return RedirectResponse(
            url=request.url.path.replace(project, project_canonical),
            status_code=301,
        )

    project_details = database.get_project_detail(project_canonical, index)
    if not project_details.files:
        raise HTTPException(HTTP_404_NOT_FOUND)
    for project_file in project_details.files:
        if project_file.url.startswith("http"):
            continue
        project_file.url = str(request.url_for("files", path=project_file.url))

    if media_type == MediaType.HTML_V1:
        return SimpleV1HTMLResponse(request, name="detail.html", model=project_details)
    return SimpleV1JSONResponse(project_details)


async def ping(request: Request) -> PlainTextResponse:
    return PlainTextResponse("")


@asynccontextmanager
async def lifespan(app: Starlette):
    CACHE_DIR.mkdir(exist_ok=True)
    with database:
        database.update()
        yield


static_files = StaticFiles()
static_files.all_directories += [BASE_DIR, CACHE_DIR]

routes = [
    Route("/simple/", endpoint=index),
    Route("/simple/{project}/", endpoint=detail),
    Route("/{index:path}/simple/", endpoint=index),
    Route("/{index:path}/simple/{project}/", endpoint=detail),
    Route("/ping", endpoint=ping),
    Mount("/files", static_files, name="files"),
]

app = Starlette(routes=routes, lifespan=lifespan)
