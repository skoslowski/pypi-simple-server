import logging
from contextlib import asynccontextmanager

from packaging.utils import canonicalize_name
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import PlainTextResponse, RedirectResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.status import HTTP_404_NOT_FOUND

from .config import BASE_DIR, CACHE_DIR
from .database import Database
from .endpoint_utils import ETagProvider, get_response, handle_etag

logger = logging.getLogger(__name__)
database = Database(BASE_DIR, CACHE_DIR)


async def index(request: Request) -> Response:
    headers = handle_etag(request, None)
    index: str = request.path_params.get("index", "")

    project_list = await run_in_threadpool(database.get_project_list, index)
    if not project_list.projects:
        raise HTTPException(HTTP_404_NOT_FOUND)

    return get_response(request, headers, project_list, "index.html")


async def detail(request: Request) -> Response:
    headers = handle_etag(request, None)
    index: str = request.path_params.get("index", "")
    project_raw: str = request.path_params.get("project", "")

    project = canonicalize_name(project_raw)
    if project_raw != project:
        url = request.url.path.replace(project_raw, project)
        return RedirectResponse(url, status_code=301)

    project_details = await run_in_threadpool(database.get_project_detail, project, index)
    if not project_details.files:
        raise HTTPException(HTTP_404_NOT_FOUND)
    for project_file in project_details.files:
        project_file.url = str(request.url_for("files", path=project_file.url))

    return get_response(request, headers, project_details, "detail.html")


async def ping(request: Request) -> PlainTextResponse:
    return PlainTextResponse("")


@asynccontextmanager
async def lifespan(app: Starlette):
    CACHE_DIR.mkdir(exist_ok=True)
    with database:
        database.update()
        yield {"etag": ETagProvider(database.database_file)}


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
