import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime

import msgspec
from packaging.utils import canonicalize_name
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.status import HTTP_404_NOT_FOUND

from .config import BASE_DIR, CACHE_FILE
from .database import Database
from .dist_scanner import FileWatcher, ProjectFileReader
from .endpoint_utils import ETagProvider, get_response, handle_etag

logger = logging.getLogger(__name__)
database = Database(CACHE_FILE)

logging.basicConfig(
    level=logging.INFO,
    handlers=logging.getLogger("uvicorn").handlers or None,
)


async def index(request: Request) -> Response:
    headers = handle_etag(request, None)
    index: str = request.path_params.get("index", "")

    project_list = await database.get_project_list(index)
    if not project_list.projects:
        raise HTTPException(HTTP_404_NOT_FOUND)

    return get_response(request, headers, project_list, "index.html")


async def detail(request: Request) -> Response:
    headers = handle_etag(request, None)
    index: str = request.path_params.get("index", "")
    project_raw: str = request.path_params["project"]

    project = canonicalize_name(project_raw)
    if project_raw != project:
        url = request.url.path.replace(project_raw, project)
        return RedirectResponse(url, status_code=301)

    project_details = await database.get_project_detail(project, index)
    if not project_details.files:
        raise HTTPException(HTTP_404_NOT_FOUND)
    for project_file in project_details.files:
        project_file.url = str(request.url_for("files", path=project_file.url))

    return get_response(request, headers, project_details, "detail.html")


async def metadata(request: Request) -> Response:
    headers = {
        **handle_etag(request, None),
        "Cache-Control": "max-age=600, public",
    }
    index: str = request.path_params.get("index", "")
    filename: str = request.path_params["filename"]  # w/o suffix .metadata

    metadata_content = await database.get_metadata(filename, index)
    if metadata_content is None:
        raise HTTPException(HTTP_404_NOT_FOUND)
    return Response(metadata_content, headers=headers, media_type="binary/octet-stream")


async def ping(request: Request) -> PlainTextResponse:
    return PlainTextResponse("")


def status(request: Request) -> JSONResponse:
    last_changed = datetime.fromtimestamp(request.state.etag.last_changed, UTC)
    result = {
        "global": msgspec.to_builtins(database.stats()),
        "indexes": database.stats_per_index(),
        "last_update": last_changed.replace(microsecond=0).astimezone().isoformat(),
    }
    return JSONResponse(result)


@asynccontextmanager
async def lifespan(app: Starlette):
    CACHE_FILE.parent.mkdir(exist_ok=True, parents=True)
    _update_database()
    _ = FileWatcher(BASE_DIR, _update_database)
    with database:
        yield {"etag": ETagProvider(CACHE_FILE)}


def _update_database() -> None:
    logger.info("Updating database")
    with replace(database, read_only=False) as db:
        db.update(ProjectFileReader(BASE_DIR))


static_files = StaticFiles(directory=BASE_DIR)

routes = [
    Route("/", endpoint=status),
    Route("/ping", endpoint=ping),
    Route("/simple/", endpoint=index),
    Route("/simple/{project}/", endpoint=detail),
    Route("/{index:path}/simple/", endpoint=index),
    Route("/{index:path}/simple/{project}/", endpoint=detail),
    Route("/files/{filename:path}.metadata", endpoint=metadata),
    Mount("/files", static_files, name="files"),
]

app = Starlette(routes=routes, lifespan=lifespan)
