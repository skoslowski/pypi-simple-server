import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import md5
from pathlib import Path

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
from .endpoint_utils import get_response, handle_etag

logger = logging.getLogger(__name__)
database = Database(CACHE_FILE)
etag = ""

if not logging.root.hasHandlers():
    logging.basicConfig(
        level=logging.WARNING,
        handlers=logging.getLogger("uvicorn").handlers or None,
    )
    logging.getLogger(__package__).setLevel(logging.INFO)


async def index(request: Request) -> Response:
    headers = handle_etag(request, etag)
    index: str = request.path_params.get("index", "")

    project_list = await database.get_project_list(index)
    if not project_list.projects:
        raise HTTPException(HTTP_404_NOT_FOUND)

    return get_response(request, headers, project_list, "index.html")


async def detail(request: Request) -> Response:
    headers = handle_etag(request, etag)
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
        **handle_etag(request, etag),
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
    last_changed = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime, UTC)
    result = {
        "global": msgspec.to_builtins(database.stats()),
        "indexes": database.stats_per_index(),
        "last_update": last_changed.replace(microsecond=0).astimezone().isoformat(),
    }
    return JSONResponse(result)


@asynccontextmanager
async def lifespan(app: Starlette):
    CACHE_FILE.parent.mkdir(exist_ok=True, parents=True)

    _handle_file_change({CACHE_FILE, BASE_DIR})
    watch = FileWatcher(BASE_DIR, _handle_file_change)
    watch.ignore = {CACHE_FILE.with_name(CACHE_FILE.name + "-journal")}

    with database:
        yield


def _handle_file_change(files: set[Path]) -> None:
    global etag

    if files != {CACHE_FILE}:
        logger.info("Updating database")
        with replace(database, read_only=False) as db:
            db.update(ProjectFileReader(BASE_DIR))

    if CACHE_FILE in files:
        logger.info("Updating ETag")
        etag = md5(str(CACHE_FILE.stat().st_mtime).encode(), usedforsecurity=False).hexdigest()


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
