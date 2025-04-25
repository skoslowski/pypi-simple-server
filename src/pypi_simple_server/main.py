import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
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

from .config import BASE_DIR, CACHE_FILE, FILES_DIR
from .database import Database
from .dist_scanner import FileWatcher, ProjectFileReader
from .endpoint_utils import ResponseHeaders, get_response, handle_conditional_request
from .static_files_gen import StaticFilesDirGenerator

logger = logging.getLogger(__name__)
database = Database(CACHE_FILE)
static_files = StaticFilesDirGenerator(directory=FILES_DIR)
response_headers = ResponseHeaders(
    {
        "Cache-Control": "max-age=600, public",
        "Vary": "Accept, Accept-Encoding",
    }
)


if not logging.root.hasHandlers():  # pragma: no cover
    logging.basicConfig(
        level=logging.WARNING,
        handlers=logging.getLogger("uvicorn").handlers or None,
    )
    logging.getLogger(__package__).setLevel(logging.INFO)


async def index(request: Request) -> Response:
    handle_conditional_request(request.headers, response_headers)
    index: str = request.path_params.get("index", "")

    project_list = await database.get_project_list(index)
    if not project_list.projects:
        raise HTTPException(HTTP_404_NOT_FOUND)

    return get_response(request, response_headers, project_list, "index.html")


async def detail(request: Request) -> Response:
    handle_conditional_request(request.headers, response_headers)
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

    return get_response(request, response_headers, project_details, "detail.html")


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

    await _handle_file_change({CACHE_FILE, BASE_DIR})
    watch = FileWatcher(BASE_DIR, _handle_file_change)
    watch.ignore = {
        static_files.directory,
        CACHE_FILE.with_name(CACHE_FILE.name + "-journal"),
    }
    with database:
        yield


async def _handle_file_change(files: set[Path]) -> None:
    if files and files != {CACHE_FILE}:
        logger.info("Updating database")
        with replace(database, read_only=False) as db:
            reader = ProjectFileReader(BASE_DIR, ignore_dirs={static_files.directory})
            await db.update(reader, static_files)
        logger.info("Completed database update")

    if CACHE_FILE in files:
        logger.info("Updating response headers after database update")
        response_headers.update_changed(CACHE_FILE.stat().st_mtime)


routes = [
    Route("/", endpoint=status),
    Route("/ping", endpoint=ping),
    Route("/simple/", endpoint=index),
    Route("/simple/{project}/", endpoint=detail),
    Route("/{index:path}/simple/", endpoint=index),
    Route("/{index:path}/simple/{project}/", endpoint=detail),
    Mount("/files", StaticFiles(directory=static_files.directory, follow_symlink=True), name="files"),
]

app = Starlette(routes=routes, lifespan=lifespan)
