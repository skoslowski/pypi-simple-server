import logging
from contextlib import asynccontextmanager
from hashlib import md5
from importlib.metadata import version
from typing import Annotated

from anyio import to_thread
from fastapi import Depends, FastAPI, Request, Response
from fastapi import Path as PathParam
from fastapi.exceptions import HTTPException
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from packaging.utils import canonicalize_name
from sqlmodel import Session
from starlette.status import HTTP_200_OK, HTTP_404_NOT_FOUND

from .config import Settings
from .database import (
    create_db_and_tables,
    create_engine,
    get_project_detail,
    get_project_list,
)
from .dependencies.content_negotiation import MediaType, ResponseMediaTypeDep, SimpleV1JSONResponse
from .dependencies.etag import ETagDepends
from .loader import update_db
from .models import ProjectDetail, ProjectList
from .templates import jinja_env
from .utils import FileMTimeWatcher

logger = logging.getLogger(__name__)
settings = Settings()
engine = create_engine(
    url=f"sqlite:///{settings.database_file}",
    connect_args={"check_same_thread": False},
)
templates = Jinja2Templates(env=jinja_env)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Create and init database")
    settings.database_file.parent.mkdir(parents=True, exist_ok=True)
    create_db_and_tables(engine)

    with Session(engine) as session:
        await reload_index_data(session)

    watcher = FileMTimeWatcher(settings.database_file)
    watcher.start()

    yield {"etag_gen": lambda: md5(str(watcher.mtime).encode()).hexdigest()}


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


app = FastAPI(
    title="Python Package Index Simple-API Server",
    version=version(__package__ or ""),
    lifespan=lifespan,
)


@app.get("/ping", response_class=PlainTextResponse)
async def ping():
    return  # docker health-check


@app.get("/reload", response_class=PlainTextResponse)
async def reload_index_data(session: SessionDep):
    logger.info("Scan files and update database")
    await to_thread.run_sync(update_db, session, settings.base_dir, settings.cache_dir_)


def index_get(path: str, summary: str):
    return app.get(
        path,
        dependencies=[ETagDepends],
        response_class=SimpleV1JSONResponse,
        response_model=ProjectList,
        response_model_exclude_none=True,
        summary=summary,
        responses={
            HTTP_200_OK: {
                "content": {MediaType.HTML_V1: {}},
                "description": "Project Index either as HTML or JSON",
            }
        },
    )


def detail_get(path: str, summary: str):
    return app.get(
        path,
        dependencies=[ETagDepends],
        response_class=SimpleV1JSONResponse,
        response_model=ProjectDetail,
        response_model_exclude_none=True,
        summary=summary,
        responses={
            HTTP_200_OK: {
                "content": {MediaType.HTML_V1: {}},
                "description": "Project Index either as HTML or JSON",
            }
        },
    )


IndexParam = PathParam(description="Name of a sub-index. Allows creating package namespace", examples=["sub"])
ProjectParam = PathParam(description="Name of the project to show details for.", examples=["pytest"])


@index_get("/simple/", summary="Project Index")
async def index_root(
    request: Request, response: Response, media_type: ResponseMediaTypeDep, session: SessionDep
):
    return await index(request, response, media_type, session, index=None)


@detail_get("/simple/{project}/", summary="Root Project Detail")
async def project_detail_root(
    request: Request,
    response: Response,
    media_type: ResponseMediaTypeDep,
    session: SessionDep,
    project: str,
):
    return await project_detail(request, response, media_type, session, project, index=None)


@index_get("/{index}/simple/", summary="Namespaced Project Index")
async def index(
    request: Request,
    response: Response,
    media_type: ResponseMediaTypeDep,
    session: SessionDep,
    index: str | None = IndexParam,
):
    project_list = await to_thread.run_sync(get_project_list, session, index)

    if not project_list.projects:
        raise HTTPException(HTTP_404_NOT_FOUND, detail="Can't find this index")

    if media_type == MediaType.HTML_V1:
        return templates.TemplateResponse(
            request,
            name="index.html",
            context={"model": project_list},
            media_type=media_type,
            headers=response.headers,
        )
    return project_list


@detail_get("/{index}/simple/{project}/", summary="Namespaced Project Detail")
async def project_detail(
    request: Request,
    response: Response,
    media_type: ResponseMediaTypeDep,
    session: SessionDep,
    project: str = ProjectParam,
    index: str | None = IndexParam,
):
    project_canonical = canonicalize_name(project)
    if project != project_canonical:
        return RedirectResponse(
            url=request.url.path.replace(project, project_canonical),
            status_code=301,
        )

    project_details = await to_thread.run_sync(get_project_detail, session, project_canonical, index)
    for project_file in project_details.files:
        project_file.url = str(request.url_for("files", path=project_file.url))
    if not project_details.files:
        raise HTTPException(HTTP_404_NOT_FOUND, detail="Can't find this project")

    if media_type == MediaType.HTML_V1:
        return templates.TemplateResponse(
            request=request,
            name="detail.html",
            context={"model": project_details},
            media_type=media_type,
            headers=response.headers,
        )
    return project_details


staticfiles = StaticFiles()
staticfiles.all_directories += [settings.base_dir, settings.cache_dir_]
app.mount(settings.files_url, staticfiles, name="files")
