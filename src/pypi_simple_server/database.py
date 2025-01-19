from collections.abc import Callable, Iterable
from typing import Any

from packaging.utils import NormalizedName
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlmodel import Session, SQLModel, select
from sqlmodel import create_engine as create_engine

from .models import ProjectDB, ProjectDetail, ProjectFileDB, ProjectList


def create_db_and_tables(engine):
    SQLModel.metadata.create_all(engine)


def get_project_list(session: Session, index: str | None) -> ProjectList:
    query = select(ProjectDB).distinct().order_by(ProjectDB.name)
    if index:
        query = query.where(ProjectDB.index == index)
    return ProjectList(projects=unique_on(session.exec(query), "name"))


def get_project_detail(session: Session, project: NormalizedName, index: str | None) -> ProjectDetail:
    query = (
        select(ProjectFileDB)
        .where(ProjectDB.id == ProjectFileDB.project_id)
        .where(ProjectDB.name == project)
        .order_by(ProjectFileDB.filename)
    )
    if index:
        query = query.where(ProjectDB.index == index)

    files = unique_on(session.exec(query), "filename")
    versions = {file.project_version for file in files}
    return ProjectDetail(name=project, versions=sorted(versions), files=files)


def get_one_or_create[T](session: Session, query: Any, factory: Callable[[], T]) -> T:
    try:
        return session.exec(query).one()
    except NoResultFound:
        pass

    created = factory()

    try:
        session.add(created)
        session.commit()
    except IntegrityError:
        session.rollback()
        return session.exec(query).one()

    session.refresh(created)
    return created


def unique_on[T](iter: Iterable[T], attr: str) -> list[T]:
    seen = set()
    return [e for e in iter if (u := getattr(e, attr)) not in seen and not seen.add(u)]
