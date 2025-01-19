from collections.abc import Sequence

from packaging.utils import NormalizedName

# from pydantic import SQLModel, Field
from sqlmodel import JSON, Column, Field, SQLModel
from sqlmodel import create_engine as create_engine

# https://peps.python.org/pep-0508/#names
PROJECT_NAME_PATTERN = r"^([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])$"
NORMALIZED_PROJECT_NAME_PATTERN = r"^([0-9a-z]+-)*[0-9a-z]+$"


class Meta(SQLModel):
    # api_version: str = "1.0"  # PEP-629
    api_version: str = "1.1"  # PEP-700


class ProjectFile(SQLModel):
    # PEP-503
    filename: str
    # PEP-700
    size: int
    # PEP-503
    url: str  # HttpUrl
    # Limited to a len() of 1 in HTML
    hashes: dict[str, str] = Field(sa_column=Column(JSON))
    # not used here
    # gpg_sig: bool | None = None
    # PEP-503 (updated)
    requires_python: str | None = None
    # PEP-592
    yanked: str | None = None
    # PEP-658, renamed from dist_info_metadata in PEP-714
    core_metadata: dict[str, str] | None = Field(sa_column=Column(JSON))


class ProjectFileDB(ProjectFile, table=True):
    id: int | None = Field(default=None, primary_key=True)

    project_id: int | None = Field(default=None, foreign_key="project.id")
    project_version: str


# Simple Detail page (/simple/$PROJECT/)
class ProjectDetail(SQLModel):
    # PEP-629
    meta: Meta = Meta()
    # PEP-691
    name: NormalizedName = Field(regex=NORMALIZED_PROJECT_NAME_PATTERN)
    # PEP-700
    versions: list[str] = list()
    # PEP-503
    files: Sequence[ProjectFile] = list()


class Project(SQLModel):
    # PEP-691
    name: str = Field(regex=PROJECT_NAME_PATTERN)  # may be normalized


class ProjectDB(Project, table=True):
    id: int | None = Field(default=None, primary_key=True)
    index: str = Field(index=True)


# Simple Index page (/simple/)
class ProjectList(SQLModel):
    """list of project names, a.k.a. project index - /simple/"""

    # PEP-629
    meta: Meta = Meta()
    # PEP-503
    projects: Sequence[Project] = []
