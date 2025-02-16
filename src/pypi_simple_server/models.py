"""
Model for Simple Index data

https://packaging.python.org/en/latest/specifications/simple-repository-api/
"""
from collections.abc import Sequence
from operator import attrgetter
from typing import Annotated

from msgspec import Meta as M
from msgspec import Struct
from packaging.utils import NormalizedName
from sortedcontainers import SortedSet

# https://peps.python.org/pep-0508/#names
ProjectName = Annotated[str, M(pattern=r"^([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])$")]
NormalizedProjectName = Annotated[NormalizedName, M(pattern=r"^([0-9a-z]+-)*[0-9a-z]+$")]


class Meta(Struct, frozen=True):
    # api_version: str = "1.0"  # PEP-629
    api_version: str = "1.1"  # PEP-700


class ProjectFile(Struct, omit_defaults=True):
    # PEP-503
    filename: str
    # PEP-700
    size: int
    # PEP-503
    url: str  # HttpUrl
    # Limited to a len() of 1 in HTML
    hashes: dict[str, str]
    # not used here
    gpg_sig: bool | None = None
    # PEP-503 (updated)
    requires_python: str | None = None
    # PEP-592
    yanked: str | None = None
    # PEP-658, renamed from dist_info_metadata in PEP-714
    core_metadata: dict[str, str] | None = None

    def __hash__(self) -> int:
        return hash(self.filename)


class ProjectDetail(Struct, kw_only=True):
    """details on project - /simple/$NORM_NAME/"""
    # PEP-629
    meta: Meta = Meta()
    # PEP-691
    name: NormalizedProjectName
    # PEP-700
    versions: set[str] = set()
    # PEP-503
    files: set[ProjectFile] = set()

    def __hash__(self) -> int:
        return hash(self.name)


class Project(Struct):
    # PEP-691
    name: ProjectName  # may be normalized

    def __hash__(self) -> int:
        return hash(self.name)


class ProjectList(Struct):
    """list of project names, a.k.a. project index - /simple/"""
    # PEP-629
    meta: Meta = Meta()
    # PEP-503
    projects: Sequence[Project] = SortedSet(key=attrgetter("name"))
