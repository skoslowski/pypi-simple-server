"""
Model for Simple Index data

https://packaging.python.org/en/latest/specifications/simple-repository-api/
"""

from typing import Annotated

from msgspec import Meta as M
from msgspec import Struct
from packaging.utils import NormalizedName, Version

# https://peps.python.org/pep-0508/#names
ProjectName = Annotated[str, M(pattern=r"^([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])$")]
NormalizedProjectName = Annotated[NormalizedName, M(pattern=r"^([0-9a-z]+-)*[0-9a-z]+$")]


class Meta(Struct, frozen=True, rename="kebab"):
    # api_version: str = "1.0"  # PEP-629
    api_version: str = "1.1"  # PEP-700


class Hashes(Struct):
    sha256: str


class ProjectFile(Struct, omit_defaults=True, rename="kebab"):
    # PEP-503
    filename: str
    # PEP-700
    size: int
    # PEP-503
    url: str  # HttpUrl
    # Limited to a len() of 1 in HTML
    hashes: Hashes  # could be empty
    # not used here
    gpg_sig: bool | None = None
    # PEP-503 (updated)
    requires_python: str | None = None
    # PEP-592
    yanked: str | None = None
    # PEP-658, renamed from dist_info_metadata in PEP-714
    core_metadata: Hashes | None = None
    # PEP-700
    upload_time: str | None = None

    @property
    def version(self) -> Version:
        try:
            return Version(self.filename.split("-", 3)[1].removesuffix(".tar.gz"))
        except Exception:
            return Version("?")


class ProjectDetail(Struct, kw_only=True):
    """details on project - /simple/$NORM_NAME/"""

    # PEP-629
    meta: Meta = Meta()
    # PEP-691
    name: NormalizedProjectName
    # PEP-700
    versions: list[str] = list()
    # PEP-503
    files: list[ProjectFile] = list()


class Project(Struct):
    # PEP-691
    name: ProjectName  # may be normalized


class ProjectList(Struct):
    """list of project names, a.k.a. project index - /simple/"""

    # PEP-629
    meta: Meta = Meta()
    # PEP-503
    projects: list[Project] = list()
