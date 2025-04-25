import os
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path


@dataclass
class StaticFilesDirGenerator:
    directory: Path

    def __post_init__(self):
        self.directory.mkdir(parents=True, exist_ok=True)

    def _files(self, url_path: str) -> tuple[Path, Path]:
        dist_file = self.directory / url_path
        metadata_file = dist_file.with_name(dist_file.name + ".metadata")
        return dist_file, metadata_file

    def url_path(self, filename: str, hash: str) -> str:
        return f"{hash[:2]}/{filename}"

    def add(self, file: Path, hash: str, metadata: bytes) -> str:
        url_path = self.url_path(file.name, hash)

        dist_file, metadata_file = self._files(url_path)
        dist_file.parent.mkdir(parents=True, exist_ok=True)
        with suppress(FileExistsError):
            dist_file.symlink_to(_relative_to(file, dist_file.parent))

            metadata_file.write_bytes(metadata)
            stat = file.stat()
            os.utime(metadata_file, (stat.st_atime, stat.st_mtime))

        return url_path

    def update_link(self, file: Path, hash: str) -> None:
        dist_file, metadata_file = self._files(self.url_path(file.name, hash))
        dist_file.unlink()
        dist_file.symlink_to(_relative_to(file, dist_file.parent))
        stat = file.stat()
        os.utime(metadata_file, (stat.st_atime, stat.st_mtime))

    def remove(self, url_path: str) -> None:
        dist_file, metadata_file = self._files(url_path)
        dist_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)


def _relative_to(file: Path, dir: Path) -> Path:
    up = Path()
    for parent in (dir / "dummy").parents:
        if parent in file.parents:
            return up / file.relative_to(parent)
        up /= ".."

    raise ValueError("No common path found")  # pragma: no cover
