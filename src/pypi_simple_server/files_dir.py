import os
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FilesDir:
    directory: Path

    def __post_init__(self):
        self.directory.mkdir(parents=True, exist_ok=True)

    def _files(self, dist_url: str) -> tuple[Path, Path]:
        dist_file = self.directory / dist_url
        metadata_file = dist_file.with_name(dist_file.name + ".metadata")
        return dist_file, metadata_file

    def add(self, file: Path, dist_url: str, metadata: bytes) -> None:
        dist_file, metadata_file = self._files(dist_url)
        dist_file.parent.mkdir(parents=True, exist_ok=True)
        with suppress(FileExistsError):
            dist_file.symlink_to(_relative_to(file, dist_file.parent))

            metadata_file.write_bytes(metadata)
            stat = file.stat()
            os.utime(metadata_file, (stat.st_atime, stat.st_mtime))

    def update_link(self, dist_url: str, file: Path) -> None:
        dist_file, _ = self._files(dist_url)
        dist_file.unlink(missing_ok=True)
        dist_file.symlink_to(_relative_to(file, dist_file.parent))

    def remove(self, dist_url: str) -> None:
        dist_file, metadata_file = self._files(dist_url)
        dist_file.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)


def _relative_to(file: Path, dir: Path) -> Path:
    up = Path()
    for parent in (dir / "dummy").parents:
        if parent in file.parents:
            return up / file.relative_to(parent)
        up /= ".."

    raise ValueError("No common path found")
