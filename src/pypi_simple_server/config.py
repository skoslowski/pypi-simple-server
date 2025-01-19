from pathlib import Path

from pydantic import DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PYPS_", env_file=".env")

    base_dir: DirectoryPath = Path.cwd()
    cache_dir: Path | None = None
    files_url: str = "/files"

    @property
    def database_file(self) -> Path:
        return self.cache_dir_.absolute() / "database.sqlite"

    @property
    def cache_dir_(self) -> Path:
        return self.cache_dir or self.base_dir / ".cache"
