from pathlib import Path

from starlette.config import Config

env_file = Path(".env")
config = Config(
    env_file=env_file if env_file.exists() else None,  # avoid warning
    env_prefix="PYPS_",
)

BASE_DIR = config("BASE_DIR", cast=Path, default=Path.cwd()).absolute()
CACHE_FILE = config("CACHE_FILE", cast=Path, default=BASE_DIR / ".cache.sqlite").absolute()
FILES_DIR = config("FILES_DIR", cast=Path, default=BASE_DIR / "files").absolute()

PICOCSS_URL = config("PICOCSS_URL", default="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css")
