from pathlib import Path

from starlette.config import Config

env_file = Path(".env")
config = Config(
    env_file=env_file if env_file.exists() else None,  # avoid warning
    env_prefix="PYPS_",
)

BASE_DIR = config("BASE_DIR", cast=Path, default=Path.cwd())
CACHE_DIR = config("CACHE_DIR", cast=Path, default=BASE_DIR / ".cache")
