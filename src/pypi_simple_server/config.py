from pathlib import Path

from starlette.config import Config

config = Config(env_file=".env", env_prefix="PYPS_")

BASE_DIR = config("BASE_DIR", cast=Path, default=Path.cwd())
CACHE_DIR = config("CACHE_DIR", cast=Path, default=BASE_DIR / ".cache")
