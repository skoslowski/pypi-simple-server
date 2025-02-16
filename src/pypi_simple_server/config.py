from dataclasses import dataclass, fields
from os import environ
from pathlib import Path

_NO_PATH = Path("???")


@dataclass
class Config:
    base_dir: Path = Path.cwd()
    cache_dir: Path = _NO_PATH

    def __post_init__(self) -> None:
        if self.cache_dir is _NO_PATH:
            self.cache_dir = self.base_dir / ".cache"

    @property
    def database_file(self) -> Path:
        return self.cache_dir.absolute() / "database.sqlite"


def _read_dot_env() -> dict[str, str]:
    dot_env_file = Path(".env")
    if not dot_env_file.exists():
        return {}
    return {
        kv[0].strip(): kv[2].strip()
        for line in dot_env_file.read_text().splitlines()
        if (kv := line.partition("#")[0].partition("="))[1]
    }


def load_config() -> Config:
    config = Config()
    dot_env = _read_dot_env()
    for field in fields(Config):
        assert isinstance(field.type, type)
        env_var = f"PYPS_{field.name}".upper()
        if value := environ.get(env_var) or dot_env.get(env_var):
            setattr(config, field.name, field.type(value))
    return config
