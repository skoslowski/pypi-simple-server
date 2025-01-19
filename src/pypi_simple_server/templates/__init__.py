from pathlib import Path

from jinja2 import Environment, FileSystemLoader

jinja_env = Environment(loader=FileSystemLoader(Path(__file__).parent))
