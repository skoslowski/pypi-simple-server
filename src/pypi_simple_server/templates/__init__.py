from pathlib import Path

import humanize
from starlette.templating import Jinja2Templates

_templates = Jinja2Templates(Path(__file__).parent)
_templates.env.filters["naturalsize"] = humanize.naturalsize

TemplateResponse = _templates.TemplateResponse
