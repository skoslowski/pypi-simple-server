from datetime import datetime
from pathlib import Path

import humanize
from starlette.templating import Jinja2Templates


def fromisoformat(date_string: str | None) -> datetime | None:
    if not date_string:
        return None
    dt = datetime.fromisoformat(date_string)
    return dt.astimezone()


_templates = Jinja2Templates(Path(__file__).parent)
_templates.env.filters["naturaltime"] = humanize.naturaltime
_templates.env.filters["naturalsize"] = humanize.naturalsize
_templates.env.filters["fromisoformat"] = fromisoformat

TemplateResponse = _templates.TemplateResponse
