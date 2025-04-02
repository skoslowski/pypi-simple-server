import pytest
from starlette.exceptions import HTTPException

from pypi_simple_server.endpoint_utils import MediaType, get_response_media_type

CASES = [
    (MediaType.HTML_V1, "*/*"),
    (MediaType.HTML_V1, "text/*"),
    (MediaType.HTML_V1, "text/html"),
    (None, "text/plain"),
    (MediaType.HTML_V1, MediaType.HTML_V1),
    (MediaType.HTML_V1, MediaType.HTML_LATEST),
    (MediaType.JSON_V1, MediaType.JSON_LATEST),
    (MediaType.JSON_V1, f"{MediaType.JSON_V1},{MediaType.HTML_V1}"),
    (MediaType.HTML_V1, f"{MediaType.JSON_V1};q=0.9,{MediaType.HTML_V1}"),
]


@pytest.mark.parametrize(("expected", "value"), CASES)
def test_response_media_types(value: str, expected: str | None):
    if expected:
        result = get_response_media_type(value)
        assert result == expected
    else:
        with pytest.raises(HTTPException):
            get_response_media_type(value)
