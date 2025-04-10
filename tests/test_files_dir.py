from pathlib import Path

import pytest

from pypi_simple_server.files_dir import _relative_to

CASES = [
    ("/a.py", "/", "a.py"),
    ("/a/b.py", "/", "a/b.py"),
    ("/b.py", "/a", "../b.py"),
    ("/a/b.py", "/c", "../a/b.py"),
    ("a/b.py", "c", "../a/b.py"),
]


@pytest.mark.parametrize(("file", "dir", "expected"), CASES)
def test_relative_to(file: str, dir: str, expected: str):
    assert _relative_to(Path(file), Path(dir)) == Path(expected)
