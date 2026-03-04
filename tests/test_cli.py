from datetime import UTC, datetime

import jwt
import pytest

from pypi_simple_server.__main__ import main


def test_create_token_outputs_token(capsys: pytest.CaptureFixture[str]):
    exit_code = main(
        [
            "create-token",
            "--user",
            "ci",
            "--scope",
            "pytest",
            "--scope",
            "my-org-*",
            "--secret",
            "secret-0123456789abcdef0123456789",
        ]
    )

    assert exit_code == 0
    token = capsys.readouterr().out.strip()
    claims = jwt.decode(token, "secret-0123456789abcdef0123456789", algorithms=["HS256"])
    assert claims["sub"] == "ci"
    assert claims["scope"] == ["pytest", "my-org-*"]


def test_create_token_supports_expiration(capsys: pytest.CaptureFixture[str]):
    before = datetime.now(UTC).timestamp()

    main(
        [
            "create-token",
            "--user",
            "ci",
            "--scope",
            "pytest",
            "--expires-in",
            "7d",
            "--secret",
            "secret-0123456789abcdef0123456789",
        ]
    )

    token = capsys.readouterr().out.strip()
    claims = jwt.decode(
        token,
        "secret-0123456789abcdef0123456789",
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    assert before + (7 * 24 * 60 * 60) - 10 <= claims["exp"] <= before + (7 * 24 * 60 * 60) + 10


def test_create_token_supports_max_upload_size(capsys: pytest.CaptureFixture[str]):
    main(
        [
            "create-token",
            "--user",
            "ci",
            "--scope",
            "pytest",
            "--max-upload-size",
            "10M",
            "--secret",
            "secret-0123456789abcdef0123456789",
        ]
    )

    token = capsys.readouterr().out.strip()
    claims = jwt.decode(
        token,
        "secret-0123456789abcdef0123456789",
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    assert claims["max_upload_size"] == 10 * 1024 * 1024


def test_create_token_supports_gigabyte_suffix(capsys: pytest.CaptureFixture[str]):
    main(
        [
            "create-token",
            "--user",
            "ci",
            "--scope",
            "pytest",
            "--max-upload-size",
            "2G",
            "--secret",
            "secret-0123456789abcdef0123456789",
        ]
    )

    token = capsys.readouterr().out.strip()
    claims = jwt.decode(
        token,
        "secret-0123456789abcdef0123456789",
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    assert claims["max_upload_size"] == 2 * 1024 * 1024 * 1024


def test_create_token_requires_secret():
    with pytest.raises(SystemExit, match="Missing JWT secret"):
        main(
            [
                "create-token",
                "--user",
                "ci",
                "--scope",
                "pytest",
                "--secret",
                "",
            ]
        )


def test_create_token_rejects_invalid_duration():
    with pytest.raises(SystemExit):
        main(
            [
                "create-token",
                "--user",
                "ci",
                "--scope",
                "pytest",
                "--expires-in",
                "soon",
                "--secret",
                "secret-0123456789abcdef0123456789",
            ]
        )
