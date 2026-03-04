from datetime import UTC, datetime, timedelta
from typing import Self

import jwt
import msgspec

from . import config


class JWTClaims(msgspec.Struct, omit_defaults=True):
    sub: str
    scope: list[str] | str
    exp: int | float | None = None
    max_upload_size: int | None = None


class AuthContext(msgspec.Struct, frozen=True):
    user: str
    scope: list[str]
    max_upload_size: int | None = None

    @classmethod
    def from_jwt(cls, token: str, secret: str | None = None) -> Self | None:
        secret = config.UPLOAD_JWT_SECRET if secret is None else secret
        if not token or not secret:
            return None

        try:
            claims_raw = jwt.decode(
                token,
                secret,
                algorithms=["HS256"],
                options={"require": ["sub", "scope"]},
            )
            claims = msgspec.convert(claims_raw, type=JWTClaims)
        except (jwt.InvalidTokenError, msgspec.ValidationError):
            return None

        scope = claims.scope.split() if isinstance(claims.scope, str) else claims.scope
        return cls(user=claims.sub, scope=scope, max_upload_size=claims.max_upload_size)


def create_jwt(
    *,
    user: str,
    scope: list[str],
    secret: str | None = None,
    expires_in: int | None = None,
    max_upload_size: int | None = None,
) -> str:
    secret = config.UPLOAD_JWT_SECRET if secret is None else secret
    if not secret:
        raise ValueError("Missing JWT secret")

    claims = JWTClaims(
        sub=user,
        scope=scope,
        exp=(
            (datetime.now(UTC) + timedelta(seconds=expires_in)).timestamp()
            if expires_in is not None
            else None
        ),
        max_upload_size=max_upload_size,
    )
    return jwt.encode(msgspec.to_builtins(claims), secret, algorithm="HS256")
