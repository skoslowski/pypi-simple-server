import argparse
import re
import sys
from collections.abc import Sequence

from .auth import create_jwt

_DURATION_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[smhdwy])$")
_DURATION_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "w": 60 * 60 * 24 * 7,
    "y": 60 * 60 * 24 * 365,
}
_SIZE_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[KMG])$")
_SIZE_BYTES = {
    "K": 1024,
    "M": 1024 * 1024,
    "G": 1024 * 1024 * 1024,
}


def parse_duration(value: str) -> int:
    if value.isdigit():
        return int(value)

    if match := _DURATION_RE.fullmatch(value):
        amount = int(match.group("value"))
        unit = match.group("unit")
        return amount * _DURATION_SECONDS[unit]

    raise argparse.ArgumentTypeError(f"Invalid duration: {value!r}")


def parse_size(value: str) -> int:
    if value.isdigit():
        return int(value)

    if match := _SIZE_RE.fullmatch(value):
        amount = int(match.group("value"))
        unit = match.group("unit")
        return amount * _SIZE_BYTES[unit]

    raise argparse.ArgumentTypeError(f"Invalid size: {value!r}")


def build_parser() -> argparse.ArgumentParser:
    main_parser = argparse.ArgumentParser(prog="pypi_simple_server")
    subparsers = main_parser.add_subparsers(dest="command", required=True)

    parser = subparsers.add_parser("create-token", help="Create an upload token")
    parser.add_argument(
        "--user",
        "-u",
        required=True,
        help="token / uploader name",
    )
    parser.add_argument(
        "--scope",
        "-s",
        action="append",
        required=True,
        help="Allowed project glob; pass multiple times for multiple scopes",
    )
    parser.add_argument(
        "--expires-in",
        "-t",
        type=parse_duration,
        default=None,
        help="Token lifetime, e.g. 3600, 30m, 12h, 7d, 1y",
    )
    parser.add_argument(
        "--secret",
        help="JWT signing secret; defaults to PYPS_UPLOAD_JWT_SECRET",
    )
    parser.add_argument(
        "--max-upload-size",
        type=parse_size,
        default=None,
        help="Optional per-token upload size limit in bytes",
    )
    parser.set_defaults(func=create_token)
    return main_parser


def create_token(args: argparse.Namespace) -> int:
    try:
        token, token_id = create_jwt(
            user=args.user,
            scope=args.scope,
            secret=args.secret,
            expires_in=args.expires_in,
            max_upload_size=args.max_upload_size,
        )
    except Exception as e:
        raise SystemExit(e)

    print(f"Token-ID = {token_id}", file=sys.stderr)
    print(token)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
