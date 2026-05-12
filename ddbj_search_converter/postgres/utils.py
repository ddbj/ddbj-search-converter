"""PostgreSQL 関連のユーティリティ関数。"""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg2

from ddbj_search_converter.config import ISO8601_UTC_FORMAT


@contextmanager
def postgres_connection(
    postgres_url: str,
    dbname: str,
) -> Iterator[psycopg2.extensions.connection]:
    """PostgreSQL 接続のコンテキストマネージャ。"""
    host, port, user, password = parse_postgres_url(postgres_url)
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )
    try:
        yield conn
    finally:
        conn.close()


def format_date(dt: datetime | None) -> str | None:
    """datetime を ISO 8601 形式の文字列に変換する。"""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime(ISO8601_UTC_FORMAT)


ALLOWED_POSTGRES_SCHEMES = frozenset({"postgresql", "postgresql+psycopg", "postgres"})


def parse_postgres_url(postgres_url: str) -> tuple[str, int, str, str]:
    """
    PostgreSQL URL を解析して (host, port, user, password) を返す。

    Format: ``{scheme}://{username}:{password}@{host}:{port}``

    Accepted schemes: ``postgresql``, ``postgresql+psycopg``, ``postgres``.
    Anything else (missing scheme, ``mysql://``, etc.) raises ``ValueError``
    early so we never silently fall back to localhost.
    See docs/data-architecture.md §TRAD PostgreSQL の接続文字列の許容 scheme.
    """
    parsed = urlparse(postgres_url)
    if parsed.scheme not in ALLOWED_POSTGRES_SCHEMES:
        raise ValueError(
            f"unsupported PostgreSQL URL scheme: {parsed.scheme!r}. "
            f"Allowed schemes: {sorted(ALLOWED_POSTGRES_SCHEMES)}"
        )
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    user = parsed.username or ""
    password = parsed.password or ""
    return host, port, user, password
