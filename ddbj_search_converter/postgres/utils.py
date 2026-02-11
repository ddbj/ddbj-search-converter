"""PostgreSQL 関連のユーティリティ関数。"""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg2


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
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_postgres_url(postgres_url: str) -> tuple[str, int, str, str]:
    """
    PostgreSQL URL を解析して (host, port, user, password) を返す。

    Format: postgresql://{username}:{password}@{host}:{port}
    """
    parsed = urlparse(postgres_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    user = parsed.username or ""
    password = parsed.password or ""
    return host, port, user, password
