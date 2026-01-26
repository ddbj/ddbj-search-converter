"""PostgreSQL 関連のユーティリティ関数。"""
from datetime import datetime, timezone
from typing import Optional, Tuple
from urllib.parse import urlparse


def format_date(dt: Optional[datetime]) -> Optional[str]:
    """datetime を ISO 8601 形式の文字列に変換する。"""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_postgres_url(postgres_url: str) -> Tuple[str, int, str, str]:
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
