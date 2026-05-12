"""PostgreSQL 関連のユーティリティ関数。"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import psycopg2

from ddbj_search_converter.config import ISO8601_UTC_FORMAT

# 標準 logging を使うのは、本リポジトリの `log_warn` が `run_logger` context を要求する
# ため。`connect_with_retry` は run_logger 外でも呼ばれ得る (テスト・直接利用) ので、
# context 不要な標準 logger に倒す。
_LOGGER = logging.getLogger(__name__)

# 接続設定のデフォルト値。INSDC PostgreSQL は数秒〜数十秒で応答が返るのが通常で、
# 30 秒で切るのは「サーバーがダウンしている」シグナル。max_retries=3, backoff=5 で
# 一過性のネットワーク不調や立ち上げタイミングのずれを吸収する。
DEFAULT_CONNECT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = 5.0


def connect_with_retry(
    *,
    connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    **connect_kwargs: Any,
) -> psycopg2.extensions.connection:
    """`psycopg2.connect` を timeout + retry 付きで呼び出す。

    `OperationalError` のみ retry 対象。`InterfaceError` や認証エラー等の retry しても
    変わらない失敗は即 raise する。caller 側は通常の `psycopg2.connect()` と同じ kwargs
    を渡せる (host / port / user / password / dbname / keepalives_* など)。
    """
    last_err: psycopg2.OperationalError | None = None
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(connect_timeout=connect_timeout, **connect_kwargs)
        except psycopg2.OperationalError as e:
            last_err = e
            if attempt < max_retries - 1:
                _LOGGER.warning(
                    "psycopg2.connect failed (attempt %d/%d): %s; retrying in %.1fs",
                    attempt + 1, max_retries, e, backoff_seconds,
                )
                time.sleep(backoff_seconds)
    assert last_err is not None
    raise last_err


@contextmanager
def postgres_connection(
    postgres_url: str,
    dbname: str,
) -> Iterator[psycopg2.extensions.connection]:
    """PostgreSQL 接続のコンテキストマネージャ。"""
    host, port, user, password = parse_postgres_url(postgres_url)
    conn = connect_with_retry(
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
