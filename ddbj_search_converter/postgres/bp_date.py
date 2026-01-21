"""
PostgreSQL から DDBJ BioProject の日付情報を取得するモジュール。

DDBJ BioProject の日付は XML に含まれていないため、PostgreSQL から取得する必要がある。
"""
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse

import psycopg2  # type: ignore

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import log_debug, log_warn

POSTGRES_DB_NAME = "bioproject"


def _format_date(dt: Optional[datetime]) -> Optional[str]:
    """datetime を ISO 8601 形式の文字列に変換する。"""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_postgres_url(postgres_url: str) -> Tuple[str, int, str, str]:
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


def fetch_bp_dates_bulk(
    config: Config,
    accessions: Iterable[str],
) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    PostgreSQL から BioProject の日付情報をバルク取得する。

    Returns:
        {accession: (dateCreated, dateModified, datePublished)}

    SQL:
        SELECT s.accession, p.create_date, p.modified_date, p.release_date
        FROM mass.bioproject_summary s
        INNER JOIN mass.project p ON s.submission_id = p.submission_id
        WHERE s.accession IN (...)
    """
    accession_list = list(accessions)
    if not accession_list:
        return {}

    host, port, user, password = _parse_postgres_url(config.postgres_url)
    result: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=POSTGRES_DB_NAME,
        )
        try:
            with conn.cursor() as cur:
                # IN 句のプレースホルダーを動的に生成
                placeholders = ",".join(["%s"] * len(accession_list))
                query = f"""
                    SELECT
                        s.accession,
                        p.create_date,
                        p.modified_date,
                        p.release_date
                    FROM mass.bioproject_summary s
                    INNER JOIN mass.project p
                    ON s.submission_id = p.submission_id
                    WHERE s.accession IN ({placeholders})
                """
                cur.execute(query, accession_list)
                rows = cur.fetchall()

                for row in rows:
                    accession, create_date, modified_date, release_date = row
                    result[accession] = (
                        _format_date(create_date),
                        _format_date(modified_date),
                        _format_date(release_date),
                    )
        finally:
            conn.close()
    except Exception as e:
        log_debug(f"Failed to fetch dates from PostgreSQL: {e}")
        raise

    log_debug(f"Fetched {len(result)} dates from PostgreSQL for {len(accession_list)} accessions")
    return result


def fetch_bp_accessions_modified_since(
    config: Config,
    since: str,
) -> set[str]:
    """
    PostgreSQL から指定日時以降に更新された BioProject の accession を取得する。

    Args:
        config: Config オブジェクト
        since: ISO8601 形式のタイムスタンプ (例: "2026-01-19T00:00:00Z")

    Returns:
        modified_date >= since の accession の集合

    SQL:
        SELECT s.accession
        FROM mass.bioproject_summary s
        INNER JOIN mass.project p ON s.submission_id = p.submission_id
        WHERE p.modified_date >= %s
    """
    host, port, user, password = _parse_postgres_url(config.postgres_url)
    result: set[str] = set()

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=POSTGRES_DB_NAME,
        )
        try:
            with conn.cursor() as cur:
                query = """
                    SELECT s.accession
                    FROM mass.bioproject_summary s
                    INNER JOIN mass.project p
                    ON s.submission_id = p.submission_id
                    WHERE p.modified_date >= %s
                """
                cur.execute(query, (since,))
                rows = cur.fetchall()

                for row in rows:
                    result.add(row[0])
        finally:
            conn.close()
    except Exception as e:
        log_debug(f"Failed to fetch accessions from PostgreSQL: {e}")
        raise

    log_debug(f"Fetched {len(result)} accessions modified since {since}")
    return result
