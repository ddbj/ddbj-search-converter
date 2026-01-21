"""
PostgreSQL から DDBJ BioSample の日付情報を取得するモジュール。

DDBJ BioSample の日付は XML に含まれていないため、PostgreSQL から取得する必要がある。
"""
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse

import psycopg2  # type: ignore[import-untyped]

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import log_debug

POSTGRES_DB_NAME = "biosample"


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


def fetch_bs_dates_bulk(
    config: Config,
    accessions: Iterable[str],
) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    PostgreSQL から BioSample の日付情報をバルク取得する。

    Returns:
        {accession: (dateCreated, dateModified, datePublished)}

    SQL:
        SELECT s.accession_id, p.create_date, p.modified_date, p.release_date
        FROM mass.biosample_summary s
        INNER JOIN (
            SELECT DISTINCT ON (submission_id)
                submission_id, create_date, modified_date, release_date
            FROM mass.sample
            ORDER BY submission_id
        ) p ON s.submission_id = p.submission_id
        WHERE s.accession_id IN (...)
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
                placeholders = ",".join(["%s"] * len(accession_list))
                query = f"""
                    SELECT
                        s.accession_id,
                        p.create_date,
                        p.modified_date,
                        p.release_date
                    FROM mass.biosample_summary s
                    INNER JOIN (
                        SELECT DISTINCT ON (submission_id)
                            submission_id, create_date, modified_date, release_date
                        FROM mass.sample
                        ORDER BY submission_id
                    ) p ON s.submission_id = p.submission_id
                    WHERE s.accession_id IN ({placeholders})
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
