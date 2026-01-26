"""
PostgreSQL から DDBJ BioSample の日付情報を取得するモジュール。

DDBJ BioSample の日付は XML に含まれていないため、PostgreSQL から取得する必要がある。
"""
from typing import Dict, Iterable, Optional, Set, Tuple

import psycopg2

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import log_debug
from ddbj_search_converter.postgres.utils import (format_date,
                                                  parse_postgres_url)

POSTGRES_DB_NAME = "biosample"


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

    host, port, user, password = parse_postgres_url(config.postgres_url)
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
                        format_date(create_date),
                        format_date(modified_date),
                        format_date(release_date),
                    )
        finally:
            conn.close()
    except Exception as e:
        log_debug(f"failed to fetch dates from postgresql: {e}")
        raise

    log_debug(f"fetched {len(result)} dates from postgresql for {len(accession_list)} accessions")
    return result


def fetch_bs_accessions_modified_since(
    config: Config,
    since: str,
) -> Set[str]:
    """
    PostgreSQL から指定日時以降に更新された BioSample の accession を取得する。

    Args:
        config: Config オブジェクト
        since: ISO8601 形式のタイムスタンプ (例: "2026-01-19T00:00:00Z")

    Returns:
        modified_date >= since の accession の集合

    SQL:
        SELECT s.accession_id
        FROM mass.biosample_summary s
        INNER JOIN (
            SELECT DISTINCT ON (submission_id)
                submission_id, modified_date
            FROM mass.sample
            ORDER BY submission_id
        ) p ON s.submission_id = p.submission_id
        WHERE p.modified_date >= %s
    """
    host, port, user, password = parse_postgres_url(config.postgres_url)
    result: Set[str] = set()

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
                    SELECT s.accession_id
                    FROM mass.biosample_summary s
                    INNER JOIN (
                        SELECT DISTINCT ON (submission_id)
                            submission_id, modified_date
                        FROM mass.sample
                        ORDER BY submission_id
                    ) p ON s.submission_id = p.submission_id
                    WHERE p.modified_date >= %s
                """
                cur.execute(query, (since,))
                rows = cur.fetchall()

                for row in rows:
                    result.add(row[0])
        finally:
            conn.close()
    except Exception as e:
        log_debug(f"failed to fetch accessions from postgresql: {e}")
        raise

    log_debug(f"fetched {len(result)} accessions modified since {since}")
    return result
