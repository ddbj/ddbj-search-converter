"""
PostgreSQL から DDBJ BioProject の日付情報を取得するモジュール。

DDBJ BioProject の日付は XML に含まれていないため、PostgreSQL から取得する必要がある。
"""
from typing import Dict, Iterable, Optional, Set, Tuple

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import log_debug
from ddbj_search_converter.postgres.utils import (format_date,
                                                  postgres_connection)

POSTGRES_DB_NAME = "bioproject"


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

    result: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}

    try:
        with postgres_connection(config.postgres_url, POSTGRES_DB_NAME) as conn:
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
                        format_date(create_date),
                        format_date(modified_date),
                        format_date(release_date),
                    )
    except Exception as e:
        log_debug(f"failed to fetch dates from postgresql: {e}")
        raise

    log_debug(f"fetched {len(result)} dates from postgresql for {len(accession_list)} accessions")
    return result


def fetch_bp_accessions_modified_since(
    config: Config,
    since: str,
) -> Set[str]:
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
    result: Set[str] = set()

    try:
        with postgres_connection(config.postgres_url, POSTGRES_DB_NAME) as conn:
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
    except Exception as e:
        log_debug(f"failed to fetch accessions from postgresql: {e}")
        raise

    log_debug(f"fetched {len(result)} accessions modified since {since}")
    return result
