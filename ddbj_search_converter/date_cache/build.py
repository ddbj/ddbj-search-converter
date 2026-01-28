"""
PostgreSQL から BioProject/BioSample の日付情報を全件取得し、
DuckDB キャッシュを構築するモジュール。
"""
from typing import Iterator, Optional, Tuple

import psycopg2  # type: ignore[import-untyped]

from ddbj_search_converter.config import Config
from ddbj_search_converter.date_cache.db import (finalize_date_cache_db,
                                                  init_date_cache_db,
                                                  insert_bp_dates,
                                                  insert_bs_dates)
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.postgres.utils import format_date, parse_postgres_url

CURSOR_ITERSIZE = 50000

BP_POSTGRES_DB_NAME = "bioproject"
BS_POSTGRES_DB_NAME = "biosample"

BP_QUERY = """
    SELECT s.accession, p.create_date, p.modified_date, p.release_date
    FROM mass.bioproject_summary s
    INNER JOIN mass.project p ON s.submission_id = p.submission_id
"""

BS_QUERY = """
    SELECT s.accession_id, p.create_date, p.modified_date, p.release_date
    FROM mass.biosample_summary s
    INNER JOIN (
        SELECT DISTINCT ON (submission_id)
            submission_id, create_date, modified_date, release_date
        FROM mass.sample
        ORDER BY submission_id
    ) p ON s.submission_id = p.submission_id
"""


def _fetch_all_bp_dates(
    postgres_url: str,
) -> Iterator[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    host, port, user, password = parse_postgres_url(postgres_url)
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=BP_POSTGRES_DB_NAME,
    )
    try:
        with conn.cursor(name="bp_date_cursor") as cur:
            cur.itersize = CURSOR_ITERSIZE
            cur.execute(BP_QUERY)
            for row in cur:
                accession, create_date, modified_date, release_date = row
                yield (
                    accession,
                    format_date(create_date),
                    format_date(modified_date),
                    format_date(release_date),
                )
    finally:
        conn.close()


def _fetch_all_bs_dates(
    postgres_url: str,
) -> Iterator[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    host, port, user, password = parse_postgres_url(postgres_url)
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=BS_POSTGRES_DB_NAME,
    )
    try:
        with conn.cursor(name="bs_date_cursor") as cur:
            cur.itersize = CURSOR_ITERSIZE
            cur.execute(BS_QUERY)
            for row in cur:
                accession, create_date, modified_date, release_date = row
                yield (
                    accession,
                    format_date(create_date),
                    format_date(modified_date),
                    format_date(release_date),
                )
    finally:
        conn.close()


def build_date_cache(config: Config) -> None:
    log_info("initializing date cache db")
    init_date_cache_db(config)

    log_info("fetching all bp dates from postgresql")
    bp_rows = _fetch_all_bp_dates(config.postgres_url)
    bp_count = insert_bp_dates(config, bp_rows)
    log_info(f"inserted {bp_count} bp_date rows")

    log_info("fetching all bs dates from postgresql")
    bs_rows = _fetch_all_bs_dates(config.postgres_url)
    bs_count = insert_bs_dates(config, bs_rows)
    log_info(f"inserted {bs_count} bs_date rows")

    log_info("finalizing date cache db")
    finalize_date_cache_db(config)
    log_info("date cache build completed")
