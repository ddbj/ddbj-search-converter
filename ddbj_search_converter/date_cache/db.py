"""
BioProject/BioSample の日付キャッシュを格納する DuckDB データベースの操作モジュール。

PostgreSQL から事前取得した日付情報を DuckDB にキャッシュし、
JSONL 生成時にローカル読み取りを行う。

ファイルパス:
    - 一時 DB: {result_dir}/bp_bs_date.tmp.duckdb
    - 最終 DB: {result_dir}/bp_bs_date.duckdb
"""

import shutil
from collections.abc import Iterable
from pathlib import Path

import duckdb

from ddbj_search_converter.config import DATE_CACHE_DB_FILE_NAME, TMP_DATE_CACHE_DB_FILE_NAME, Config
from ddbj_search_converter.logging.logger import log_info

CHUNK_SIZE = 10000

DateTuple = tuple[str | None, str | None, str | None]


def _tmp_db_path(config: Config) -> Path:
    return config.result_dir / TMP_DATE_CACHE_DB_FILE_NAME


def _final_db_path(config: Config) -> Path:
    return config.result_dir / DATE_CACHE_DB_FILE_NAME


def date_cache_exists(config: Config) -> bool:
    return _final_db_path(config).exists()


def init_date_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE bp_date (
                accession TEXT NOT NULL,
                date_created TEXT,
                date_modified TEXT,
                date_published TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE bs_date (
                accession TEXT NOT NULL,
                date_created TEXT,
                date_modified TEXT,
                date_published TEXT
            )
        """)


def insert_bp_dates(
    config: Config,
    rows: Iterable[tuple[str, str | None, str | None, str | None]],
) -> int:
    db_path = _tmp_db_path(config)
    total = 0
    chunk: list[tuple[str, str | None, str | None, str | None]] = []

    with duckdb.connect(str(db_path)) as conn:
        for row in rows:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                conn.executemany("INSERT INTO bp_date VALUES (?, ?, ?, ?)", chunk)
                total += len(chunk)
                chunk.clear()
        if chunk:
            conn.executemany("INSERT INTO bp_date VALUES (?, ?, ?, ?)", chunk)
            total += len(chunk)

    return total


def insert_bs_dates(
    config: Config,
    rows: Iterable[tuple[str, str | None, str | None, str | None]],
) -> int:
    db_path = _tmp_db_path(config)
    total = 0
    chunk: list[tuple[str, str | None, str | None, str | None]] = []

    with duckdb.connect(str(db_path)) as conn:
        for row in rows:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                conn.executemany("INSERT INTO bs_date VALUES (?, ?, ?, ?)", chunk)
                total += len(chunk)
                chunk.clear()
        if chunk:
            conn.executemany("INSERT INTO bs_date VALUES (?, ?, ?, ?)", chunk)
            total += len(chunk)

    return total


def finalize_date_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE UNIQUE INDEX idx_bp_date_accession ON bp_date (accession)")
        conn.execute("CREATE INDEX idx_bp_date_modified ON bp_date (date_modified)")
        conn.execute("CREATE UNIQUE INDEX idx_bs_date_accession ON bs_date (accession)")
        conn.execute("CREATE INDEX idx_bs_date_modified ON bs_date (date_modified)")

    final_path = _final_db_path(config)
    if final_path.exists():
        final_path.unlink()

    shutil.move(str(db_path), str(final_path))
    log_info(f"date cache finalized: {final_path}")


def fetch_bp_dates_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, DateTuple]:
    accession_list = list(accessions)
    if not accession_list:
        return {}

    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession, date_created, date_modified, date_published
            FROM bp_date
            WHERE accession IN (SELECT UNNEST(?))
            """,
            (accession_list,),
        ).fetchall()

    result: dict[str, DateTuple] = {}
    for acc, dc, dm, dp in rows:
        result[acc] = (dc, dm, dp)

    return result


def fetch_bs_dates_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, DateTuple]:
    accession_list = list(accessions)
    if not accession_list:
        return {}

    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession, date_created, date_modified, date_published
            FROM bs_date
            WHERE accession IN (SELECT UNNEST(?))
            """,
            (accession_list,),
        ).fetchall()

    result: dict[str, DateTuple] = {}
    for acc, dc, dm, dp in rows:
        result[acc] = (dc, dm, dp)

    return result


def fetch_bp_accessions_modified_since_from_cache(
    config: Config,
    since: str,
) -> set[str]:
    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession
            FROM bp_date
            WHERE date_modified >= ?
            """,
            (since,),
        ).fetchall()

    return {row[0] for row in rows}


def fetch_bs_accessions_modified_since_from_cache(
    config: Config,
    since: str,
) -> set[str]:
    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession
            FROM bs_date
            WHERE date_modified >= ?
            """,
            (since,),
        ).fetchall()

    return {row[0] for row in rows}
