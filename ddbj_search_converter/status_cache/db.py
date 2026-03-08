"""
BioProject/BioSample の status キャッシュを格納する DuckDB データベースの操作モジュール。

Livelist ファイルから事前取得した status 情報を DuckDB にキャッシュし、
JSONL 生成時にローカル読み取りを行う。

ファイルパス:
    - 一時 DB: {result_dir}/bp_bs_status.tmp.duckdb
    - 最終 DB: {result_dir}/bp_bs_status.duckdb
"""

import shutil
from collections.abc import Iterable
from pathlib import Path

import duckdb

from ddbj_search_converter.config import STATUS_CACHE_DB_FILE_NAME, TMP_STATUS_CACHE_DB_FILE_NAME, Config
from ddbj_search_converter.logging.logger import log_info

CHUNK_SIZE = 10000


def _tmp_db_path(config: Config) -> Path:
    return config.result_dir.joinpath(TMP_STATUS_CACHE_DB_FILE_NAME)


def _final_db_path(config: Config) -> Path:
    return config.result_dir.joinpath(STATUS_CACHE_DB_FILE_NAME)


def status_cache_exists(config: Config) -> bool:
    return _final_db_path(config).exists()


def init_status_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE bp_status (
                accession TEXT NOT NULL,
                status TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE bs_status (
                accession TEXT NOT NULL,
                status TEXT NOT NULL
            )
        """)


def insert_bp_statuses(
    config: Config,
    rows: Iterable[tuple[str, str]],
) -> int:
    db_path = _tmp_db_path(config)
    total = 0
    chunk: list[tuple[str, str]] = []

    with duckdb.connect(str(db_path)) as conn:
        for row in rows:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                conn.executemany("INSERT INTO bp_status VALUES (?, ?)", chunk)
                total += len(chunk)
                chunk.clear()
        if chunk:
            conn.executemany("INSERT INTO bp_status VALUES (?, ?)", chunk)
            total += len(chunk)

    return total


def insert_bs_statuses(
    config: Config,
    rows: Iterable[tuple[str, str]],
) -> int:
    db_path = _tmp_db_path(config)
    total = 0
    chunk: list[tuple[str, str]] = []

    with duckdb.connect(str(db_path)) as conn:
        for row in rows:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                conn.executemany("INSERT INTO bs_status VALUES (?, ?)", chunk)
                total += len(chunk)
                chunk.clear()
        if chunk:
            conn.executemany("INSERT INTO bs_status VALUES (?, ?)", chunk)
            total += len(chunk)

    return total


def finalize_status_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE UNIQUE INDEX idx_bp_status_accession ON bp_status (accession)")
        conn.execute("CREATE UNIQUE INDEX idx_bs_status_accession ON bs_status (accession)")

    final_path = _final_db_path(config)
    if final_path.exists():
        final_path.unlink()

    shutil.move(str(db_path), str(final_path))
    log_info(f"status cache finalized: {final_path}")


def fetch_bp_statuses_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, str]:
    accession_list = list(accessions)
    if not accession_list:
        return {}

    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession, status
            FROM bp_status
            WHERE accession IN (SELECT UNNEST(?))
            """,
            (accession_list,),
        ).fetchall()

    return dict(rows)


def fetch_bs_statuses_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, str]:
    accession_list = list(accessions)
    if not accession_list:
        return {}

    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession, status
            FROM bs_status
            WHERE accession IN (SELECT UNNEST(?))
            """,
            (accession_list,),
        ).fetchall()

    return dict(rows)
