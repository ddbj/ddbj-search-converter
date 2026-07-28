"""
BioProject/BioSample の status キャッシュを格納する DuckDB データベースの操作モジュール。

Livelist ファイルから事前取得した status 情報を DuckDB にキャッシュし、
JSONL 生成時にローカル読み取りを行う。

Livelist は日付付きの全件スナップショットで status 自体に更新日時を持たないため、
取得範囲を絞る手がかりがなく、毎回全件で作り直す。

ファイルパス:
    - 一時 DB: {result_dir}/bp_bs_status.tmp.duckdb
    - 最終 DB: {result_dir}/bp_bs_status.duckdb
    - 中間 TSV: {result_dir}/{bp,bs}_status.tsv
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Literal, get_args

import duckdb

from ddbj_search_converter.config import STATUS_CACHE_DB_FILE_NAME, TMP_STATUS_CACHE_DB_FILE_NAME, Config
from ddbj_search_converter.duckdb_bulk import load_tsv_into_table, write_rows_to_tsv
from ddbj_search_converter.logging.logger import log_info

StatusTable = Literal["bp_status", "bs_status"]
STATUS_TABLES: tuple[StatusTable, ...] = get_args(StatusTable)
STATUS_COLUMNS = ("accession", "status")


def _tmp_db_path(config: Config) -> Path:
    return config.result_dir.joinpath(TMP_STATUS_CACHE_DB_FILE_NAME)


def _final_db_path(config: Config) -> Path:
    return config.result_dir.joinpath(STATUS_CACHE_DB_FILE_NAME)


def _tsv_path(config: Config, table: StatusTable) -> Path:
    return config.result_dir.joinpath(f"{table}.tsv")


def status_cache_exists(config: Config) -> bool:
    return _final_db_path(config).exists()


def init_status_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as conn:
        for table in STATUS_TABLES:
            conn.execute(f"""
                CREATE TABLE {table} (
                    accession TEXT NOT NULL,
                    status TEXT NOT NULL
                )
            """)


def insert_statuses(
    config: Config,
    table: StatusTable,
    rows: Iterable[tuple[str, str]],
) -> int:
    """Livelist 由来の行を中間 TSV 経由で一時 DB に投入し、投入した行数を返す。"""
    tsv_path = _tsv_path(config, table)
    written = write_rows_to_tsv(tsv_path, rows)

    with duckdb.connect(str(_tmp_db_path(config))) as conn:
        return load_tsv_into_table(conn, table, STATUS_COLUMNS, tsv_path, written)


def finalize_status_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)

    with duckdb.connect(str(db_path)) as conn:
        for table in STATUS_TABLES:
            conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_accession ON {table} (accession)")

    # replace() 自体が atomic な上書きなので、事前に unlink して
    # cache が存在しない瞬間を作らない。
    final_path = _final_db_path(config)
    db_path.replace(final_path)
    log_info(f"status cache finalized: {final_path}")


def fetch_bp_statuses_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, str]:
    return _fetch_statuses_from_cache(config, "bp_status", accessions)


def fetch_bs_statuses_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, str]:
    return _fetch_statuses_from_cache(config, "bs_status", accessions)


def _fetch_statuses_from_cache(
    config: Config,
    table: StatusTable,
    accessions: Iterable[str],
) -> dict[str, str]:
    accession_list = list(accessions)
    if not accession_list:
        return {}

    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT accession, status
            FROM {table}
            WHERE accession IN (SELECT UNNEST(?))
            """,
            (accession_list,),
        ).fetchall()

    return dict(rows)
