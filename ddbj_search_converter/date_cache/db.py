"""
BioProject/BioSample の日付キャッシュを格納する DuckDB データベースの操作モジュール。

PostgreSQL から取得した日付情報を DuckDB にキャッシュし、
JSONL 生成時にローカル読み取りを行う。

日次実行では窓ビルド (更新された行だけを取得して upsert) を行うため、既存の
最終 DB を一時 DB に seed してから追記する。全件ビルドのときだけ空から作り直す。
どちらのモードで動くかの判断材料は ``cache_meta`` テーブルに持つ。DB 本体と同じ
ファイルに置くことで、finalize の atomic replace で中身と一緒に切り替わる。

ファイルパス:
    - 一時 DB: {result_dir}/bp_bs_date.tmp.duckdb
    - 最終 DB: {result_dir}/bp_bs_date.duckdb
    - 中間 TSV: {result_dir}/{bp,bs}_date.tsv
"""

import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, get_args

import duckdb
from pydantic import BaseModel

from ddbj_search_converter.config import DATE_CACHE_DB_FILE_NAME, TMP_DATE_CACHE_DB_FILE_NAME, Config
from ddbj_search_converter.duckdb_bulk import load_tsv_into_table, write_rows_to_tsv
from ddbj_search_converter.logging.logger import log_info

# 日付の意味づけが変わる変更を入れたら上げる。既存 DB の値と一致しないときは
# 窓ビルドに入らず全件ビルドへ倒れるので、移行のための特別な手順が要らなくなる。
SCHEMA_VERSION = 1

DateTable = Literal["bp_date", "bs_date"]
DATE_TABLES: tuple[DateTable, ...] = get_args(DateTable)
DATE_COLUMNS = ("accession", "date_created", "date_modified", "date_published")

DateRow = tuple[str, str | None, str | None, str | None]
DateTuple = tuple[str | None, str | None, str | None]


class CacheMeta(BaseModel):
    table_name: str
    schema_version: int
    full_built_at: str | None
    watermark: str | None


def _tmp_db_path(config: Config) -> Path:
    return config.result_dir / TMP_DATE_CACHE_DB_FILE_NAME


def _final_db_path(config: Config) -> Path:
    return config.result_dir / DATE_CACHE_DB_FILE_NAME


def _tsv_path(config: Config, table: DateTable) -> Path:
    return config.result_dir / f"{table}.tsv"


def read_cache_meta(config: Config) -> dict[str, CacheMeta] | None:
    """最終 DB の cache_meta を読む。DB や テーブルが無ければ None を返す。"""
    db_path = _final_db_path(config)
    if not db_path.exists():
        return None

    with duckdb.connect(str(db_path), read_only=True) as conn:
        exists = conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'cache_meta'"
        ).fetchone()
        if exists is None or exists[0] == 0:
            return None

        rows = conn.execute(
            "SELECT table_name, schema_version, full_built_at, watermark FROM cache_meta"
        ).fetchall()

    return {
        row[0]: CacheMeta(table_name=row[0], schema_version=row[1], full_built_at=row[2], watermark=row[3])
        for row in rows
    }


def date_cache_ready(config: Config) -> bool:
    """窓ビルドと JSONL 生成が前提にできる状態かを判定する。"""
    return cache_meta_is_usable(read_cache_meta(config))


def cache_meta_is_usable(meta: dict[str, CacheMeta] | None) -> bool:
    """cache_meta が窓ビルドの土台として使えるかを判定する。

    全件ビルドを一度も通っていない DB や、`SCHEMA_VERSION` が食い違う古い DB は
    「無い」ものとして扱う。前者は accession の取りこぼし、後者は日付の意味の
    ずれをそのまま下流へ流すことになるため。
    """
    if meta is None:
        return False

    for table in DATE_TABLES:
        entry = meta.get(table)
        if entry is None or entry.schema_version != SCHEMA_VERSION:
            return False
        if entry.full_built_at is None or entry.watermark is None:
            return False

    return True


def init_date_cache_db(config: Config, *, seed_from_final: bool) -> None:
    """一時 DB を用意する。

    ``seed_from_final`` が真なら最終 DB を複製して既存行を引き継ぐ (窓ビルド)。
    偽なら空のテーブルを作る (全件ビルド)。
    """
    db_path = _tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    if seed_from_final:
        shutil.copy2(_final_db_path(config), db_path)
        return

    with duckdb.connect(str(db_path)) as conn:
        for table in DATE_TABLES:
            conn.execute(f"""
                CREATE TABLE {table} (
                    accession TEXT NOT NULL,
                    date_created TEXT,
                    date_modified TEXT,
                    date_published TEXT
                )
            """)
        conn.execute("""
            CREATE TABLE cache_meta (
                table_name TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                full_built_at TEXT,
                watermark TEXT
            )
        """)


def write_dates_tsv(config: Config, table: DateTable, rows: Iterable[DateRow]) -> int:
    """PostgreSQL から取得した行を中間 TSV に書き切り、書いた行数を返す。

    rows を消費し切ってから返るので、呼び出し側はこの直後に PostgreSQL 接続を
    閉じてから DuckDB へのロードに進める。
    """
    return write_rows_to_tsv(_tsv_path(config, table), rows)


def load_dates(config: Config, table: DateTable, expected_rows: int, *, replace_existing: bool) -> int:
    """中間 TSV を一時 DB へロードし、投入した行数を返す。"""
    with duckdb.connect(str(_tmp_db_path(config))) as conn:
        return load_tsv_into_table(
            conn,
            table,
            DATE_COLUMNS,
            _tsv_path(config, table),
            expected_rows,
            conflict_key="accession" if replace_existing else None,
        )


def set_cache_meta(
    config: Config,
    table: DateTable,
    *,
    full_built_at: str | None,
    watermark: str,
) -> None:
    """一時 DB の cache_meta を更新する。

    ``full_built_at`` に None を渡すと既存の値を保持する (窓ビルド)。
    """
    with duckdb.connect(str(_tmp_db_path(config))) as conn:
        row = conn.execute(
            "SELECT full_built_at FROM cache_meta WHERE table_name = ?", (table,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO cache_meta VALUES (?, ?, ?, ?)",
                (table, SCHEMA_VERSION, full_built_at, watermark),
            )
            return

        conn.execute(
            """
            UPDATE cache_meta
            SET schema_version = ?, full_built_at = ?, watermark = ?
            WHERE table_name = ?
            """,
            (SCHEMA_VERSION, full_built_at if full_built_at is not None else row[0], watermark, table),
        )


def finalize_date_cache_db(config: Config) -> None:
    db_path = _tmp_db_path(config)

    with duckdb.connect(str(db_path)) as conn:
        for table in DATE_TABLES:
            conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_accession ON {table} (accession)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_modified ON {table} (date_modified)")

    # replace() 自体が atomic な上書きなので、事前に unlink して
    # cache が存在しない瞬間を作らない。
    final_path = _final_db_path(config)
    db_path.replace(final_path)
    log_info(f"date cache finalized: {final_path}")


def fetch_bp_dates_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, DateTuple]:
    return _fetch_dates_from_cache(config, "bp_date", accessions)


def fetch_bs_dates_from_cache(
    config: Config,
    accessions: Iterable[str],
) -> dict[str, DateTuple]:
    return _fetch_dates_from_cache(config, "bs_date", accessions)


def _fetch_dates_from_cache(
    config: Config,
    table: DateTable,
    accessions: Iterable[str],
) -> dict[str, DateTuple]:
    accession_list = list(accessions)
    if not accession_list:
        return {}

    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT accession, date_created, date_modified, date_published
            FROM {table}
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
    return _fetch_accessions_modified_since_from_cache(config, "bp_date", since)


def fetch_bs_accessions_modified_since_from_cache(
    config: Config,
    since: str,
) -> set[str]:
    return _fetch_accessions_modified_since_from_cache(config, "bs_date", since)


def _fetch_accessions_modified_since_from_cache(
    config: Config,
    table: DateTable,
    since: str,
) -> set[str]:
    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT accession
            FROM {table}
            WHERE date_modified >= ?
            """,
            (since,),
        ).fetchall()

    return {row[0] for row in rows}
