"""DuckDB operations for logging."""

import json
from datetime import date
from pathlib import Path

import duckdb

from ddbj_search_converter.config import LOG_DB_FILE_NAME, Config


def _get_db_path(config: Config) -> Path:
    return config.result_dir.joinpath(LOG_DB_FILE_NAME)


def init_log_db(config: Config) -> None:
    """Create log_records table if not exists.

    ``lifecycle`` is a physical column (derived from ``extra->>'$.lifecycle'``
    at insert time) so that DuckDB can carry a UNIQUE constraint on
    ``(run_id, lifecycle)``.  DuckDB does not support expression-based unique
    indexes (Binder Error: Cannot use json_extract_string in this context),
    so the JSON value has to be materialised in a column.

    Non-lifecycle rows (regular INFO / DEBUG) have ``lifecycle=NULL`` and are
    exempt from the unique constraint per standard SQL semantics (multiple
    NULLs do not collide).  Application layer also enforces "start once +
    end-or-failed once" per run_id (logger.run_logger emits only one terminal
    lifecycle).  See docs/logging.md § DuckDB スキーマと run_id lifecycle.
    """
    db_path = _get_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS log_records (
                timestamp TIMESTAMP,
                run_date DATE,
                run_id TEXT,
                run_name TEXT,
                source TEXT,
                log_level TEXT,
                message TEXT,
                error JSON,
                extra JSON,
                lifecycle TEXT
            )
        """)
        # Migration for DBs created before the lifecycle column existed.
        # DuckDB の ALTER TABLE ADD COLUMN IF NOT EXISTS は v0.9 以降サポート。
        try:
            con.execute("ALTER TABLE log_records ADD COLUMN IF NOT EXISTS lifecycle TEXT")
        except duckdb.CatalogException:
            # Column already exists on older DuckDB without IF NOT EXISTS support
            pass
        con.execute("CREATE INDEX IF NOT EXISTS idx_run_name ON log_records(run_name)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_run_date ON log_records(run_date)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_log_level ON log_records(log_level)")
        # (run_id, lifecycle) UNIQUE for non-NULL lifecycle rows.
        # CREATE UNIQUE INDEX fails if pre-existing rows violate the constraint,
        # so migrate_unique_run_id.py must clean up duplicates first.
        con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_run_lifecycle_unique "
            "ON log_records(run_id, lifecycle)"
        )
    finally:
        con.close()


def insert_log_records(config: Config, jsonl_path: Path) -> None:
    """Insert log records from JSONL file to DuckDB.

    ``extra.lifecycle`` is denormalised into the ``lifecycle`` column at insert
    time so that the ``(run_id, lifecycle)`` UNIQUE index can fire.
    """
    db_path = _get_db_path(config)

    if not db_path.exists():
        init_log_db(config)

    records = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            extra = record.get("extra")
            lifecycle: str | None = None
            if isinstance(extra, dict):
                lifecycle_val = extra.get("lifecycle")
                if isinstance(lifecycle_val, str):
                    lifecycle = lifecycle_val
            records.append(
                (
                    record.get("timestamp"),
                    record.get("run_date"),
                    record.get("run_id"),
                    record.get("run_name"),
                    record.get("source"),
                    record.get("log_level"),
                    record.get("message"),
                    json.dumps(record.get("error")) if record.get("error") else None,
                    json.dumps(extra) if extra else None,
                    lifecycle,
                )
            )

    if not records:
        return

    con = duckdb.connect(str(db_path))
    try:
        # ON CONFLICT DO NOTHING: ``(run_id, lifecycle)`` の UNIQUE 違反 (= 同じ
        # run の重複 start / end / failed 行) は silent skip する。DB レベルで
        # 重複は許さないが、insert は冪等にしたい (並列テスト時の偶発的衝突や、
        # logger 系の二重初期化など) 。INSERT そのものを失敗にしたくない。
        con.executemany(
            """
            INSERT INTO log_records
            (timestamp, run_date, run_id, run_name, source, log_level,
             message, error, extra, lifecycle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            records,
        )
    finally:
        con.close()


def get_last_successful_run_date(
    config: Config,
    run_name: str,
) -> date | None:
    """
    Get the last successful run date for a given run_name.

    A successful run is identified by:
    - log_level = 'INFO'
    - lifecycle = 'end'
    """
    db_path = _get_db_path(config)

    if not db_path.exists():
        return None

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = con.execute(
            """
            SELECT MAX(run_date)
            FROM log_records
            WHERE run_name = ?
              AND log_level = 'INFO'
              AND lifecycle = 'end'
            """,
            [run_name],
        ).fetchone()

        if result and result[0]:
            val = result[0]
            if isinstance(val, date):
                return val
            # DuckDB may return datetime.date or string
            return date.fromisoformat(str(val))

        return None
    finally:
        con.close()
