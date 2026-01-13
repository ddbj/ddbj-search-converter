"""DuckDB operations for logging."""
import json
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb

from ddbj_search_converter.config import LOG_DB_FILE_NAME, Config


def _get_db_path(config: Config) -> Path:
    return config.result_dir.joinpath(LOG_DB_FILE_NAME)


def init_log_db(config: Config) -> None:
    """Create log_records table if not exists."""
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
                extra JSON
            )
        """)
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_name ON log_records(run_name)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_date ON log_records(run_date)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_log_level ON log_records(log_level)"
        )
    finally:
        con.close()


def insert_log_records(config: Config, jsonl_path: Path) -> None:
    """Insert log records from JSONL file to DuckDB."""
    db_path = _get_db_path(config)

    if not db_path.exists():
        init_log_db(config)

    records = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            records.append((
                record.get("timestamp"),
                record.get("run_date"),
                record.get("run_id"),
                record.get("run_name"),
                record.get("source"),
                record.get("log_level"),
                record.get("message"),
                json.dumps(record.get("error")) if record.get("error") else None,
                json.dumps(record.get("extra")) if record.get("extra") else None,
            ))

    if not records:
        return

    con = duckdb.connect(str(db_path))
    try:
        con.executemany(
            """
            INSERT INTO log_records
            (timestamp, run_date, run_id, run_name, source, log_level,
             message, error, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
    finally:
        con.close()


def get_last_successful_run_date(
    config: Config,
    run_name: str,
) -> Optional[date]:
    """
    Get the last successful run date for a given run_name.

    A successful run is identified by:
    - log_level = 'INFO'
    - extra.lifecycle = 'end'
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
              AND json_extract_string(extra, '$.lifecycle') = 'end'
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
