"""Log summary CLI.

Shows summary of logs: run status, debug categories by CLI, and log level counts.
"""
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import duckdb

from ddbj_search_converter.config import LOG_DB_FILE_NAME, TODAY, get_config


def _get_db_path(result_dir: Path) -> Path:
    return result_dir.joinpath(LOG_DB_FILE_NAME)


def _format_date_range(days: int) -> Tuple[date, date]:
    """Get date range for the query."""
    end_date = TODAY
    start_date = TODAY - timedelta(days=days - 1)
    return start_date, end_date


def print_run_status(con: duckdb.DuckDBPyConnection, start_date: date, end_date: date) -> None:
    """Print recent run status (SUCCESS/FAILED)."""
    print("Run Status:")
    print("-" * 70)

    # Get runs with their lifecycle status
    rows = con.execute(
        """
        WITH run_lifecycle AS (
            SELECT
                run_date,
                run_name,
                run_id,
                MAX(CASE WHEN json_extract_string(extra, '$.lifecycle') = 'end' THEN 1 ELSE 0 END) as has_end,
                MAX(CASE WHEN json_extract_string(extra, '$.lifecycle') = 'failed' THEN 1 ELSE 0 END) as has_failed
            FROM log_records
            WHERE run_date >= ? AND run_date <= ?
            GROUP BY run_date, run_name, run_id
        )
        SELECT
            run_date,
            run_name,
            CASE
                WHEN has_end = 1 THEN 'SUCCESS'
                WHEN has_failed = 1 THEN 'FAILED'
                ELSE 'IN_PROGRESS'
            END as status
        FROM run_lifecycle
        ORDER BY run_date DESC, run_name
        LIMIT 30
        """,
        [start_date.isoformat(), end_date.isoformat()],
    ).fetchall()

    if not rows:
        print("  No runs found in the specified period.")
    else:
        for run_date, run_name, status in rows:
            status_mark = "✓" if status == "SUCCESS" else ("✗" if status == "FAILED" else "⋯")
            print(f"  {run_date}  {run_name:<45}  {status_mark} {status}")

    print()


def print_debug_category_summary(con: duckdb.DuckDBPyConnection, start_date: date, end_date: date) -> None:
    """Print debug category counts by CLI."""
    print("Debug Category by CLI:")
    print("-" * 70)
    print(f"  {'run_name':<40} {'category':<25} {'count':>8}")
    print("  " + "-" * 75)

    rows = con.execute(
        """
        SELECT
            run_name,
            json_extract_string(extra, '$.debug_category') as category,
            COUNT(*) as cnt
        FROM log_records
        WHERE run_date >= ? AND run_date <= ?
          AND log_level = 'DEBUG'
          AND json_extract_string(extra, '$.debug_category') IS NOT NULL
        GROUP BY run_name, category
        ORDER BY run_name, cnt DESC
        """,
        [start_date.isoformat(), end_date.isoformat()],
    ).fetchall()

    if not rows:
        print("  No DEBUG logs with debug_category found.")
    else:
        for run_name, category, cnt in rows:
            print(f"  {run_name:<40} {category:<25} {cnt:>8,}")

    print()


def print_log_level_summary(con: duckdb.DuckDBPyConnection, start_date: date, end_date: date) -> None:
    """Print log level counts."""
    print("Log Level Summary:")
    print("-" * 70)

    rows = con.execute(
        """
        SELECT
            log_level,
            COUNT(*) as cnt
        FROM log_records
        WHERE run_date >= ? AND run_date <= ?
        GROUP BY log_level
        ORDER BY
            CASE log_level
                WHEN 'CRITICAL' THEN 1
                WHEN 'ERROR' THEN 2
                WHEN 'WARNING' THEN 3
                WHEN 'INFO' THEN 4
                WHEN 'DEBUG' THEN 5
            END
        """,
        [start_date.isoformat(), end_date.isoformat()],
    ).fetchall()

    if not rows:
        print("  No logs found in the specified period.")
    else:
        for log_level, cnt in rows:
            print(f"  {log_level:<10}: {cnt:>10,}")

    print()


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Show log summary (run status, debug categories, log levels)."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to include in summary. Default: 7",
    )
    parser.add_argument(
        "--result-dir",
        help="Result directory containing log.duckdb. Default: from config",
        default=None,
    )

    return parser.parse_args(args)


def main() -> None:
    """CLI entry point."""
    parsed = parse_args(sys.argv[1:])

    config = get_config()
    if parsed.result_dir is not None:
        config.result_dir = Path(parsed.result_dir)

    db_path = _get_db_path(config.result_dir)

    if not db_path.exists():
        print(f"Log database not found: {db_path}")
        sys.exit(1)

    start_date, end_date = _format_date_range(parsed.days)

    print()
    print(f"=== Log Summary ({start_date} ~ {end_date}) ===")
    print()

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        print_run_status(con, start_date, end_date)
        print_debug_category_summary(con, start_date, end_date)
        print_log_level_summary(con, start_date, end_date)
    finally:
        con.close()


if __name__ == "__main__":
    main()
