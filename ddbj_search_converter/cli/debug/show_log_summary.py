"""Log summary CLI.

Shows per-run_name summary for a given date: status, duration, and log level counts.
"""
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from ddbj_search_converter.config import DATE_FORMAT, LOG_DB_FILE_NAME, TODAY, get_config


def _parse_date(value: str) -> date:
    """Validate and return a date object from YYYYMMDD string."""
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {value!r} (expected YYYYMMDD)"
        ) from exc


def _run_date(parsed_date: Optional[date]) -> date:
    """Return the run_date from parsed arg or TODAY."""
    if parsed_date is not None:
        return parsed_date
    return TODAY


def _get_db_path(result_dir: Path) -> Path:
    return result_dir.joinpath(LOG_DB_FILE_NAME)


def _fetch_run_ids(
    con: duckdb.DuckDBPyConnection, run_date: date
) -> Dict[str, List[str]]:
    """Fetch run_name -> list of run_ids (latest first) for the given date."""
    rows = con.execute(
        """
        SELECT run_name, run_id, MAX(timestamp) AS last_ts
        FROM log_records
        WHERE run_date = ?
        GROUP BY run_name, run_id
        ORDER BY run_name, last_ts DESC
        """,
        [run_date],
    ).fetchall()

    result: Dict[str, List[str]] = {}
    for run_name, run_id, _ in rows:
        result.setdefault(str(run_name), []).append(str(run_id))
    return result


def _fetch_run_summary(con: duckdb.DuckDBPyConnection, run_id: str) -> Dict[str, Any]:
    """Fetch lifecycle info and log level counts for a single run_id."""
    row = con.execute(
        """
        SELECT
            MAX(CASE WHEN json_extract_string(extra, '$.lifecycle') = 'start'
                THEN timestamp END) AS start_time,
            MAX(CASE WHEN json_extract_string(extra, '$.lifecycle') IN ('end', 'failed')
                THEN timestamp END) AS end_time,
            MAX(CASE WHEN json_extract_string(extra, '$.lifecycle') = 'end'
                THEN 1 ELSE 0 END) AS has_end,
            MAX(CASE WHEN json_extract_string(extra, '$.lifecycle') = 'failed'
                THEN 1 ELSE 0 END) AS has_failed,
            COUNT(CASE WHEN log_level = 'DEBUG' THEN 1 END) AS debug_count,
            COUNT(CASE WHEN log_level = 'INFO' THEN 1 END) AS info_count,
            COUNT(CASE WHEN log_level = 'WARNING' THEN 1 END) AS warning_count,
            COUNT(CASE WHEN log_level = 'ERROR' THEN 1 END) AS error_count,
            COUNT(CASE WHEN log_level = 'CRITICAL' THEN 1 END) AS critical_count
        FROM log_records
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()

    if row is None:
        return {}

    (start_time, end_time, has_end, has_failed,
     debug_count, info_count, warning_count, error_count, critical_count) = row

    if has_failed:
        status = "FAILED"
    elif has_end:
        status = "SUCCESS"
    else:
        status = "IN_PROGRESS"

    start_str = str(start_time)[:19] if start_time else None
    end_str = str(end_time)[:19] if end_time else None

    duration_seconds: Optional[int] = None
    if start_time and end_time:
        try:
            st = datetime.fromisoformat(str(start_time)[:19])
            et = datetime.fromisoformat(str(end_time)[:19])
            duration_seconds = int((et - st).total_seconds())
        except (ValueError, TypeError):
            pass

    return {
        "status": status,
        "start_time": start_str,
        "end_time": end_str,
        "duration_seconds": duration_seconds,
        "log_levels": {
            "DEBUG": debug_count,
            "INFO": info_count,
            "WARNING": warning_count,
            "ERROR": error_count,
            "CRITICAL": critical_count,
        },
    }


def _build_summary(con: duckdb.DuckDBPyConnection, run_date: date) -> Dict[str, Any]:
    """Build the full summary dict for the given date."""
    run_ids_map = _fetch_run_ids(con, run_date)

    runs: List[Dict[str, Any]] = []
    for run_name, run_id_list in run_ids_map.items():
        latest_run_id = run_id_list[0]
        summary = _fetch_run_summary(con, latest_run_id)
        runs.append({
            "run_name": run_name,
            "status": summary.get("status", "IN_PROGRESS"),
            "latest_run_id": latest_run_id,
            "all_run_ids": run_id_list,
            "start_time": summary.get("start_time"),
            "end_time": summary.get("end_time"),
            "duration_seconds": summary.get("duration_seconds"),
            "log_levels": summary.get("log_levels", {}),
        })

    return {
        "date": run_date.isoformat(),
        "runs": runs,
    }


def _format_duration(seconds: Optional[int]) -> str:
    """Format seconds as human-readable duration string."""
    if seconds is None:
        return "-"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    parts = []
    if h > 0:
        parts.append(f"{h}h")
    if m > 0:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def _print_raw(summary: Dict[str, Any]) -> None:
    """Print summary in human-readable format."""
    print()
    print(f"=== Log Summary ({summary['date']}) ===")
    print()

    runs = summary["runs"]
    if not runs:
        print("No runs found.")
        return

    for run in runs:
        run_name = run["run_name"]
        status = run["status"]
        print(f"{run_name}  [{status}]")

        print(f"  run_id (latest): {run['latest_run_id']}")

        all_ids = run["all_run_ids"]
        if len(all_ids) > 1:
            print("  all run_ids:")
            for i, rid in enumerate(all_ids):
                label = " (latest)" if i == 0 else ""
                print(f"    - {rid}{label}")

        start = run["start_time"] or "-"
        end = run["end_time"] or "-"
        duration = _format_duration(run["duration_seconds"])
        print(f"  start:    {start}")
        print(f"  end:      {end}")
        print(f"  duration: {duration}")

        levels = run.get("log_levels", {})
        print("  log levels:")
        for level_name in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            count = levels.get(level_name, 0)
            print(f"    {level_name:<10}: {count:>10,}")

        print()


def _print_json(summary: Dict[str, Any]) -> None:
    """Print summary as JSON."""
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Show per-run_name log summary (status, duration, log level counts)."
    )
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="Filter by date (YYYYMMDD). Default: today.",
    )

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--raw",
        action="store_true",
        default=False,
        help="Human-readable text output.",
    )
    output_group.add_argument(
        "--json",
        action="store_true",
        default=True,
        help="JSON output (default).",
    )

    parser.add_argument(
        "--result-dir",
        default=None,
        help="Result directory containing log.duckdb. Default: from config",
    )

    parsed = parser.parse_args(args)

    if parsed.raw:
        parsed.json = False

    return parsed


def main() -> None:
    """CLI entry point."""
    parsed = parse_args(sys.argv[1:])

    config = get_config()
    if parsed.result_dir is not None:
        config.result_dir = Path(parsed.result_dir)

    db_path = _get_db_path(config.result_dir)

    if not db_path.exists():
        print(f"Log database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    run_date = _run_date(parsed.date)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        summary = _build_summary(con, run_date)

        if parsed.raw:
            _print_raw(summary)
        else:
            _print_json(summary)
    finally:
        con.close()


if __name__ == "__main__":
    main()
