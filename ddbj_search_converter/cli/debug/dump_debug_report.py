"""Dump debug report CLI.

Creates a debug_log/ directory with all debug information:
- summary.txt: log summary output
- relation_counts.json: DBLink relation counts
- {run_name}_debug.jsonl: per run_name debug logs (JSON Lines)
"""
import argparse
import io
import json
import sys
from contextlib import redirect_stdout
from datetime import timedelta
from pathlib import Path
from typing import List

import duckdb

from ddbj_search_converter.cli.debug.show_log_debug import _row_to_dict
from ddbj_search_converter.config import (LOG_DB_FILE_NAME,
                                           TMP_DBLINK_DB_FILE_NAME, TODAY,
                                           get_config)


def _log_db_path(result_dir: Path) -> Path:
    return result_dir.joinpath(LOG_DB_FILE_NAME)


def _dblink_db_path(const_dir: Path) -> Path:
    return const_dir.joinpath("dblink", TMP_DBLINK_DB_FILE_NAME)


def _dump_summary(result_dir: Path, output_dir: Path) -> None:
    """Dump log summary to summary.txt."""
    from ddbj_search_converter.cli.debug.show_log_summary import (
        print_debug_category_summary, print_log_level_summary,
        print_run_status)

    db_path = _log_db_path(result_dir)
    if not db_path.exists():
        print(f"  [skip] log database not found: {db_path}", file=sys.stderr)
        return

    start_date = TODAY - timedelta(days=29)
    end_date = TODAY

    buf = io.StringIO()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        with redirect_stdout(buf):
            print()
            print(f"=== Log Summary ({start_date} ~ {end_date}) ===")
            print()
            print_run_status(con, start_date, end_date)
            print_debug_category_summary(con, start_date, end_date)
            print_log_level_summary(con, start_date, end_date)
    finally:
        con.close()

    out_path = output_dir / "summary.txt"
    out_path.write_text(buf.getvalue(), encoding="utf-8")
    print(f"  {out_path}", file=sys.stderr)


def _dump_relation_counts(const_dir: Path, output_dir: Path) -> None:
    """Dump relation counts to relation_counts.json."""
    db_path = _dblink_db_path(const_dir)
    if not db_path.exists():
        print(f"  [skip] dblink database not found: {db_path}", file=sys.stderr)
        return

    from ddbj_search_converter.cli.debug.show_dblink_counts import \
        get_relation_counts

    results = get_relation_counts(const_dir)

    out_path = output_dir / "relation_counts.json"
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"  {out_path}", file=sys.stderr)


def _dump_debug_logs(result_dir: Path, output_dir: Path) -> None:
    """Dump all debug logs per run_name as JSONL."""
    db_path = _log_db_path(result_dir)
    if not db_path.exists():
        return

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        run_names = con.execute("""
            SELECT DISTINCT run_name
            FROM log_records
            ORDER BY run_name
        """).fetchall()

        if not run_names:
            print("  No log records found.", file=sys.stderr)
            return

        for (run_name,) in run_names:
            rows = con.execute(
                """
                SELECT
                    timestamp,
                    message,
                    extra
                FROM log_records
                WHERE run_name = ?
                  AND log_level = 'DEBUG'
                  AND json_extract_string(extra, '$.debug_category') IS NOT NULL
                ORDER BY timestamp DESC
                """,
                [run_name],
            ).fetchall()

            out_path = output_dir / f"{run_name}_debug.jsonl"

            with open(out_path, "w", encoding="utf-8") as f:
                for timestamp, message, extra_json in rows:
                    record = _row_to_dict(timestamp, run_name, message, extra_json)
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")

            count = len(rows)
            print(f"  {out_path} ({count:,} entries)", file=sys.stderr)

    finally:
        con.close()


def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump all debug information to a directory."
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for debug files. Default: {result_dir}/debug_log/",
        default=None,
    )
    parser.add_argument(
        "--result-dir",
        help="Result directory containing log.duckdb. Default: from config",
        default=None,
    )
    parser.add_argument(
        "--const-dir",
        help="Const directory containing dblink/dblink.tmp.duckdb. Default: from config",
        default=None,
    )

    return parser.parse_args(args)


def main() -> None:
    parsed = parse_args(sys.argv[1:])

    config = get_config()
    if parsed.result_dir is not None:
        config.result_dir = Path(parsed.result_dir)
    if parsed.const_dir is not None:
        config.const_dir = Path(parsed.const_dir)

    if parsed.output_dir is not None:
        output_dir = Path(parsed.output_dir)
    else:
        output_dir = config.result_dir / "debug_log"

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Dumping debug report to: {output_dir}/", file=sys.stderr)
    print(file=sys.stderr)

    print("[1/3] summary.txt", file=sys.stderr)
    _dump_summary(config.result_dir, output_dir)

    print("[2/3] relation_counts.json", file=sys.stderr)
    _dump_relation_counts(config.const_dir, output_dir)

    print("[3/3] debug logs (JSONL)", file=sys.stderr)
    _dump_debug_logs(config.result_dir, output_dir)

    print(file=sys.stderr)
    print(f"Done: {output_dir}/", file=sys.stderr)


if __name__ == "__main__":
    main()
