"""Dump debug report CLI.

Creates a debug_log/ directory with all debug information:
- summary.txt: log summary output
- relation_counts.json: DBLink relation counts
- {run_name}_{category}.txt: per debug category logs
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
    """Dump all (run_name, category) debug logs."""
    db_path = _log_db_path(result_dir)
    if not db_path.exists():
        return

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        pairs = con.execute("""
            SELECT DISTINCT
                run_name,
                json_extract_string(extra, '$.debug_category') as category
            FROM log_records
            WHERE log_level = 'DEBUG'
              AND json_extract_string(extra, '$.debug_category') IS NOT NULL
            ORDER BY run_name, category
        """).fetchall()

        if not pairs:
            print("  No debug logs found.", file=sys.stderr)
            return

        for run_name, category in pairs:
            rows = con.execute(
                """
                SELECT
                    timestamp,
                    message,
                    extra
                FROM log_records
                WHERE run_name = ?
                  AND log_level = 'DEBUG'
                  AND json_extract_string(extra, '$.debug_category') = ?
                ORDER BY timestamp DESC
                """,
                [run_name, category],
            ).fetchall()

            total_count = len(rows)

            buf = io.StringIO()
            buf.write(f"\n=== DEBUG logs: {run_name} / {category} ===\n")
            buf.write(f"Total: {total_count:,} entries\n\n")

            for timestamp, message, extra_json in rows:
                ts_str = str(timestamp)[:19] if timestamp else ""
                extra_parts: list[str] = []
                if extra_json:
                    try:
                        extra = json.loads(extra_json) if isinstance(extra_json, str) else extra_json
                        if extra.get("accession"):
                            extra_parts.append(f"accession={extra['accession']}")
                        if extra.get("file"):
                            extra_parts.append(f"file={extra['file']}")
                        if extra.get("source"):
                            extra_parts.append(f"source={extra['source']}")
                    except (json.JSONDecodeError, TypeError):
                        pass
                extra_str = "  " + "  ".join(extra_parts) if extra_parts else ""
                buf.write(f"{ts_str}  {message}{extra_str}\n")

            out_path = output_dir / f"{run_name}_{category}.txt"
            out_path.write_text(buf.getvalue(), encoding="utf-8")
            print(f"  {out_path}", file=sys.stderr)

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

    print("[3/3] debug logs", file=sys.stderr)
    _dump_debug_logs(config.result_dir, output_dir)

    print(file=sys.stderr)
    print(f"Done: {output_dir}/", file=sys.stderr)


if __name__ == "__main__":
    main()
