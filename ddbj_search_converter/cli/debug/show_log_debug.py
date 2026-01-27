"""Log show debug CLI.

Shows DEBUG logs filtered by date, run_name, and run_id.
Output is JSON Lines (default) or human-readable text to stdout.
Metadata and jq examples are printed to stderr.
"""
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from ddbj_search_converter.config import (DATE_FORMAT, LOG_DB_FILE_NAME,
                                           TODAY, get_config)


def _get_db_path(result_dir: Path) -> Path:
    return result_dir.joinpath(LOG_DB_FILE_NAME)


def _row_to_dict(timestamp: Any, run_name: str, message: str, extra_json: Any) -> Dict[str, Any]:
    """Convert a log row to a dict for JSONL output."""
    record: Dict[str, Any] = {
        "timestamp": str(timestamp)[:19] if timestamp else None,
        "run_name": run_name,
        "message": message,
    }

    if extra_json:
        try:
            extra = json.loads(extra_json) if isinstance(extra_json, str) else extra_json
            if extra.get("debug_category"):
                record["debug_category"] = extra["debug_category"]
            if extra.get("accession"):
                record["accession"] = extra["accession"]
            if extra.get("file"):
                record["file"] = extra["file"]
            if extra.get("source"):
                record["source"] = extra["source"]
            for key, value in extra.items():
                if key not in ("debug_category", "accession", "file", "source", "lifecycle"):
                    record[key] = value
        except (json.JSONDecodeError, TypeError):
            pass

    return record


def _select_interactively(label: str, items: List[str], hint_flag: str) -> str:
    """Present a numbered list on stderr and prompt user to select one.

    If stdin is not a TTY, prints the list and exits with an error message
    suggesting the user specify the value with the given hint_flag.
    """
    if sys.stdin.isatty():
        print(f"\n{label}:", file=sys.stderr)
        for i, item in enumerate(items, 1):
            print(f"  {i}) {item}", file=sys.stderr)
        while True:
            try:
                choice = input(f"Select [1-{len(items)}]: ")
            except (EOFError, KeyboardInterrupt):
                print(file=sys.stderr)
                sys.exit(1)
            if choice.isdigit() and 1 <= int(choice) <= len(items):
                return items[int(choice) - 1]
            print(f"  Invalid choice. Enter 1-{len(items)}.", file=sys.stderr)
    else:
        print(f"\n{label}:", file=sys.stderr)
        for item in items:
            print(f"  - {item}", file=sys.stderr)
        print(f"\nMultiple values found. Specify {hint_flag} explicitly.", file=sys.stderr)
        sys.exit(1)


def _resolve_run_name(con: duckdb.DuckDBPyConnection, run_date: date) -> str:
    """Resolve run_name for the given date, interactively if needed."""
    rows = con.execute(
        "SELECT DISTINCT run_name FROM log_records WHERE run_date = ? ORDER BY run_name",
        [run_date],
    ).fetchall()
    names: List[str] = [str(r[0]) for r in rows]

    date_str = run_date.strftime(DATE_FORMAT)
    if len(names) == 0:
        print(f"No runs found for date {date_str}.", file=sys.stderr)
        sys.exit(1)
    if len(names) == 1:
        return names[0]

    return _select_interactively(
        f"Run names for {date_str}",
        names,
        "--run-name",
    )


def _resolve_run_id(
    con: duckdb.DuckDBPyConnection,
    run_date: date,
    run_name: str,
    latest: bool,
) -> str:
    """Resolve run_id for the given date/run_name, optionally picking the latest."""
    rows = con.execute(
        """
        SELECT DISTINCT run_id, MAX(timestamp) AS last_ts
        FROM log_records
        WHERE run_date = ? AND run_name = ?
        GROUP BY run_id
        ORDER BY last_ts DESC
        """,
        [run_date, run_name],
    ).fetchall()
    ids_with_ts: List[tuple[str, str]] = [
        (str(r[0]), str(r[1])[:19]) for r in rows
    ]

    date_str = run_date.strftime(DATE_FORMAT)
    if len(ids_with_ts) == 0:
        print(f"No run_id found for {run_name} on {date_str}.", file=sys.stderr)
        sys.exit(1)
    if len(ids_with_ts) == 1:
        return ids_with_ts[0][0]
    if latest:
        return ids_with_ts[0][0]  # already ordered DESC

    display = [f"{rid}  ({ts})" for rid, ts in ids_with_ts]
    selected = _select_interactively(
        f"Run IDs for {run_name} on {date_str}",
        display,
        "--latest",
    )
    idx = display.index(selected)
    return ids_with_ts[idx][0]


def _format_raw_record(record: Dict[str, Any]) -> str:
    """Format a record as human-readable text."""
    ts = record.get("timestamp", "")
    category = record.get("debug_category", "")
    accession = record.get("accession", "")

    header_parts = [f"[{ts}]"]
    if category:
        header_parts.append(f"[{category}]")
    if accession:
        header_parts.append(accession)

    header = " ".join(header_parts)
    message = record.get("message", "")
    return f"{header}\n  {message}"


def _fetch_debug_logs(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    limit: int,
) -> List[Any]:
    """Fetch debug log rows for a given run_id."""
    query = """
        SELECT timestamp, run_name, message, extra
        FROM log_records
        WHERE run_id = ?
          AND log_level = 'DEBUG'
          AND json_extract_string(extra, '$.debug_category') IS NOT NULL
        ORDER BY timestamp DESC
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    return con.execute(query, [run_id]).fetchall()


def _print_jq_examples(run_name: str) -> None:
    """Print jq usage examples to stderr."""
    cmd = f"show_log_debug --run-name {run_name}"
    print(file=sys.stderr)
    print("jq examples:", file=sys.stderr)
    print("  # Count by category", file=sys.stderr)
    print(f"  {cmd} | jq -s 'group_by(.debug_category) | map({{category: .[0].debug_category, count: length}})'", file=sys.stderr)
    print("  # Filter by category", file=sys.stderr)
    print(f'  {cmd} | jq \'select(.debug_category == "invalid_biosample_id")\'', file=sys.stderr)
    print("  # Unique accessions", file=sys.stderr)
    print(f"  {cmd} | jq -r '.accession // empty' | sort -u", file=sys.stderr)
    print("  # Count per accession", file=sys.stderr)
    print(f"  {cmd} | jq -r '.accession // empty' | sort | uniq -c | sort -rn | head -20", file=sys.stderr)
    print(file=sys.stderr)


def _parse_date(value: str) -> date:
    """Validate and return a date object from YYYYMMDD string."""
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {value!r} (expected YYYYMMDD)"
        ) from exc


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Show DEBUG logs as JSON Lines (default) or human-readable text."
    )
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="Filter by date (YYYYMMDD). Default: today.",
    )
    parser.add_argument(
        "--run-name",
        default=None,
        help="CLI command name (run_name) to filter. If omitted, select interactively.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        default=False,
        help="Auto-select the latest run_id when multiple exist for the same date/run_name.",
    )

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--raw",
        action="store_true",
        default=False,
        help="Human-readable text output.",
    )
    output_group.add_argument(
        "--jsonl",
        action="store_true",
        default=True,
        help="JSON Lines output (default).",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of entries to output. 0 = unlimited. Default: 0",
    )
    parser.add_argument(
        "--result-dir",
        default=None,
        help="Result directory containing log.duckdb. Default: from config",
    )

    parsed = parser.parse_args(args)

    # --raw disables the default --jsonl=True
    if parsed.raw:
        parsed.jsonl = False

    return parsed


def _run_date(parsed_date: Optional[date]) -> date:
    """Return the run_date from parsed arg or TODAY."""
    if parsed_date is not None:
        return parsed_date
    return TODAY


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
        # 1. Resolve run_name
        if parsed.run_name is not None:
            run_name = parsed.run_name
        else:
            run_name = _resolve_run_name(con, run_date)

        # 2. Resolve run_id
        run_id = _resolve_run_id(con, run_date, run_name, parsed.latest)

        # 3. Fetch logs
        rows = _fetch_debug_logs(con, run_id, parsed.limit)

        total_result = con.execute(
            """
            SELECT COUNT(*) FROM log_records
            WHERE run_id = ?
              AND log_level = 'DEBUG'
              AND json_extract_string(extra, '$.debug_category') IS NOT NULL
            """,
            [run_id],
        ).fetchone()
        total_count = total_result[0] if total_result else 0

        limit_label = f" (showing first {parsed.limit})" if parsed.limit > 0 else ""
        print(f"DEBUG logs: {run_name} / run_id={run_id}", file=sys.stderr)
        print(f"Total: {total_count:,} entries{limit_label}", file=sys.stderr)

        if total_count == 0:
            print("No matching DEBUG logs found.", file=sys.stderr)
            if parsed.jsonl:
                _print_jq_examples(run_name)
            return

        if parsed.jsonl:
            _print_jq_examples(run_name)

        # 4. Output
        for timestamp, rn, message, extra_json in rows:
            record = _row_to_dict(timestamp, rn, message, extra_json)
            if parsed.raw:
                print(_format_raw_record(record))
            else:
                print(json.dumps(record, ensure_ascii=False))

    finally:
        con.close()


if __name__ == "__main__":
    main()
