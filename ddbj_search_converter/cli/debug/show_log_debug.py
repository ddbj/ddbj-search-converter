"""Log show debug CLI.

Shows DEBUG logs filtered by run_name and optionally debug_category.
Output is JSON Lines (one JSON object per line) to stdout.
Metadata and jq examples are printed to stderr.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import duckdb

from ddbj_search_converter.config import LOG_DB_FILE_NAME, get_config


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


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Show DEBUG logs as JSON Lines. Pipe to jq for analysis."
    )
    parser.add_argument(
        "--run-name",
        required=True,
        help="CLI command name (run_name) to filter.",
    )
    parser.add_argument(
        "--category",
        required=False,
        default=None,
        help="Debug category to filter (e.g., invalid_biosample_id). If omitted, show all categories.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of entries to output. 0 = unlimited. Default: 0",
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
        print(f"Log database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        conditions = ["run_name = ?", "log_level = 'DEBUG'"]
        params: List[Any] = [parsed.run_name]

        if parsed.category is not None:
            conditions.append("json_extract_string(extra, '$.debug_category') = ?")
            params.append(parsed.category)
        else:
            conditions.append("json_extract_string(extra, '$.debug_category') IS NOT NULL")

        where_clause = " AND ".join(conditions)

        total_result = con.execute(
            f"SELECT COUNT(*) FROM log_records WHERE {where_clause}",
            params,
        ).fetchone()
        total_count = total_result[0] if total_result else 0

        category_label = parsed.category if parsed.category else "(all)"
        limit_label = f" (showing first {parsed.limit})" if parsed.limit > 0 else ""
        print(f"DEBUG logs: {parsed.run_name} / {category_label}", file=sys.stderr)
        print(f"Total: {total_count:,} entries{limit_label}", file=sys.stderr)

        if total_count == 0:
            print("No matching DEBUG logs found.", file=sys.stderr)
            _print_jq_examples(parsed.run_name)
            return

        _print_jq_examples(parsed.run_name)

        query = f"""
            SELECT timestamp, run_name, message, extra
            FROM log_records
            WHERE {where_clause}
            ORDER BY timestamp DESC
        """
        if parsed.limit > 0:
            query += f" LIMIT {parsed.limit}"

        rows = con.execute(query, params).fetchall()

        for timestamp, run_name, message, extra_json in rows:
            record = _row_to_dict(timestamp, run_name, message, extra_json)
            print(json.dumps(record, ensure_ascii=False))

    finally:
        con.close()


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


if __name__ == "__main__":
    main()
