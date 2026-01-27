"""Log show debug CLI.

Shows detailed DEBUG logs filtered by run_name and debug_category.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import List

import duckdb

from ddbj_search_converter.config import LOG_DB_FILE_NAME, get_config


def _get_db_path(result_dir: Path) -> Path:
    return result_dir.joinpath(LOG_DB_FILE_NAME)


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Show detailed DEBUG logs filtered by run_name and debug_category."
    )
    parser.add_argument(
        "--run-name",
        required=True,
        help="CLI command name (run_name) to filter.",
    )
    parser.add_argument(
        "--category",
        required=True,
        help="Debug category to filter (e.g., invalid_biosample_id).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of entries to show. Default: 100",
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

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Get total count
        total_result = con.execute(
            """
            SELECT COUNT(*)
            FROM log_records
            WHERE run_name = ?
              AND log_level = 'DEBUG'
              AND json_extract_string(extra, '$.debug_category') = ?
            """,
            [parsed.run_name, parsed.category],
        ).fetchone()
        total_count = total_result[0] if total_result else 0

        print()
        print(f"=== DEBUG logs: {parsed.run_name} / {parsed.category} ===")
        print(f"Total: {total_count:,} entries (showing first {parsed.limit})")
        print()

        if total_count == 0:
            print("No matching DEBUG logs found.")
            return

        # Get log entries
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
            LIMIT ?
            """,
            [parsed.run_name, parsed.category, parsed.limit],
        ).fetchall()

        for timestamp, message, extra_json in rows:
            # Format timestamp
            ts_str = str(timestamp)[:19] if timestamp else ""

            # Parse extra fields
            extra_parts = []
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
            print(f"{ts_str}  {message}{extra_str}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
