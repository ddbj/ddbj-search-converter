"""Show DBLink relation counts CLI.

Queries dblink.tmp.duckdb and outputs (src_type, dst_type) pair counts as JSON.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

import duckdb

from ddbj_search_converter.config import (DBLINK_DB_FILE_NAME,
                                           TMP_DBLINK_DB_FILE_NAME, get_config)


def _get_db_path(const_dir: Path) -> Path:
    """Return the dblink DB path, preferring finalized over tmp."""
    finalized = const_dir.joinpath("dblink", DBLINK_DB_FILE_NAME)
    if finalized.exists():
        return finalized
    return const_dir.joinpath("dblink", TMP_DBLINK_DB_FILE_NAME)


def get_relation_counts(const_dir: Path) -> List[Dict[str, object]]:
    db_path = _get_db_path(const_dir)
    if not db_path.exists():
        print(f"DBLink database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("""
            SELECT src_type, dst_type, COUNT(*) as count
            FROM relation
            GROUP BY src_type, dst_type
            ORDER BY count DESC
        """).fetchall()
    finally:
        con.close()

    return [
        {"src_type": src_type, "dst_type": dst_type, "count": count}
        for src_type, dst_type, count in rows
    ]


def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show DBLink relation counts by (src_type, dst_type) pair as JSON."
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
    if parsed.const_dir is not None:
        config.const_dir = Path(parsed.const_dir)

    results = get_relation_counts(config.const_dir)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
