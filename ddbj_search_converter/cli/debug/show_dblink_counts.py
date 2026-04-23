"""Show DBLink 無向 edge 数 CLI.

dblink DB (``dbxref`` 半辺化 table) を参照し、type ペアごとの無向 edge 数を
JSON で出力する。1 つの無向 edge は ``dbxref`` に 2 行存在するため、
``LEAST / GREATEST`` で canonical にまとめたうえで ``COUNT(*) / 2`` を出す。
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb

from ddbj_search_converter.config import DBLINK_DB_FILE_NAME, TMP_DBLINK_DB_FILE_NAME, get_config


def _get_db_path(const_dir: Path) -> Path:
    """Return the dblink DB path, preferring finalized over tmp."""
    finalized = const_dir.joinpath("dblink", DBLINK_DB_FILE_NAME)
    if finalized.exists():
        return finalized
    return const_dir.joinpath("dblink", TMP_DBLINK_DB_FILE_NAME)


def get_edge_counts(const_dir: Path) -> list[dict[str, object]]:
    db_path = _get_db_path(const_dir)
    if not db_path.exists():
        print(f"DBLink database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("""
            SELECT
                LEAST(accession_type, linked_type) AS type_a,
                GREATEST(accession_type, linked_type) AS type_b,
                COUNT(*) / 2 AS count
            FROM dbxref
            GROUP BY LEAST(accession_type, linked_type), GREATEST(accession_type, linked_type)
            ORDER BY count DESC
        """).fetchall()
    finally:
        con.close()

    return [{"type_a": type_a, "type_b": type_b, "count": count} for type_a, type_b, count in rows]


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show DBLink 無向 edge 数 by (type_a, type_b) pair as JSON.")

    return parser.parse_args(args)


def main() -> None:
    parse_args(sys.argv[1:])

    config = get_config()

    results = get_edge_counts(config.const_dir)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
