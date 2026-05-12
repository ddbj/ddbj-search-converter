"""``log_records`` の ``(run_id, lifecycle)`` UNIQUE 制約導入用 migration。

既存 ``log.duckdb`` に同一 (run_id, lifecycle) の重複行があると
``CREATE UNIQUE INDEX`` が失敗するため、本 script で 1 行に集約してから
``init_log_db`` を流す。``--keep latest`` は最新 timestamp を、
``--keep earliest`` は最古 timestamp を残す。

Usage:

    python -m ddbj_search_converter.logging.migrate_unique_run_id \
        --db /path/to/log.duckdb --keep latest

スクリプトはダウンタイム前提 (converter プロセスが logging していない
ことを確認してから実行)。
"""

import argparse
import sys
from pathlib import Path

import duckdb

KEEP_CHOICES = ("latest", "earliest")


def _ensure_lifecycle_column(con: duckdb.DuckDBPyConnection) -> None:
    """旧 schema (lifecycle 物理 column 無し) に column を足し、JSON 値を埋める。"""
    try:
        con.execute("ALTER TABLE log_records ADD COLUMN IF NOT EXISTS lifecycle TEXT")
    except duckdb.CatalogException:
        pass
    # JSON から物理 column へ転記 (既に lifecycle が入っている行は触らない)
    con.execute(
        """
        UPDATE log_records
        SET lifecycle = json_extract_string(extra, '$.lifecycle')
        WHERE lifecycle IS NULL
          AND json_extract_string(extra, '$.lifecycle') IS NOT NULL
        """
    )


def _detect_duplicates(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str | None, int]]:
    return con.execute(
        """
        SELECT run_id, lifecycle, COUNT(*) AS cnt
        FROM log_records
        WHERE lifecycle IS NOT NULL
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, run_id, lifecycle
        """
    ).fetchall()


def _delete_duplicates(con: duckdb.DuckDBPyConnection, keep: str) -> int:
    aggregate = "MAX(timestamp)" if keep == "latest" else "MIN(timestamp)"
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE _keep_ts AS
        SELECT run_id, lifecycle, {aggregate} AS keep_ts
        FROM log_records
        WHERE lifecycle IS NOT NULL
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
        """
    )
    before = con.execute("SELECT COUNT(*) FROM log_records").fetchone()
    con.execute(
        """
        DELETE FROM log_records
        WHERE rowid IN (
            SELECT lr.rowid
            FROM log_records lr
            JOIN _keep_ts k
              ON lr.run_id = k.run_id
             AND lr.lifecycle = k.lifecycle
             AND lr.timestamp != k.keep_ts
        )
        """
    )
    after = con.execute("SELECT COUNT(*) FROM log_records").fetchone()
    con.execute("DROP TABLE IF EXISTS _keep_ts")
    if before is None or after is None:
        return -1
    return int(before[0]) - int(after[0])


def _create_unique_index(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_run_lifecycle_unique "
        "ON log_records(run_id, lifecycle)"
    )


def migrate(db_path: Path, keep: str, dry_run: bool) -> int:
    """Run the migration. Returns the number of duplicate groups detected."""
    if keep not in KEEP_CHOICES:
        raise ValueError(f"--keep must be one of {KEEP_CHOICES}, got {keep!r}")
    if not db_path.exists():
        raise FileNotFoundError(f"log DB not found: {db_path}")

    con = duckdb.connect(str(db_path))
    try:
        _ensure_lifecycle_column(con)
        dupes = _detect_duplicates(con)
        if not dupes:
            print(f"[migrate] {db_path}: no duplicate (run_id, lifecycle) rows found.")
            if not dry_run:
                _create_unique_index(con)
                print("[migrate] UNIQUE INDEX created (or already exists).")
            return 0

        print(f"[migrate] {db_path}: found {len(dupes)} duplicate groups:")
        for run_id, lifecycle, cnt in dupes[:20]:
            print(f"  run_id={run_id!r} lifecycle={lifecycle!r} count={cnt}")
        if len(dupes) > 20:
            print(f"  ... and {len(dupes) - 20} more")

        if dry_run:
            print("[migrate] dry-run: no rows deleted, no index created.")
            return len(dupes)

        deleted_count = _delete_duplicates(con, keep)
        print(f"[migrate] kept '{keep}' per group; deleted ~{deleted_count} rows.")
        _create_unique_index(con)
        print("[migrate] UNIQUE INDEX created.")
        return len(dupes)
    finally:
        con.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="Path to log.duckdb")
    parser.add_argument(
        "--keep",
        choices=KEEP_CHOICES,
        default="latest",
        help="Which row to keep per (run_id, lifecycle) duplicate group",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect duplicates but do not delete or create index",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        migrate(args.db, args.keep, args.dry_run)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[migrate] error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
