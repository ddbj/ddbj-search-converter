"""Tests for ddbj_search_converter.logging.migrate_unique_run_id.

The migration must:
1. Detect (run_id, lifecycle) duplicates among non-NULL lifecycle rows.
2. Keep exactly one per group based on --keep latest/earliest.
3. Leave NULL-lifecycle rows untouched (regular INFO/DEBUG can repeat).
4. Create the UNIQUE INDEX, blocking subsequent UNIQUE violations.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ddbj_search_converter.config import LOG_DB_FILE_NAME, Config
from ddbj_search_converter.logging.db import init_log_db
from ddbj_search_converter.logging.migrate_unique_run_id import (
    KEEP_CHOICES,
    main,
    migrate,
)


def _insert_raw(db_path: Path, rows: list[tuple[str, str, str, str | None]]) -> None:
    """Insert (run_id, timestamp, log_level, lifecycle) rows skipping the UNIQUE
    constraint by writing directly to a fresh table without the index.

    We rebuild the table without the unique index, populate, then the migration
    will create the index.  This is the only way to seed pre-migration state.
    """
    con = duckdb.connect(str(db_path))
    try:
        con.execute("DROP TABLE IF EXISTS log_records")
        con.execute(
            """
            CREATE TABLE log_records (
                timestamp TIMESTAMP,
                run_date DATE,
                run_id TEXT,
                run_name TEXT,
                source TEXT,
                log_level TEXT,
                message TEXT,
                error JSON,
                extra JSON,
                lifecycle TEXT
            )
            """
        )
        for run_id, timestamp, log_level, lifecycle in rows:
            extra = json.dumps({"lifecycle": lifecycle}) if lifecycle else None
            con.execute(
                """
                INSERT INTO log_records
                (timestamp, run_date, run_id, run_name, source, log_level,
                 message, error, extra, lifecycle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [timestamp, date(2026, 5, 12), run_id, "test_run", "tests", log_level, "msg", None, extra, lifecycle],
            )
    finally:
        con.close()


def _all_rows(db_path: Path) -> list[tuple[Any, ...]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(
            """
            SELECT run_id, timestamp, lifecycle
            FROM log_records
            ORDER BY run_id, timestamp
            """
        ).fetchall()
    finally:
        con.close()


def _indexes(db_path: Path) -> set[str]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'log_records'"
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        con.close()


class TestKeepLatest:
    def test_keeps_max_timestamp_per_group(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("RUN1", "2026-05-12 10:00:00", "INFO", "start"),
                ("RUN1", "2026-05-12 11:00:00", "INFO", "start"),  # dup
                ("RUN1", "2026-05-12 12:00:00", "INFO", "end"),
                ("RUN2", "2026-05-12 13:00:00", "INFO", "start"),
            ],
        )

        n = migrate(db, keep="latest", dry_run=False)
        assert n == 1

        rows = _all_rows(db)
        # RUN1 start は 11:00 が残る、12:00 end と RUN2 start は触らない
        run1_starts = [r for r in rows if r[0] == "RUN1" and r[2] == "start"]
        assert len(run1_starts) == 1
        assert "11:00:00" in str(run1_starts[0][1])

    def test_multiple_duplicate_groups(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 10:01:00", "INFO", "start"),
                ("R1", "2026-05-12 12:00:00", "INFO", "end"),
                ("R1", "2026-05-12 12:01:00", "INFO", "end"),
                ("R2", "2026-05-12 13:00:00", "CRITICAL", "failed"),
                ("R2", "2026-05-12 13:01:00", "CRITICAL", "failed"),
            ],
        )

        n = migrate(db, keep="latest", dry_run=False)
        assert n == 3

        rows = _all_rows(db)
        groups: dict[tuple[str, str | None], int] = {}
        for run_id, _ts, lifecycle in rows:
            key = (run_id, lifecycle)
            groups[key] = groups.get(key, 0) + 1
        for key, count in groups.items():
            assert count == 1, f"group {key} still has {count} rows"


class TestKeepEarliest:
    def test_keeps_min_timestamp(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 11:00:00", "INFO", "start"),
                ("R1", "2026-05-12 12:00:00", "INFO", "start"),
            ],
        )

        n = migrate(db, keep="earliest", dry_run=False)
        assert n == 1

        rows = _all_rows(db)
        starts = [r for r in rows if r[2] == "start"]
        assert len(starts) == 1
        assert "10:00:00" in str(starts[0][1])


class TestNullLifecyclePreserved:
    def test_regular_info_rows_are_left_alone(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 10:30:00", "INFO", None),  # regular log
                ("R1", "2026-05-12 10:31:00", "INFO", None),  # regular log
                ("R1", "2026-05-12 10:32:00", "INFO", None),  # regular log
                ("R1", "2026-05-12 11:00:00", "INFO", "start"),  # dup
            ],
        )

        migrate(db, keep="latest", dry_run=False)
        rows = _all_rows(db)
        null_rows = [r for r in rows if r[2] is None]
        # 3 NULL lifecycle rows untouched
        assert len(null_rows) == 3


class TestDryRun:
    def test_detects_but_does_not_modify(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 11:00:00", "INFO", "start"),
            ],
        )
        before = _all_rows(db)

        n = migrate(db, keep="latest", dry_run=True)
        assert n == 1

        after = _all_rows(db)
        assert before == after  # 行は変化しない
        assert "idx_run_lifecycle_unique" not in _indexes(db)


class TestUniqueIndexCreated:
    def test_index_created_after_cleanup(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 11:00:00", "INFO", "start"),
            ],
        )
        assert "idx_run_lifecycle_unique" not in _indexes(db)

        migrate(db, keep="latest", dry_run=False)

        assert "idx_run_lifecycle_unique" in _indexes(db)

    def test_index_created_when_no_duplicates(self, tmp_path: Path) -> None:
        """重複ゼロでも index は作成される (再実行時のべき等性)。"""
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 12:00:00", "INFO", "end"),
            ],
        )
        n = migrate(db, keep="latest", dry_run=False)
        assert n == 0
        assert "idx_run_lifecycle_unique" in _indexes(db)


class TestEnforcement:
    def test_unique_violation_after_migration(self, tmp_path: Path) -> None:
        """migration 後は (run_id, lifecycle) 重複 INSERT が失敗することを assert。"""
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(
            db,
            [
                ("R1", "2026-05-12 10:00:00", "INFO", "start"),
                ("R1", "2026-05-12 12:00:00", "INFO", "end"),
            ],
        )
        migrate(db, keep="latest", dry_run=False)

        con = duckdb.connect(str(db))
        try:
            with pytest.raises(duckdb.ConstraintException):
                con.execute(
                    """
                    INSERT INTO log_records
                    (timestamp, run_date, run_id, run_name, source, log_level,
                     message, error, extra, lifecycle)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "2026-05-12 13:00:00",
                        date(2026, 5, 12),
                        "R1",
                        "test_run",
                        "tests",
                        "INFO",
                        "duplicate start",
                        None,
                        json.dumps({"lifecycle": "start"}),
                        "start",
                    ],
                )
        finally:
            con.close()


class TestCli:
    def test_main_returns_zero_on_success(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(db, [("R1", "2026-05-12 10:00:00", "INFO", "start")])
        rc = main(["--db", str(db), "--keep", "latest"])
        assert rc == 0

    def test_main_returns_nonzero_on_missing_db(self, tmp_path: Path) -> None:
        rc = main(["--db", str(tmp_path / "missing.duckdb"), "--keep", "latest"])
        assert rc == 1

    def test_invalid_keep_rejected_by_migrate(self, tmp_path: Path) -> None:
        db = tmp_path / LOG_DB_FILE_NAME
        _insert_raw(db, [("R1", "2026-05-12 10:00:00", "INFO", "start")])
        with pytest.raises(ValueError, match="--keep"):
            migrate(db, keep="middle", dry_run=False)


class TestSchemaContract:
    def test_keep_choices_constant(self) -> None:
        assert KEEP_CHOICES == ("latest", "earliest")

    def test_init_log_db_idempotent_with_unique_index(self, tmp_path: Path) -> None:
        """init_log_db を 2 度呼んでも UNIQUE INDEX が登録され続ける。"""
        config = Config(result_dir=tmp_path)
        init_log_db(config)
        init_log_db(config)
        db = tmp_path / LOG_DB_FILE_NAME
        assert "idx_run_lifecycle_unique" in _indexes(db)
