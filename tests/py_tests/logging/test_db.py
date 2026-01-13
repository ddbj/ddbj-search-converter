"""Tests for logging database operations."""
import json
from datetime import date
from pathlib import Path
from typing import Generator

import duckdb
import pytest

from ddbj_search_converter.config import LOG_DB_FILE_NAME, Config
from ddbj_search_converter.logging.db import (
    _get_db_path,
    get_last_successful_run_date,
    init_log_db,
    insert_log_records,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestInitLogDb:
    """Tests for init_log_db function."""

    def test_init_log_db_creates_table(self, test_config: Config) -> None:
        """Test that init_log_db creates the log_records table."""
        init_log_db(test_config)

        db_path = _get_db_path(test_config)
        assert db_path.exists()

        # Verify table exists
        with duckdb.connect(str(db_path)) as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='log_records'"
            ).fetchone()
            assert result is not None

    def test_init_log_db_creates_indexes(self, test_config: Config) -> None:
        """Test that init_log_db creates indexes."""
        init_log_db(test_config)

        db_path = _get_db_path(test_config)

        with duckdb.connect(str(db_path)) as conn:
            # Check indexes exist
            indexes = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
            index_names = [idx[0] for idx in indexes]

            assert "idx_run_name" in index_names
            assert "idx_run_date" in index_names
            assert "idx_log_level" in index_names

    def test_init_log_db_idempotent(self, test_config: Config) -> None:
        """Test that init_log_db can be called multiple times."""
        init_log_db(test_config)
        init_log_db(test_config)  # Should not raise

        db_path = _get_db_path(test_config)
        assert db_path.exists()


class TestInsertLogRecords:
    """Tests for insert_log_records function."""

    def test_insert_log_records(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test inserting log records from JSONL file."""
        # Create a run to generate log records
        with run_logger(run_name="test_insert", config=test_config):
            pass  # Just start and end

        # Verify records were inserted into DuckDB
        db_path = _get_db_path(test_config)
        assert db_path.exists()

        with duckdb.connect(str(db_path), read_only=True) as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM log_records WHERE run_name = 'test_insert'"
            ).fetchone()
            assert result is not None
            assert result[0] == 2  # start and end

    def test_insert_log_records_preserves_data(
        self,
        test_config: Config,
    ) -> None:
        """Test that inserted records preserve all data."""
        # Create JSONL file manually
        log_dir = test_config.result_dir.joinpath("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = log_dir.joinpath("test.log.jsonl")

        record = {
            "timestamp": "2026-01-13T10:30:00+09:00",
            "run_date": "2026-01-13",
            "run_id": "20260113_test_a1b2",
            "run_name": "test_preserve",
            "source": "test.module",
            "log_level": "INFO",
            "message": "Test message",
            "error": None,
            "extra": {"lifecycle": "end", "file": "/test/file.xml"},
        }

        with jsonl_path.open("w", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        # Insert records
        insert_log_records(test_config, jsonl_path)

        # Verify data
        db_path = _get_db_path(test_config)
        with duckdb.connect(str(db_path), read_only=True) as conn:
            result = conn.execute(
                """
                SELECT run_name, log_level, message, extra
                FROM log_records
                WHERE run_name = 'test_preserve'
                """
            ).fetchone()

            assert result is not None
            assert result[0] == "test_preserve"
            assert result[1] == "INFO"
            assert result[2] == "Test message"

            # Check extra field
            extra = json.loads(result[3])
            assert extra["lifecycle"] == "end"
            assert extra["file"] == "/test/file.xml"


class TestGetLastSuccessfulRunDate:
    """Tests for get_last_successful_run_date function."""

    def test_get_last_successful_run_date(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test getting last successful run date."""
        # Create a successful run
        with run_logger(run_name="test_success_date", config=test_config):
            pass

        # Get last successful run date
        result = get_last_successful_run_date(test_config, "test_success_date")

        assert result is not None
        assert isinstance(result, date)

    def test_get_last_successful_run_date_no_records(
        self,
        test_config: Config,
    ) -> None:
        """Test getting last successful run date when no records exist."""
        # Initialize DB but don't insert any records
        init_log_db(test_config)

        result = get_last_successful_run_date(test_config, "nonexistent_run")

        assert result is None

    def test_get_last_successful_run_date_no_db(
        self,
        test_config: Config,
    ) -> None:
        """Test getting last successful run date when DB doesn't exist."""
        result = get_last_successful_run_date(test_config, "any_run")

        assert result is None

    def test_get_last_successful_run_date_failed_run(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test that failed runs are not considered successful."""
        # Create a failed run
        with pytest.raises(ValueError):
            with run_logger(run_name="test_failed_date", config=test_config):
                raise ValueError("Test failure")

        # Get last successful run date - should be None
        result = get_last_successful_run_date(test_config, "test_failed_date")

        assert result is None


class TestDbPath:
    """Tests for database path handling."""

    def test_get_db_path(self, test_config: Config) -> None:
        """Test that _get_db_path returns correct path."""
        db_path = _get_db_path(test_config)

        expected = test_config.result_dir.joinpath(LOG_DB_FILE_NAME)
        assert db_path == expected
