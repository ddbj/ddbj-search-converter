"""Tests for logging schema."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from ddbj_search_converter.logging.schema import (
    ErrorInfo,
    Extra,
    LogRecord,
)


class TestLogRecord:
    """Tests for LogRecord model."""

    def test_log_record_validation(self) -> None:
        """Test that LogRecord validates correctly with all required fields."""
        record = LogRecord(
            timestamp=datetime(2026, 1, 13, 10, 30, 0, tzinfo=ZoneInfo("Asia/Tokyo")),
            run_date=date(2026, 1, 13),
            run_id="20260113_test_run_a1b2",
            run_name="test_run",
            source="ddbj_search_converter.test",
            log_level="INFO",
            message="Test message",
        )

        assert record.timestamp.year == 2026
        assert record.run_date == date(2026, 1, 13)
        assert record.run_id == "20260113_test_run_a1b2"
        assert record.run_name == "test_run"
        assert record.source == "ddbj_search_converter.test"
        assert record.log_level == "INFO"
        assert record.message == "Test message"
        assert record.error is None
        assert record.extra is not None

    def test_log_record_with_error(self) -> None:
        """Test LogRecord with error information."""
        error_info = ErrorInfo(
            type="ValueError",
            message="Invalid value",
            traceback="Traceback...",
        )
        record = LogRecord(
            timestamp=datetime.now(ZoneInfo("Asia/Tokyo")),
            run_date=date.today(),
            run_id="20260113_test_a1b2",
            run_name="test",
            source="test.module",
            log_level="ERROR",
            message="An error occurred",
            error=error_info,
        )

        assert record.error is not None
        assert record.error.type == "ValueError"
        assert record.error.message == "Invalid value"

    def test_log_record_invalid_log_level(self) -> None:
        """Test that invalid log level raises ValidationError."""
        with pytest.raises(ValidationError):
            LogRecord(
                timestamp=datetime.now(ZoneInfo("Asia/Tokyo")),
                run_date=date.today(),
                run_id="20260113_test_a1b2",
                run_name="test",
                source="test.module",
                log_level="INVALID",  # type: ignore[arg-type]
                message="Test",
            )


class TestExtra:
    """Tests for Extra model."""

    def test_extra_default_values(self) -> None:
        """Test Extra with default values."""
        extra = Extra()

        assert extra.lifecycle is None
        assert extra.file is None
        assert extra.accession is None
        assert extra.index is None
        assert extra.table is None
        assert extra.row is None

    def test_extra_with_values(self) -> None:
        """Test Extra with provided values."""
        extra = Extra(
            lifecycle="start",
            file="/path/to/file.xml",
            accession="PRJDB12345",
            index="bioproject",
            table="relation",
            row=100,
        )

        assert extra.lifecycle == "start"
        assert extra.file == "/path/to/file.xml"
        assert extra.accession == "PRJDB12345"
        assert extra.index == "bioproject"
        assert extra.table == "relation"
        assert extra.row == 100

    def test_extra_allow_additional_fields(self) -> None:
        """Test that Extra allows arbitrary additional fields."""
        extra = Extra(
            lifecycle="end",
            custom_field="custom_value",
            another_field=123,
        )

        assert extra.lifecycle == "end"
        assert extra.model_extra is not None
        assert extra.model_extra.get("custom_field") == "custom_value"
        assert extra.model_extra.get("another_field") == 123

    def test_extra_invalid_lifecycle(self) -> None:
        """Test that invalid lifecycle raises ValidationError."""
        with pytest.raises(ValidationError):
            Extra(lifecycle="invalid")  # type: ignore[arg-type]

    def test_extra_row_must_be_non_negative(self) -> None:
        """Test that row must be >= 0."""
        with pytest.raises(ValidationError):
            Extra(row=-1)


class TestErrorInfo:
    """Tests for ErrorInfo model."""

    def test_error_info_creation(self) -> None:
        """Test ErrorInfo creation with all fields."""
        error = ErrorInfo(
            type="FileNotFoundError",
            message="File not found: /path/to/file",
            traceback="Traceback (most recent call last):\n...",
        )

        assert error.type == "FileNotFoundError"
        assert error.message == "File not found: /path/to/file"
        assert error.traceback is not None

    def test_error_info_without_traceback(self) -> None:
        """Test ErrorInfo creation without traceback."""
        error = ErrorInfo(
            type="ValueError",
            message="Invalid value",
        )

        assert error.type == "ValueError"
        assert error.message == "Invalid value"
        assert error.traceback is None

    def test_error_info_from_exception(self) -> None:
        """Test creating ErrorInfo from an actual exception."""
        try:
            raise ValueError("Test error")
        except ValueError as e:
            error = ErrorInfo(
                type=type(e).__name__,
                message=str(e),
            )

        assert error.type == "ValueError"
        assert error.message == "Test error"
