"""Tests for ddbj_search_converter.cli.debug.show_log module."""
import json

from ddbj_search_converter.cli.debug.show_log import _row_to_dict


class TestRowToDict:
    """Tests for _row_to_dict function."""

    def test_basic_conversion(self) -> None:
        """基本的な行を dict に変換する。"""
        result = _row_to_dict(
            timestamp="2026-01-01 10:00:00.123456",
            run_name="test_run",
            message="Test message",
            extra_json=None,
            error_json=None,
        )

        assert result["timestamp"] == "2026-01-01 10:00:00"
        assert result["run_name"] == "test_run"
        assert result["message"] == "Test message"

    def test_with_extra_json_string(self) -> None:
        """extra_json が文字列の場合。"""
        extra = json.dumps({
            "debug_category": "test_category",
            "accession": "PRJDB1111",
            "file": "/path/to/file",
            "source": "test_source",
            "custom_key": "custom_value",
        })

        result = _row_to_dict(
            timestamp="2026-01-01 10:00:00",
            run_name="test_run",
            message="Test message",
            extra_json=extra,
            error_json=None,
        )

        assert result["debug_category"] == "test_category"
        assert result["accession"] == "PRJDB1111"
        assert result["file"] == "/path/to/file"
        assert result["source"] == "test_source"
        assert result["custom_key"] == "custom_value"

    def test_with_extra_json_dict(self) -> None:
        """extra_json が dict の場合。"""
        extra = {
            "debug_category": "test_category",
            "accession": "PRJDB1111",
        }

        result = _row_to_dict(
            timestamp="2026-01-01 10:00:00",
            run_name="test_run",
            message="Test message",
            extra_json=extra,
            error_json=None,
        )

        assert result["debug_category"] == "test_category"
        assert result["accession"] == "PRJDB1111"

    def test_with_error_json(self) -> None:
        """error_json がある場合。"""
        error = json.dumps({"type": "ValueError", "message": "Test error"})

        result = _row_to_dict(
            timestamp="2026-01-01 10:00:00",
            run_name="test_run",
            message="Test message",
            extra_json=None,
            error_json=error,
        )

        assert result["error"] == {"type": "ValueError", "message": "Test error"}

    def test_with_invalid_extra_json(self) -> None:
        """extra_json が不正な JSON の場合。"""
        result = _row_to_dict(
            timestamp="2026-01-01 10:00:00",
            run_name="test_run",
            message="Test message",
            extra_json="invalid json",
            error_json=None,
        )

        assert "debug_category" not in result
        assert result["message"] == "Test message"

    def test_with_none_timestamp(self) -> None:
        """timestamp が None の場合。"""
        result = _row_to_dict(
            timestamp=None,
            run_name="test_run",
            message="Test message",
            extra_json=None,
            error_json=None,
        )

        assert result["timestamp"] is None
