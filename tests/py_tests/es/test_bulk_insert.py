"""Tests for ES bulk insert operations."""

import contextlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ddbj_search_converter.es.bulk_insert import (
    BulkInsertResult,
    _sanitize_error_info,
    bulk_insert_jsonl,
    generate_bulk_actions,
)


class TestSanitizeErrorInfo:
    """_sanitize_error_info must produce JSON-serializable dicts
    that survive BulkInsertResult.model_dump_json()."""

    def _assert_pydantic_serializable(self, errors: list[dict]) -> None:  # type: ignore[type-arg]
        """Build a BulkInsertResult and verify model_dump_json succeeds."""
        result = BulkInsertResult(
            index="test",
            total_docs=1,
            success_count=0,
            error_count=1,
            errors=errors,
        )
        # This is the exact call that was failing in production
        result.model_dump_json()

    def test_plain_dict_passes_through(self) -> None:
        info = {"index": {"_id": "ID1", "error": "mapping error"}}
        sanitized = _sanitize_error_info(info)
        assert sanitized == info
        self._assert_pydantic_serializable([sanitized])

    def test_exception_object_at_top_level(self) -> None:
        exc = RuntimeError("connection reset")
        sanitized = _sanitize_error_info(exc)
        assert sanitized["error_type"] == "RuntimeError"
        assert "connection reset" in sanitized["error_message"]
        self._assert_pydantic_serializable([sanitized])

    def test_dict_with_nested_exception_value(self) -> None:
        """The actual failure case: dict values contain ApiError objects."""
        exc = ValueError("simulated ApiError")
        info = {"index": {"_id": "ID1", "status": 400, "error": exc}}
        sanitized = _sanitize_error_info(info)
        assert isinstance(sanitized["index"]["error"], str)
        self._assert_pydantic_serializable([sanitized])

    def test_dict_with_deeply_nested_exception(self) -> None:
        exc = TypeError("deep error")
        info = {"a": {"b": {"c": [exc, "ok"]}}}
        sanitized = _sanitize_error_info(info)
        self._assert_pydantic_serializable([sanitized])

    def test_dict_with_list_containing_exception(self) -> None:
        exc = OSError("disk full")
        info = {"errors": [exc, {"nested": exc}]}
        sanitized = _sanitize_error_info(info)
        self._assert_pydantic_serializable([sanitized])

    def test_dict_with_mixed_serializable_values(self) -> None:
        info = {"str": "ok", "int": 42, "float": 3.14, "bool": True, "none": None}
        sanitized = _sanitize_error_info(info)
        assert sanitized == info
        self._assert_pydantic_serializable([sanitized])

    def test_elasticsearch_api_error_like_object(self) -> None:
        """Simulate elasticsearch.ApiError behavior."""

        class FakeApiError(Exception):
            def __init__(self) -> None:
                super().__init__("ApiError(400, 'mapper_parsing_exception')")
                self.status_code = 400
                self.body = {"error": {"type": "mapper_parsing_exception"}}

        info = {"index": {"_id": "PRJDB99999", "error": FakeApiError()}}
        sanitized = _sanitize_error_info(info)
        self._assert_pydantic_serializable([sanitized])
        assert isinstance(sanitized["index"]["error"], str)


class TestGenerateBulkActions:
    def test_generates_correct_action_format(self, tmp_path: Path) -> None:
        """Test that bulk actions have correct format."""
        jsonl_file = tmp_path / "test.jsonl"
        doc = {
            "identifier": "TEST123",
            "type": "bioproject",
            "title": "Test Title",
        }
        jsonl_file.write_text(json.dumps(doc) + "\n")

        actions = list(generate_bulk_actions(jsonl_file, "bioproject"))

        assert len(actions) == 1
        action = actions[0]
        assert action["_op_type"] == "index"
        assert action["_index"] == "bioproject"
        assert action["_id"] == "TEST123"
        assert action["_source"]["identifier"] == "TEST123"
        assert action["_source"]["type"] == "bioproject"
        assert action["_source"]["title"] == "Test Title"

    def test_skips_empty_lines(self, tmp_path: Path) -> None:
        """Test that empty lines are skipped."""
        jsonl_file = tmp_path / "test.jsonl"
        content = [
            json.dumps({"identifier": "A"}),
            "",
            json.dumps({"identifier": "B"}),
            "   ",
            json.dumps({"identifier": "C"}),
        ]
        jsonl_file.write_text("\n".join(content))

        actions = list(generate_bulk_actions(jsonl_file, "test"))

        assert len(actions) == 3
        assert [a["_id"] for a in actions] == ["A", "B", "C"]

    def test_skips_docs_without_identifier(self, tmp_path: Path) -> None:
        """Test that documents without identifier are skipped."""
        jsonl_file = tmp_path / "test.jsonl"
        content = [
            json.dumps({"identifier": "A", "title": "Has ID"}),
            json.dumps({"title": "No ID"}),
            json.dumps({"identifier": "B", "title": "Has ID"}),
        ]
        jsonl_file.write_text("\n".join(content))

        actions = list(generate_bulk_actions(jsonl_file, "test"))

        assert len(actions) == 2
        assert [a["_id"] for a in actions] == ["A", "B"]

    def test_processes_multiple_docs(self, tmp_path: Path) -> None:
        """Test processing multiple documents."""
        jsonl_file = tmp_path / "test.jsonl"
        docs = [{"identifier": f"ID{i}", "index": i} for i in range(10)]
        jsonl_file.write_text("\n".join(json.dumps(d) for d in docs))

        actions = list(generate_bulk_actions(jsonl_file, "test"))

        assert len(actions) == 10
        for i, action in enumerate(actions):
            assert action["_id"] == f"ID{i}"
            assert action["_source"]["index"] == i


def _make_jsonl_file(tmp_path: Path, docs: list[dict]) -> Path:  # type: ignore[type-arg]
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text("\n".join(json.dumps(d) for d in docs) + "\n")
    return jsonl_file


@patch("ddbj_search_converter.es.bulk_insert.refresh_index")
@patch("ddbj_search_converter.es.bulk_insert.set_refresh_interval")
@patch("ddbj_search_converter.es.bulk_insert.check_index_exists", return_value=True)
@patch("ddbj_search_converter.es.bulk_insert.get_es_client")
class TestBulkInsertJsonl:
    def test_all_success(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        docs = [{"identifier": f"ID{i}"} for i in range(5)]
        jsonl_file = _make_jsonl_file(tmp_path, docs)
        parallel_results = [(True, {"index": {"_id": f"ID{i}"}}) for i in range(5)]

        with patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", return_value=iter(parallel_results)):
            result = bulk_insert_jsonl(test_config, [jsonl_file], "test-index")  # type: ignore[arg-type]

        assert result.success_count == 5
        assert result.error_count == 0
        assert result.total_docs == 5
        assert result.errors == []

    def test_all_failures(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        docs = [{"identifier": f"ID{i}"} for i in range(3)]
        jsonl_file = _make_jsonl_file(tmp_path, docs)
        parallel_results = [(False, {"index": {"_id": f"ID{i}", "error": "mapping error"}}) for i in range(3)]

        with patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", return_value=iter(parallel_results)):
            result = bulk_insert_jsonl(test_config, [jsonl_file], "test-index")  # type: ignore[arg-type]

        assert result.success_count == 0
        assert result.error_count == 3
        assert result.total_docs == 3
        assert len(result.errors) == 3

    def test_mixed_success_and_failure(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        docs = [{"identifier": f"ID{i}"} for i in range(4)]
        jsonl_file = _make_jsonl_file(tmp_path, docs)
        parallel_results = [
            (True, {"index": {"_id": "ID0"}}),
            (False, {"index": {"_id": "ID1", "error": "err"}}),
            (True, {"index": {"_id": "ID2"}}),
            (False, {"index": {"_id": "ID3", "error": "err"}}),
        ]

        with patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", return_value=iter(parallel_results)):
            result = bulk_insert_jsonl(test_config, [jsonl_file], "test-index")  # type: ignore[arg-type]

        assert result.success_count == 2
        assert result.error_count == 2
        assert result.total_docs == 4
        assert len(result.errors) == 2

    def test_error_limit(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        docs = [{"identifier": f"ID{i}"} for i in range(5)]
        jsonl_file = _make_jsonl_file(tmp_path, docs)
        parallel_results = [(False, {"index": {"_id": f"ID{i}", "error": f"err{i}"}}) for i in range(5)]

        with patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", return_value=iter(parallel_results)):
            result = bulk_insert_jsonl(test_config, [jsonl_file], "test-index", max_errors=2)  # type: ignore[arg-type]

        assert result.error_count == 5
        assert len(result.errors) == 2

    def test_empty_jsonl_file(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        jsonl_file = tmp_path / "empty.jsonl"
        jsonl_file.write_text("")

        with patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", return_value=iter([])):
            result = bulk_insert_jsonl(test_config, [jsonl_file], "test-index")  # type: ignore[arg-type]

        assert result.success_count == 0
        assert result.error_count == 0
        assert result.total_docs == 0

    def test_index_not_exists_raises(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        mock_check.return_value = False
        jsonl_file = _make_jsonl_file(tmp_path, [{"identifier": "ID0"}])

        with pytest.raises(Exception, match="nonexistent"):
            bulk_insert_jsonl(test_config, [jsonl_file], "nonexistent")  # type: ignore[arg-type]

    def test_multiple_jsonl_files(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        file1 = tmp_path / "a.jsonl"
        file1.write_text(json.dumps({"identifier": "A"}) + "\n")
        file2 = tmp_path / "b.jsonl"
        file2.write_text(json.dumps({"identifier": "B"}) + "\n")

        call_count = 0

        def fake_parallel_bulk(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return iter([(True, {"index": {"_id": f"file{call_count}"}})])

        with patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", side_effect=fake_parallel_bulk):
            result = bulk_insert_jsonl(test_config, [file1, file2], "test-index")  # type: ignore[arg-type]

        assert result.success_count == 2
        assert result.total_docs == 2
        assert call_count == 2

    def test_refresh_restored_on_error(
        self,
        mock_get_client: MagicMock,
        mock_check: MagicMock,
        mock_set_refresh: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
        test_config: MagicMock,
    ) -> None:
        jsonl_file = _make_jsonl_file(tmp_path, [{"identifier": "ID0"}])

        def raising_parallel_bulk(*args, **kwargs):
            raise RuntimeError("connection error")

        with (
            patch("ddbj_search_converter.es.bulk_insert.helpers.parallel_bulk", side_effect=raising_parallel_bulk),
            contextlib.suppress(RuntimeError),
        ):
            bulk_insert_jsonl(test_config, [jsonl_file], "test-index")  # type: ignore[arg-type]

        assert mock_set_refresh.call_count == 2
        assert mock_refresh.call_count == 1
