"""Tests for ES bulk insert operations."""

import json
from pathlib import Path

from ddbj_search_converter.es.bulk_insert import generate_bulk_actions


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
        docs = [
            {"identifier": f"ID{i}", "index": i}
            for i in range(10)
        ]
        jsonl_file.write_text("\n".join(json.dumps(d) for d in docs))

        actions = list(generate_bulk_actions(jsonl_file, "test"))

        assert len(actions) == 10
        for i, action in enumerate(actions):
            assert action["_id"] == f"ID{i}"
            assert action["_source"]["index"] == i
