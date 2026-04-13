"""Tests for ES client helpers."""

from unittest.mock import MagicMock

from ddbj_search_converter.es.client import resolve_alias_to_indexes


class TestResolveAliasToIndexes:
    def test_alias_with_single_index(self) -> None:
        es_client = MagicMock()
        es_client.indices.get_alias.return_value = MagicMock(
            body={"bioproject-20260413": {"aliases": {"bioproject": {}}}}
        )

        result = resolve_alias_to_indexes(es_client, "bioproject")

        assert result == ["bioproject-20260413"]
        es_client.indices.get_alias.assert_called_once_with(name="bioproject")

    def test_alias_with_multiple_indexes(self) -> None:
        es_client = MagicMock()
        es_client.indices.get_alias.return_value = MagicMock(
            body={
                "sra-run-20260412": {"aliases": {"sra": {}}},
                "sra-run-20260413": {"aliases": {"sra": {}}},
            }
        )

        result = resolve_alias_to_indexes(es_client, "sra")

        assert sorted(result) == ["sra-run-20260412", "sra-run-20260413"]

    def test_alias_not_found_returns_empty(self) -> None:
        es_client = MagicMock()
        es_client.indices.get_alias.side_effect = Exception("alias not found")

        result = resolve_alias_to_indexes(es_client, "nonexistent")

        assert result == []
