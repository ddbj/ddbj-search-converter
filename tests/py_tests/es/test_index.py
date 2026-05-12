"""Tests for ES index operations."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.index import (
    ALIASES,
    ALL_INDEXES,
    get_indexes_for_group,
    get_mapping_for_index,
    make_physical_index_name,
    swap_aliases,
)


def _make_mock_response(body: Any) -> MagicMock:
    resp = MagicMock()
    resp.body = body
    return resp


class TestAllIndexes:
    def test_all_indexes_count(self) -> None:
        """All 14 indexes should be defined."""
        assert len(ALL_INDEXES) == 14

    def test_all_indexes_list(self) -> None:
        expected = [
            "bioproject",
            "biosample",
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
            "jga-study",
            "jga-dataset",
            "jga-dac",
            "jga-policy",
            "gea",
            "metabobank",
        ]
        assert expected == ALL_INDEXES


class TestGetIndexesForGroup:
    def test_bioproject_group(self) -> None:
        indexes = get_indexes_for_group("bioproject")
        assert indexes == ["bioproject"]

    def test_biosample_group(self) -> None:
        indexes = get_indexes_for_group("biosample")
        assert indexes == ["biosample"]

    def test_sra_group(self) -> None:
        indexes = get_indexes_for_group("sra")
        expected = [
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
        ]
        assert indexes == expected

    def test_jga_group(self) -> None:
        indexes = get_indexes_for_group("jga")
        expected = [
            "jga-study",
            "jga-dataset",
            "jga-dac",
            "jga-policy",
        ]
        assert indexes == expected

    def test_gea_group(self) -> None:
        indexes = get_indexes_for_group("gea")
        assert indexes == ["gea"]

    def test_metabobank_group(self) -> None:
        indexes = get_indexes_for_group("metabobank")
        assert indexes == ["metabobank"]

    def test_all_group(self) -> None:
        indexes = get_indexes_for_group("all")
        assert len(indexes) == 14


class TestGetMappingForIndex:
    def test_bioproject_mapping(self) -> None:
        mapping = get_mapping_for_index("bioproject")
        assert "settings" in mapping
        assert "mappings" in mapping
        props = mapping["mappings"]["properties"]
        assert "objectType" in props

    def test_biosample_mapping(self) -> None:
        mapping = get_mapping_for_index("biosample")
        props = mapping["mappings"]["properties"]
        assert "attributes" not in props
        assert "model" in props
        assert "package" in props
        assert "organization" in props

    def test_sra_mappings(self) -> None:
        for sra_type in [
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
        ]:
            mapping = get_mapping_for_index(sra_type)  # type: ignore[arg-type]
            props = mapping["mappings"]["properties"]
            assert "downloadUrl" not in props

    def test_jga_mappings(self) -> None:
        for jga_type in ["jga-study", "jga-dataset", "jga-dac", "jga-policy"]:
            mapping = get_mapping_for_index(jga_type)  # type: ignore[arg-type]
            props = mapping["mappings"]["properties"]
            assert "identifier" in props
            assert "dbXrefs" in props

    def test_gea_mapping(self) -> None:
        mapping = get_mapping_for_index("gea")
        props = mapping["mappings"]["properties"]
        assert "identifier" in props
        assert "experimentType" in props
        assert "organization" in props
        assert "publication" in props

    def test_metabobank_mapping(self) -> None:
        mapping = get_mapping_for_index("metabobank")
        props = mapping["mappings"]["properties"]
        assert "identifier" in props
        assert "studyType" in props
        assert "experimentType" in props
        assert "submissionType" in props


class TestAliases:
    def test_alias_names(self) -> None:
        assert "sra" in ALIASES
        assert "jga" in ALIASES
        assert "entries" in ALIASES

    def test_sra_alias_contains_all_sra_indexes(self) -> None:
        sra_indexes = [
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
        ]
        assert ALIASES["sra"] == sra_indexes

    def test_jga_alias_contains_all_jga_indexes(self) -> None:
        jga_indexes = [
            "jga-study",
            "jga-dataset",
            "jga-dac",
            "jga-policy",
        ]
        assert ALIASES["jga"] == jga_indexes

    def test_entries_alias_contains_all_indexes(self) -> None:
        assert ALIASES["entries"] == list(ALL_INDEXES)


class TestMakePhysicalIndexName:
    def test_bioproject(self) -> None:
        assert make_physical_index_name("bioproject", "20260413") == "bioproject-20260413"

    def test_sra_run(self) -> None:
        assert make_physical_index_name("sra-run", "20260413") == "sra-run-20260413"

    def test_jga_study(self) -> None:
        assert make_physical_index_name("jga-study", "20260101") == "jga-study-20260101"

    def test_all_indexes_produce_valid_names(self) -> None:
        for idx in ALL_INDEXES:
            name = make_physical_index_name(idx, "20260413")
            assert name == f"{idx}-20260413"
            assert name.startswith(idx)
            assert name.endswith("-20260413")


@pytest.mark.usefixtures("with_logger_isolated")
class TestSwapAliasesVerification:
    """``swap_aliases`` の post-swap verification の挙動を mock client で検証する。

    `update_aliases` 自体は atomic に成功するが、その後 `indices.get_alias` で読み直して
    期待する dated index と一致するかを assert する。一致しない場合は ``RuntimeError``
    を raise する設計。
    """

    @patch("ddbj_search_converter.es.index.get_es_client")
    def test_succeeds_when_aliases_match_expected(
        self, mock_get_client: MagicMock, tmp_path: Any
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        # bioproject group のみ swap する (1 index): new = bioproject-20260512
        exists_resp = MagicMock()
        exists_resp.meta.status = 200
        mock_client.indices.exists.return_value = exists_resp
        mock_client.indices.get_alias.return_value = _make_mock_response({
            "bioproject-20260512": {"aliases": {"bioproject": {}}},
        })

        config = Config(result_dir=tmp_path)
        swap_aliases(config, "20260512", "bioproject")

        # update_aliases は呼ばれた
        mock_client.indices.update_aliases.assert_called_once()

    @patch("ddbj_search_converter.es.index.get_es_client")
    def test_raises_when_alias_target_mismatches(
        self, mock_get_client: MagicMock, tmp_path: Any
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        exists_resp = MagicMock()
        exists_resp.meta.status = 200
        mock_client.indices.exists.return_value = exists_resp
        # get_alias の戻り値が期待 dated index と一致しない (古い index を指したまま)
        mock_client.indices.get_alias.return_value = _make_mock_response({
            "bioproject-20260101": {"aliases": {"bioproject": {}}},
        })

        config = Config(result_dir=tmp_path)
        with pytest.raises(RuntimeError, match="post-verification failed"):
            swap_aliases(config, "20260512", "bioproject")

    @patch("ddbj_search_converter.es.index.get_es_client")
    def test_raises_when_alias_fetch_fails(
        self, mock_get_client: MagicMock, tmp_path: Any
    ) -> None:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        exists_resp = MagicMock()
        exists_resp.meta.status = 200
        mock_client.indices.exists.return_value = exists_resp
        mock_client.indices.get_alias.side_effect = Exception("alias api error")

        config = Config(result_dir=tmp_path)
        with pytest.raises(RuntimeError, match="post-verification failed"):
            swap_aliases(config, "20260512", "bioproject")
