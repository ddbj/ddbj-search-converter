"""Tests for ES index operations."""

from ddbj_search_converter.es.index import (ALL_INDEXES, get_indexes_for_group,
                                            get_mapping_for_index)


class TestAllIndexes:
    def test_all_indexes_count(self) -> None:
        """All 12 indexes should be defined."""
        assert len(ALL_INDEXES) == 12

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
        ]
        assert ALL_INDEXES == expected


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

    def test_all_group(self) -> None:
        indexes = get_indexes_for_group("all")
        assert len(indexes) == 12


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
        assert "attributes" in props

    def test_sra_mappings(self) -> None:
        for sra_type in [
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
        ]:
            mapping = get_mapping_for_index(sra_type)  # type: ignore
            props = mapping["mappings"]["properties"]
            assert "centerName" in props

    def test_jga_mappings(self) -> None:
        for jga_type in ["jga-study", "jga-dataset", "jga-dac", "jga-policy"]:
            mapping = get_mapping_for_index(jga_type)  # type: ignore
            props = mapping["mappings"]["properties"]
            assert "identifier" in props
            assert "dbXref" in props
