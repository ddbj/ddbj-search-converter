"""Tests for ES mapping generation."""

from ddbj_search_converter.es.mappings import (INDEX_SETTINGS,
                                               get_bioproject_mapping,
                                               get_biosample_mapping,
                                               get_jga_mapping,
                                               get_sra_mapping)
from ddbj_search_converter.es.mappings.common import get_common_mapping
from ddbj_search_converter.es.mappings.jga import JGA_INDEXES
from ddbj_search_converter.es.mappings.sra import SRA_INDEXES


class TestCommonMapping:
    def test_common_mapping_has_required_fields(self) -> None:
        mapping = get_common_mapping()
        required_fields = [
            "identifier",
            "properties",
            "distribution",
            "isPartOf",
            "type",
            "name",
            "url",
            "organism",
            "title",
            "description",
            "dbXrefs",
            "sameAs",
            "status",
            "visibility",
            "dateCreated",
            "dateModified",
            "datePublished",
        ]
        for field in required_fields:
            assert field in mapping, f"Missing field: {field}"

    def test_dbxref_is_nested(self) -> None:
        """dbXrefs should be nested for relationship queries."""
        mapping = get_common_mapping()
        assert mapping["dbXrefs"]["type"] == "nested"
        assert "properties" in mapping["dbXrefs"]
        assert "identifier" in mapping["dbXrefs"]["properties"]
        assert "type" in mapping["dbXrefs"]["properties"]

    def test_properties_is_disabled(self) -> None:
        """properties field should be disabled (not searchable)."""
        mapping = get_common_mapping()
        assert mapping["properties"]["type"] == "object"
        assert mapping["properties"]["enabled"] is False


class TestBioProjectMapping:
    def test_has_settings_and_mappings(self) -> None:
        mapping = get_bioproject_mapping()
        assert "settings" in mapping
        assert "mappings" in mapping
        assert "properties" in mapping["mappings"]

    def test_has_bioproject_specific_fields(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert "objectType" in props
        assert "organization" in props
        assert "publication" in props
        assert "grant" in props
        assert "externalLink" in props

    def test_organization_is_nested(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert props["organization"]["type"] == "nested"

    def test_grant_agency_is_nested(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert props["grant"]["type"] == "nested"
        assert props["grant"]["properties"]["agency"]["type"] == "nested"


class TestBioSampleMapping:
    def test_has_settings_and_mappings(self) -> None:
        mapping = get_biosample_mapping()
        assert "settings" in mapping
        assert "mappings" in mapping

    def test_has_biosample_specific_fields(self) -> None:
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert "attributes" in props
        assert "model" in props
        assert "package" in props

    def test_attributes_is_nested(self) -> None:
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert props["attributes"]["type"] == "nested"


class TestSraMapping:
    def test_all_sra_indexes_defined(self) -> None:
        expected = [
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
        ]
        assert list(SRA_INDEXES) == expected

    def test_all_sra_types_have_center_name(self) -> None:
        for sra_type in SRA_INDEXES:
            mapping = get_sra_mapping(sra_type)
            props = mapping["mappings"]["properties"]
            assert "centerName" in props

    def test_submission_has_lab_name(self) -> None:
        mapping = get_sra_mapping("sra-submission")
        props = mapping["mappings"]["properties"]
        assert "labName" in props

    def test_study_has_study_type(self) -> None:
        mapping = get_sra_mapping("sra-study")
        props = mapping["mappings"]["properties"]
        assert "studyType" in props

    def test_experiment_has_library_fields(self) -> None:
        mapping = get_sra_mapping("sra-experiment")
        props = mapping["mappings"]["properties"]
        assert "instrumentModel" in props
        assert "libraryStrategy" in props
        assert "librarySource" in props
        assert "librarySelection" in props
        assert "libraryLayout" in props

    def test_run_has_run_fields(self) -> None:
        mapping = get_sra_mapping("sra-run")
        props = mapping["mappings"]["properties"]
        assert "runDate" in props
        assert "runCenter" in props

    def test_sample_has_attributes(self) -> None:
        mapping = get_sra_mapping("sra-sample")
        props = mapping["mappings"]["properties"]
        assert "attributes" in props
        assert props["attributes"]["type"] == "nested"

    def test_analysis_has_analysis_type(self) -> None:
        mapping = get_sra_mapping("sra-analysis")
        props = mapping["mappings"]["properties"]
        assert "analysisType" in props


class TestJgaMapping:
    def test_all_jga_indexes_defined(self) -> None:
        expected = [
            "jga-study",
            "jga-dataset",
            "jga-dac",
            "jga-policy",
        ]
        assert list(JGA_INDEXES) == expected

    def test_all_jga_types_have_common_mapping(self) -> None:
        for jga_type in JGA_INDEXES:
            mapping = get_jga_mapping(jga_type)
            props = mapping["mappings"]["properties"]
            assert "identifier" in props
            assert "dbXrefs" in props
            assert "status" in props


class TestIndexSettings:
    def test_settings_have_required_fields(self) -> None:
        assert "index" in INDEX_SETTINGS
        assert "refresh_interval" in INDEX_SETTINGS["index"]
        assert "mapping.nested_objects.limit" in INDEX_SETTINGS["index"]
        assert "number_of_shards" in INDEX_SETTINGS["index"]
        assert "number_of_replicas" in INDEX_SETTINGS["index"]

    def test_nested_objects_limit(self) -> None:
        assert INDEX_SETTINGS["index"]["mapping.nested_objects.limit"] == 100000
