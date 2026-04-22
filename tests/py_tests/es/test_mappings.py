"""Tests for ES mapping generation."""

from ddbj_search_converter.es.mappings import (
    INDEX_SETTINGS,
    get_bioproject_mapping,
    get_biosample_mapping,
    get_gea_mapping,
    get_jga_mapping,
    get_metabobank_mapping,
    get_sra_mapping,
)
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
            "accessibility",
            "dateCreated",
            "dateModified",
            "datePublished",
        ]
        for field in required_fields:
            assert field in mapping, f"Missing field: {field}"

    def test_dbxref_is_disabled(self) -> None:
        """dbXrefs should be disabled object (not searchable)."""
        mapping = get_common_mapping()
        assert mapping["dbXrefs"]["type"] == "object"
        assert mapping["dbXrefs"]["enabled"] is False

    def test_same_as_is_nested(self) -> None:
        """sameAs should be nested with identifier/type/url properties."""
        mapping = get_common_mapping()
        assert mapping["sameAs"]["type"] == "nested"
        props = mapping["sameAs"]["properties"]
        assert props["identifier"]["type"] == "keyword"
        assert props["type"]["type"] == "keyword"
        assert props["url"]["type"] == "keyword"
        assert props["url"]["index"] is False

    def test_properties_is_disabled(self) -> None:
        """properties field should be disabled (not searchable)."""
        mapping = get_common_mapping()
        assert mapping["properties"]["type"] == "object"
        assert mapping["properties"]["enabled"] is False

    def test_organism_is_object(self) -> None:
        """organism should be object type (not nested)."""
        mapping = get_common_mapping()
        assert mapping["organism"]["type"] == "object"
        assert "identifier" in mapping["organism"]["properties"]
        assert "name" in mapping["organism"]["properties"]

    def test_text_fields_have_keyword_subfield(self) -> None:
        """text fields should have keyword sub-field for sorting/aggregation."""
        mapping = get_common_mapping()
        for field in ["name", "title", "description"]:
            assert mapping[field]["type"] == "text"
            assert "fields" in mapping[field]
            assert "keyword" in mapping[field]["fields"]
            assert mapping[field]["fields"]["keyword"]["type"] == "keyword"
            assert "ignore_above" in mapping[field]["fields"]["keyword"]


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

    def test_grant_agency_shares_organization_properties(self) -> None:
        """CP2 会話 2: grant.agency は organization と同一 properties を持つ (共通 helper 経由)。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        agency_props = props["grant"]["properties"]["agency"]["properties"]
        organization_props = props["organization"]["properties"]
        assert agency_props == organization_props
        # role / organizationType / department / url も agency 側に含まれる
        # (Grant.agency では常に None だが mapping は統一)
        for field in ("name", "abbreviation", "role", "organizationType", "department", "url"):
            assert field in agency_props
        # name は text + keyword subfield
        assert agency_props["name"]["type"] == "text"
        assert agency_props["name"]["fields"]["keyword"]["type"] == "keyword"

    def test_organization_name_has_keyword_subfield(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        name = props["organization"]["properties"]["name"]
        assert name["type"] == "text"
        assert name["fields"]["keyword"]["type"] == "keyword"

    def test_publication_title_has_keyword_subfield(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        title = props["publication"]["properties"]["title"]
        assert title["type"] == "text"
        assert title["fields"]["keyword"]["type"] == "keyword"

    def test_grant_title_has_keyword_subfield(self) -> None:
        """Grant.title は共通 helper 昇格により text + keyword subfield に upgrade される。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        title = props["grant"]["properties"]["title"]
        assert title["type"] == "text"
        assert title["fields"]["keyword"]["type"] == "keyword"

    def test_organization_has_department(self) -> None:
        """共通 helper 経由で organization.department フィールドが mapping に含まれる。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert "department" in props["organization"]["properties"]
        assert props["organization"]["properties"]["department"]["type"] == "keyword"


class TestBioSampleMapping:
    def test_has_settings_and_mappings(self) -> None:
        mapping = get_biosample_mapping()
        assert "settings" in mapping
        assert "mappings" in mapping

    def test_has_biosample_specific_fields(self) -> None:
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert "model" in props
        assert "package" in props

    def test_attributes_mapping_removed(self) -> None:
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert "attributes" not in props

    def test_model_is_keyword(self) -> None:
        """model は list[str] として格下げ、keyword 単独で index される。"""
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert props["model"] == {"type": "keyword"}

    def test_package_has_name_and_display_name(self) -> None:
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        pkg = props["package"]
        assert pkg["type"] == "object"
        assert pkg["properties"]["name"] == {"type": "keyword"}
        assert pkg["properties"]["displayName"]["type"] == "keyword"

    def test_package_display_name_is_not_indexed(self) -> None:
        """displayName は表示専用で検索対象外。"""
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert props["package"]["properties"]["displayName"]["index"] is False

    def test_has_organization_from_common_helper(self) -> None:
        """Owner.Name 由来の organization を共通 helper 経由で持つ。"""
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert "organization" in props
        assert props["organization"]["type"] == "nested"
        assert props["organization"]["properties"]["name"]["type"] == "text"
        assert props["organization"]["properties"]["abbreviation"]["type"] == "keyword"

    def test_does_not_include_publication_or_grant(self) -> None:
        """BS 確定形では publication / grant / externalLink は追加しない。"""
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        assert "publication" not in props
        assert "grant" not in props
        assert "externalLink" not in props


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

    def test_all_sra_types_have_common_fields(self) -> None:
        for sra_type in SRA_INDEXES:
            mapping = get_sra_mapping(sra_type)
            props = mapping["mappings"]["properties"]
            assert "identifier" in props
            assert "dbXrefs" in props
            assert "status" in props

    def test_sra_no_download_url(self) -> None:
        for sra_type in SRA_INDEXES:
            mapping = get_sra_mapping(sra_type)
            props = mapping["mappings"]["properties"]
            assert "downloadUrl" not in props

    def test_sra_no_old_specific_fields(self) -> None:
        """Removed fields should not be present in any SRA mapping."""
        removed_fields = [
            "centerName",
            "labName",
            "studyType",
            "runDate",
            "runCenter",
        ]
        for sra_type in SRA_INDEXES:
            mapping = get_sra_mapping(sra_type)
            props = mapping["mappings"]["properties"]
            for field in removed_fields:
                assert field not in props, f"{field} should not be in {sra_type}"

    def test_all_sra_types_have_organization_and_publication(self) -> None:
        """organization / publication は共通 helper 経由で全 type に入る。"""
        for sra_type in SRA_INDEXES:
            mapping = get_sra_mapping(sra_type)
            props = mapping["mappings"]["properties"]
            assert "organization" in props
            assert props["organization"]["type"] == "nested"
            assert "publication" in props
            assert props["publication"]["type"] == "nested"

    def test_sra_experiment_has_library_fields(self) -> None:
        """experiment specific: library / platform 系は keyword。"""
        mapping = get_sra_mapping("sra-experiment")
        props = mapping["mappings"]["properties"]
        for field in ["libraryStrategy", "librarySource", "librarySelection", "libraryLayout", "platform"]:
            assert props[field] == {"type": "keyword"}, f"{field} should be keyword"

    def test_sra_experiment_instrument_model_has_text_keyword_subfield(self) -> None:
        """instrumentModel は自然言語なので text + keyword subfield。"""
        mapping = get_sra_mapping("sra-experiment")
        props = mapping["mappings"]["properties"]
        assert props["instrumentModel"]["type"] == "text"
        assert props["instrumentModel"]["fields"]["keyword"]["type"] == "keyword"
        assert props["instrumentModel"]["fields"]["keyword"]["ignore_above"] == 256

    def test_sra_analysis_has_analysis_type(self) -> None:
        mapping = get_sra_mapping("sra-analysis")
        props = mapping["mappings"]["properties"]
        assert props["analysisType"] == {"type": "keyword"}

    def test_non_experiment_types_have_no_library_fields(self) -> None:
        """experiment 以外では library* / platform / instrumentModel 不在。"""
        for sra_type in ["sra-submission", "sra-study", "sra-run", "sra-sample", "sra-analysis"]:
            props = get_sra_mapping(sra_type)["mappings"]["properties"]
            for field in [
                "libraryStrategy",
                "librarySource",
                "librarySelection",
                "libraryLayout",
                "platform",
                "instrumentModel",
            ]:
                assert field not in props, f"{field} should not be in {sra_type}"

    def test_non_analysis_types_have_no_analysis_type(self) -> None:
        for sra_type in ["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample"]:
            props = get_sra_mapping(sra_type)["mappings"]["properties"]
            assert "analysisType" not in props


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

    def test_all_jga_types_have_organization(self) -> None:
        """organization は全 type 共通 (nested)。"""
        for jga_type in JGA_INDEXES:
            props = get_jga_mapping(jga_type)["mappings"]["properties"]
            assert "organization" in props
            assert props["organization"]["type"] == "nested"

    def test_all_jga_types_have_external_link(self) -> None:
        """externalLink も全 type 共通 (nested)。"""
        for jga_type in JGA_INDEXES:
            props = get_jga_mapping(jga_type)["mappings"]["properties"]
            assert "externalLink" in props
            assert props["externalLink"]["type"] == "nested"

    def test_study_has_publication_and_grant(self) -> None:
        """jga-study のみ publication / grant (nested) を持つ。"""
        props = get_jga_mapping("jga-study")["mappings"]["properties"]
        assert "publication" in props
        assert props["publication"]["type"] == "nested"
        assert "grant" in props
        assert props["grant"]["type"] == "nested"

    def test_study_has_study_type_and_vendor_as_keyword(self) -> None:
        props = get_jga_mapping("jga-study")["mappings"]["properties"]
        assert props["studyType"] == {"type": "keyword"}
        assert props["vendor"] == {"type": "keyword"}

    def test_dataset_has_dataset_type_as_keyword(self) -> None:
        props = get_jga_mapping("jga-dataset")["mappings"]["properties"]
        assert props["datasetType"] == {"type": "keyword"}

    def test_non_study_types_have_no_publication_or_grant(self) -> None:
        for jga_type in ("jga-dataset", "jga-dac", "jga-policy"):
            props = get_jga_mapping(jga_type)["mappings"]["properties"]
            assert "publication" not in props
            assert "grant" not in props

    def test_non_study_types_have_no_vendor_or_study_type(self) -> None:
        for jga_type in ("jga-dataset", "jga-dac", "jga-policy"):
            props = get_jga_mapping(jga_type)["mappings"]["properties"]
            assert "studyType" not in props
            assert "vendor" not in props

    def test_non_dataset_types_have_no_dataset_type(self) -> None:
        for jga_type in ("jga-study", "jga-dac", "jga-policy"):
            props = get_jga_mapping(jga_type)["mappings"]["properties"]
            assert "datasetType" not in props


class TestGeaMapping:
    def test_has_common_properties(self) -> None:
        props = get_gea_mapping()["mappings"]["properties"]
        assert "identifier" in props
        assert "title" in props
        assert "description" in props
        assert "status" in props
        assert "accessibility" in props
        assert "dateCreated" in props
        assert "dateModified" in props
        assert "datePublished" in props

    def test_has_organization_from_common_helper(self) -> None:
        props = get_gea_mapping()["mappings"]["properties"]
        assert props["organization"]["type"] == "nested"

    def test_has_publication_from_common_helper(self) -> None:
        props = get_gea_mapping()["mappings"]["properties"]
        assert props["publication"]["type"] == "nested"
        assert props["publication"]["properties"]["dbType"] == {"type": "keyword"}

    def test_experiment_type_is_keyword(self) -> None:
        props = get_gea_mapping()["mappings"]["properties"]
        assert props["experimentType"] == {"type": "keyword"}

    def test_does_not_include_grant_or_external_link(self) -> None:
        """GEA schema に grant / externalLink は無いため mapping にも含まない。"""
        props = get_gea_mapping()["mappings"]["properties"]
        assert "grant" not in props
        assert "externalLink" not in props

    def test_settings_applied(self) -> None:
        mapping = get_gea_mapping()
        assert mapping["settings"] == INDEX_SETTINGS


class TestMetabobankMapping:
    def test_has_common_properties(self) -> None:
        props = get_metabobank_mapping()["mappings"]["properties"]
        assert "identifier" in props
        assert "title" in props
        assert "description" in props
        assert "status" in props
        assert "accessibility" in props
        assert "dateCreated" in props

    def test_has_organization_and_publication(self) -> None:
        props = get_metabobank_mapping()["mappings"]["properties"]
        assert props["organization"]["type"] == "nested"
        assert props["publication"]["type"] == "nested"

    def test_three_keyword_fields(self) -> None:
        """studyType / experimentType / submissionType は keyword (aggregation 前提)。"""
        props = get_metabobank_mapping()["mappings"]["properties"]
        assert props["studyType"] == {"type": "keyword"}
        assert props["experimentType"] == {"type": "keyword"}
        assert props["submissionType"] == {"type": "keyword"}

    def test_does_not_include_grant_or_external_link(self) -> None:
        props = get_metabobank_mapping()["mappings"]["properties"]
        assert "grant" not in props
        assert "externalLink" not in props

    def test_settings_applied(self) -> None:
        mapping = get_metabobank_mapping()
        assert mapping["settings"] == INDEX_SETTINGS


class TestIndexSettings:
    def test_settings_have_required_fields(self) -> None:
        assert "index" in INDEX_SETTINGS
        assert "refresh_interval" in INDEX_SETTINGS["index"]
        assert "mapping.nested_objects.limit" in INDEX_SETTINGS["index"]
        assert "number_of_shards" in INDEX_SETTINGS["index"]
        assert "number_of_replicas" in INDEX_SETTINGS["index"]

    def test_nested_objects_limit(self) -> None:
        assert INDEX_SETTINGS["index"]["mapping.nested_objects.limit"] == 100000
