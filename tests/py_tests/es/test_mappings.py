"""Tests for ES mapping generation."""

from typing import Any

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


def _assert_text_keyword(props: dict[str, Any], field_name: str, ignore_above: int = 256) -> None:
    """Assert that ``field_name`` is text + keyword subfield with given ignore_above."""
    field = props[field_name]
    assert field["type"] == "text", f"{field_name} should be text"
    assert "fields" in field, f"{field_name} missing fields"
    assert "keyword" in field["fields"], f"{field_name} missing keyword subfield"
    assert field["fields"]["keyword"]["type"] == "keyword"
    assert field["fields"]["keyword"]["ignore_above"] == ignore_above


def _assert_text_only(props: dict[str, Any], field_name: str) -> None:
    """Assert that ``field_name`` is text-only (no keyword subfield)."""
    assert props[field_name] == {"type": "text"}, f"{field_name} should be text only"


# common mapping (BioProject の共通フィールド由来) に存在すべき field 名。
# Pydantic schema 上の required と直接の対応はなく (scalar の name/title/
# description/date* は optional)、mapping 側はあくまで「定義の有無」を見る。
_COMMON_MAPPING_EXPECTED_FIELDS: list[str] = [
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


class TestCommonMapping:
    def test_common_mapping_has_required_fields(self) -> None:
        mapping = get_common_mapping()
        for field in _COMMON_MAPPING_EXPECTED_FIELDS:
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

    def test_organism_identifier_is_keyword(self) -> None:
        """organism.identifier (TaxID) は完全一致のみで keyword 維持。"""
        mapping = get_common_mapping()
        assert mapping["organism"]["properties"]["identifier"] == {"type": "keyword"}

    def test_organism_name_has_text_keyword_subfield(self) -> None:
        mapping = get_common_mapping()
        _assert_text_keyword(mapping["organism"]["properties"], "name")

    def test_name_has_text_keyword_subfield(self) -> None:
        mapping = get_common_mapping()
        _assert_text_keyword(mapping, "name")

    def test_title_and_description_are_text_only(self) -> None:
        """title / description は全文検索専用 (facet / sort 不要) で text 単独。"""
        mapping = get_common_mapping()
        _assert_text_only(mapping, "title")
        _assert_text_only(mapping, "description")

    def test_common_mapping_expected_fields_present_in_bioproject_schema(self) -> None:
        """common mapping の expected fields がすべて BioProject Pydantic schema にも存在する。

        BioProject schema 側で共通 field をリネーム or 削除すると、common mapping の
        expected list 側との不整合がここで検出される。
        """
        from ddbj_search_converter.schema import BioProject

        pydantic_field_aliases = {(f.alias or name) for name, f in BioProject.model_fields.items()}
        missing = set(_COMMON_MAPPING_EXPECTED_FIELDS) - pydantic_field_aliases
        assert not missing, f"common mapping が想定する field が BioProject schema に存在しない: {missing}"


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
        assert "projectType" in props
        assert "relevance" in props

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
        """grant.agency は organization と同一 properties を持つ (共通 helper 経由)。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        agency_props = props["grant"]["properties"]["agency"]["properties"]
        organization_props = props["organization"]["properties"]
        assert agency_props == organization_props
        for field in ("name", "abbreviation", "role", "organizationType", "department", "url"):
            assert field in agency_props
        assert agency_props["name"]["type"] == "text"
        assert agency_props["name"]["fields"]["keyword"]["type"] == "keyword"

    def test_organization_name_has_keyword_subfield(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        name = props["organization"]["properties"]["name"]
        assert name["type"] == "text"
        assert name["fields"]["keyword"]["type"] == "keyword"

    def test_publication_title_is_text_only(self) -> None:
        """publication.title は全文検索のみで facet/sort 需要なし → text 単独。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert props["publication"]["properties"]["title"] == {"type": "text"}

    def test_grant_title_is_text_only(self) -> None:
        """Grant.title も全文検索のみで text 単独。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert props["grant"]["properties"]["title"] == {"type": "text"}

    def test_organization_has_department_text_only(self) -> None:
        """organization.department は全文検索のみで text 単独。

        部署単独 facet は UI 設計として稀、Organization.name で機関絞り込み後に
        text 検索する流れが自然。
        """
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert "department" in props["organization"]["properties"]
        assert props["organization"]["properties"]["department"] == {"type": "text"}

    def test_project_type_is_text_keyword(self) -> None:
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        _assert_text_keyword(props, "projectType")

    def test_relevance_is_keyword(self) -> None:
        """relevance は XSD 7 値完全 enum (Medical/Agricultural/...) で tokenize 意味なし。"""
        mapping = get_bioproject_mapping()
        props = mapping["mappings"]["properties"]
        assert props["relevance"] == {"type": "keyword"}

    def test_external_link_structure(self) -> None:
        """externalLink は nested で label は text 単独 / url は keyword 検索対象外。

        label は submitter 自由文 (`JGI Genome Portal` / `GOLD` / `GOLD Project` /
        URL 直書き等) で表記揺れ多数のため text 単独 (api 側が `match` /
        `match_phrase` を投げる経路に合わせる。keyword だと analyzer が走らず
        `externalLinkLabel=GOLD` で `GOLD Project` が hit しない)。
        url は表示用で検索しないため `index: False` 維持。
        """
        mapping = get_bioproject_mapping()
        external_link = mapping["mappings"]["properties"]["externalLink"]
        assert external_link["type"] == "nested"
        link_props = external_link["properties"]
        assert link_props["label"] == {"type": "text"}
        assert link_props["url"] == {"type": "keyword", "index": False}
        assert set(link_props.keys()) == {"label", "url"}

    def test_pydantic_fields_present_in_mapping(self) -> None:
        """BioProject Pydantic schema の全 field が ES mapping に存在する。

        schema に field を追加したが mapping への反映が漏れている場合の検出 meta-test。
        """
        from ddbj_search_converter.schema import BioProject

        mapping_props = get_bioproject_mapping()["mappings"]["properties"]
        pydantic_field_aliases = {(f.alias or name) for name, f in BioProject.model_fields.items()}
        missing = pydantic_field_aliases - set(mapping_props.keys())
        assert not missing, f"BioProject Pydantic schema field が ES mapping に存在しない: {missing}"


class TestBioSampleMapping:
    def test_has_settings_and_mappings(self) -> None:
        mapping = get_biosample_mapping()
        assert "settings" in mapping
        assert "mappings" in mapping

    def test_has_biosample_specific_fields(self) -> None:
        mapping = get_biosample_mapping()
        props = mapping["mappings"]["properties"]
        for field in ("model", "package", "derivedFrom", "geoLocName", "collectionDate", "host", "strain", "isolate"):
            assert field in props, f"Missing field: {field}"

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

    def test_host_is_text_keyword(self) -> None:
        """host は中程度 cardinality の生物名で facet 化要請があるため text+keyword 維持。"""
        props = get_biosample_mapping()["mappings"]["properties"]
        _assert_text_keyword(props, "host")

    def test_strain_is_text_only(self) -> None:
        """strain は `C57BL/6J` / `C57BL/6` / `C57BL6` 等の表記揺れが多く、
        `.keyword` での集計・term 検索が成立しないため text 単独。
        """
        props = get_biosample_mapping()["mappings"]["properties"]
        _assert_text_only(props, "strain")

    def test_isolate_is_text_only(self) -> None:
        """isolate は submitter 自由文 (検体名・cell line・"not provided" 等の
        placeholder 混在) で値がほぼユニーク。`.keyword` index を生やしても
        facet/term 用途に使えないため text 単独。
        """
        props = get_biosample_mapping()["mappings"]["properties"]
        _assert_text_only(props, "isolate")

    def test_geo_loc_name_is_text_only(self) -> None:
        """geoLocName は 'Japan:Kagawa, Aji city, Seto Inland Sea' のような階層値で
        raw facet が 1 sample 1 bucket となり集計意味なし → text 単独。
        """
        props = get_biosample_mapping()["mappings"]["properties"]
        _assert_text_only(props, "geoLocName")

    def test_collection_date_is_text_only(self) -> None:
        """collectionDate は placeholder ('missing'/'N/A') / format 混入 ('1991-10-17'/'2012'/'24-SEP-2004') で
        raw facet / lexical sort が意味を持たないため text 単独。
        """
        props = get_biosample_mapping()["mappings"]["properties"]
        _assert_text_only(props, "collectionDate")

    def test_derived_from_is_nested(self) -> None:
        """derivedFrom は biosample Xref list で facet 対象、sameAs と同じ nested pattern。"""
        props = get_biosample_mapping()["mappings"]["properties"]
        df = props["derivedFrom"]
        assert df["type"] == "nested"
        assert df["properties"]["identifier"] == {"type": "keyword"}
        assert df["properties"]["type"] == {"type": "keyword"}
        assert df["properties"]["url"]["type"] == "keyword"
        assert df["properties"]["url"]["index"] is False

    def test_pydantic_fields_present_in_mapping(self) -> None:
        """BioSample Pydantic schema の全 field が ES mapping に存在する。"""
        from ddbj_search_converter.schema import BioSample

        mapping_props = get_biosample_mapping()["mappings"]["properties"]
        pydantic_field_aliases = {(f.alias or name) for name, f in BioSample.model_fields.items()}
        missing = pydantic_field_aliases - set(mapping_props.keys())
        assert not missing, f"BioSample Pydantic schema field が ES mapping に存在しない: {missing}"


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
        """Removed fields should not be present in any SRA mapping.

        labName は独立 field を持たず、Organization.department に格納する。
        """
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
        """experiment specific: library / platform 系は text + keyword。"""
        mapping = get_sra_mapping("sra-experiment")
        props = mapping["mappings"]["properties"]
        for field in [
            "libraryStrategy",
            "librarySource",
            "librarySelection",
            "libraryLayout",
            "platform",
        ]:
            _assert_text_keyword(props, field)

    def test_sra_experiment_instrument_model_has_text_keyword_subfield(self) -> None:
        """instrumentModel は自然言語なので text + keyword subfield。"""
        mapping = get_sra_mapping("sra-experiment")
        props = mapping["mappings"]["properties"]
        _assert_text_keyword(props, "instrumentModel")

    def test_sra_experiment_library_name_and_protocol(self) -> None:
        """libraryName は値がほぼユニークな submitter 自由文で `.keyword` index
        コストが利得を上回るため text 単独。
        libraryConstructionProtocol は長文で facet 向かず text 単独。
        """
        props = get_sra_mapping("sra-experiment")["mappings"]["properties"]
        _assert_text_only(props, "libraryName")
        _assert_text_only(props, "libraryConstructionProtocol")

    def test_sra_analysis_has_analysis_type(self) -> None:
        """analysisType も text + keyword。"""
        mapping = get_sra_mapping("sra-analysis")
        props = mapping["mappings"]["properties"]
        _assert_text_keyword(props, "analysisType")

    def test_non_experiment_types_have_no_library_fields(self) -> None:
        """experiment 以外では library* / platform / instrumentModel / libraryName /
        libraryConstructionProtocol 不在。"""
        for sra_type in ["sra-submission", "sra-study", "sra-run", "sra-sample", "sra-analysis"]:
            props = get_sra_mapping(sra_type)["mappings"]["properties"]
            for field in [
                "libraryStrategy",
                "librarySource",
                "librarySelection",
                "libraryLayout",
                "platform",
                "instrumentModel",
                "libraryName",
                "libraryConstructionProtocol",
            ]:
                assert field not in props, f"{field} should not be in {sra_type}"

    def test_non_analysis_types_have_no_analysis_type(self) -> None:
        for sra_type in ["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample"]:
            props = get_sra_mapping(sra_type)["mappings"]["properties"]
            assert "analysisType" not in props

    def test_sra_sample_has_collection_date_geo_loc_derived_from(self) -> None:
        """sra-sample は BioSample と揃えて collectionDate/geoLocName は text 単独、
        derivedFrom は nested (biosample Xref)。
        """
        props = get_sra_mapping("sra-sample")["mappings"]["properties"]
        _assert_text_only(props, "collectionDate")
        _assert_text_only(props, "geoLocName")
        df = props["derivedFrom"]
        assert df["type"] == "nested"
        assert df["properties"]["identifier"] == {"type": "keyword"}
        assert df["properties"]["type"] == {"type": "keyword"}
        assert df["properties"]["url"]["type"] == "keyword"
        assert df["properties"]["url"]["index"] is False

    def test_non_sample_types_have_no_sample_fields(self) -> None:
        """sra-sample 以外では collectionDate / geoLocName / derivedFrom 不在。"""
        for sra_type in ["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-analysis"]:
            props = get_sra_mapping(sra_type)["mappings"]["properties"]
            for field in ("collectionDate", "geoLocName", "derivedFrom"):
                assert field not in props, f"{field} should not be in {sra_type}"

    def test_sra_organization_department_is_text_only(self) -> None:
        """SRA Organization.department (lab_name 受け入れ先) も text 単独。"""
        for sra_type in SRA_INDEXES:
            props = get_sra_mapping(sra_type)["mappings"]["properties"]
            assert props["organization"]["properties"]["department"] == {"type": "text"}

    def test_pydantic_fields_covered_by_any_index(self) -> None:
        """SRA Pydantic schema の全 field が SRA_INDEXES のいずれかの mapping に存在する。

        sub-type 間で field が分散しているため (library 系は experiment のみ等)、
        union で「どこにも入っていない field」を検出する。
        """
        from ddbj_search_converter.schema import SRA

        union_props: set[str] = set()
        for sra_type in SRA_INDEXES:
            union_props.update(get_sra_mapping(sra_type)["mappings"]["properties"].keys())
        pydantic_field_aliases = {(f.alias or name) for name, f in SRA.model_fields.items()}
        missing = pydantic_field_aliases - union_props
        assert not missing, f"SRA Pydantic schema field がどの sub-type mapping にも存在しない: {missing}"


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

    def test_jga_external_link_structure(self) -> None:
        """JGA 全 4 type で externalLink が nested + label text + url keyword/index:False。

        BioProject 側と同じ common helper 由来だが、4 type に独立に merge されるため
        merge 過程で property が欠落していないかを type ごとに検証する。
        """
        for jga_type in JGA_INDEXES:
            props = get_jga_mapping(jga_type)["mappings"]["properties"]
            external_link = props["externalLink"]
            assert external_link["type"] == "nested", f"{jga_type}: externalLink should be nested"
            link_props = external_link["properties"]
            assert link_props["label"] == {"type": "text"}, f"{jga_type}: label should be text only"
            assert link_props["url"] == {"type": "keyword", "index": False}, (
                f"{jga_type}: url should be non-indexed keyword"
            )
            assert set(link_props.keys()) == {"label", "url"}, f"{jga_type}: unexpected externalLink sub-properties"

    def test_study_has_publication_and_grant(self) -> None:
        """jga-study のみ publication / grant (nested) を持つ。"""
        props = get_jga_mapping("jga-study")["mappings"]["properties"]
        assert "publication" in props
        assert props["publication"]["type"] == "nested"
        assert "grant" in props
        assert props["grant"]["type"] == "nested"

    def test_study_has_study_type_and_vendor_text_keyword(self) -> None:
        """studyType / vendor は text + keyword。"""
        props = get_jga_mapping("jga-study")["mappings"]["properties"]
        _assert_text_keyword(props, "studyType")
        _assert_text_keyword(props, "vendor")

    def test_dataset_has_dataset_type_text_keyword(self) -> None:
        """datasetType も text + keyword。"""
        props = get_jga_mapping("jga-dataset")["mappings"]["properties"]
        _assert_text_keyword(props, "datasetType")

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

    def test_pydantic_fields_covered_by_any_index(self) -> None:
        """JGA Pydantic schema の全 field が JGA_INDEXES のいずれかの mapping に存在する。

        sub-type 間で field が分散しているため、union で整合をチェックする。
        """
        from ddbj_search_converter.schema import JGA

        union_props: set[str] = set()
        for jga_type in JGA_INDEXES:
            union_props.update(get_jga_mapping(jga_type)["mappings"]["properties"].keys())
        pydantic_field_aliases = {(f.alias or name) for name, f in JGA.model_fields.items()}
        missing = pydantic_field_aliases - union_props
        assert not missing, f"JGA Pydantic schema field がどの sub-type mapping にも存在しない: {missing}"


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

    def test_experiment_type_text_keyword(self) -> None:
        """experimentType も text + keyword。"""
        props = get_gea_mapping()["mappings"]["properties"]
        _assert_text_keyword(props, "experimentType")

    def test_does_not_include_grant_or_external_link(self) -> None:
        """GEA schema に grant / externalLink は無いため mapping にも含まない。"""
        props = get_gea_mapping()["mappings"]["properties"]
        assert "grant" not in props
        assert "externalLink" not in props

    def test_settings_applied(self) -> None:
        mapping = get_gea_mapping()
        assert mapping["settings"] == INDEX_SETTINGS

    def test_pydantic_fields_present_in_mapping(self) -> None:
        """GEA Pydantic schema の全 field が ES mapping に存在する。"""
        from ddbj_search_converter.schema import GEA

        mapping_props = get_gea_mapping()["mappings"]["properties"]
        pydantic_field_aliases = {(f.alias or name) for name, f in GEA.model_fields.items()}
        missing = pydantic_field_aliases - set(mapping_props.keys())
        assert not missing, f"GEA Pydantic schema field が ES mapping に存在しない: {missing}"


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

    def test_three_text_keyword_fields(self) -> None:
        """studyType / experimentType / submissionType は text + keyword。"""
        props = get_metabobank_mapping()["mappings"]["properties"]
        _assert_text_keyword(props, "studyType")
        _assert_text_keyword(props, "experimentType")
        _assert_text_keyword(props, "submissionType")

    def test_does_not_include_grant_or_external_link(self) -> None:
        props = get_metabobank_mapping()["mappings"]["properties"]
        assert "grant" not in props
        assert "externalLink" not in props

    def test_settings_applied(self) -> None:
        mapping = get_metabobank_mapping()
        assert mapping["settings"] == INDEX_SETTINGS

    def test_pydantic_fields_present_in_mapping(self) -> None:
        """MetaboBank Pydantic schema の全 field が ES mapping に存在する。"""
        from ddbj_search_converter.schema import MetaboBank

        mapping_props = get_metabobank_mapping()["mappings"]["properties"]
        pydantic_field_aliases = {(f.alias or name) for name, f in MetaboBank.model_fields.items()}
        missing = pydantic_field_aliases - set(mapping_props.keys())
        assert not missing, f"MetaboBank Pydantic schema field が ES mapping に存在しない: {missing}"


class TestIndexSettings:
    """``INDEX_SETTINGS`` の実値を SSOT で pin する。
    docs/elasticsearch.md § bulk insert 中の refresh 無効化 と一致させること。
    """

    def test_settings_have_required_fields(self) -> None:
        assert "index" in INDEX_SETTINGS
        assert "refresh_interval" in INDEX_SETTINGS["index"]
        assert "mapping.nested_objects.limit" in INDEX_SETTINGS["index"]
        assert "number_of_shards" in INDEX_SETTINGS["index"]
        assert "number_of_replicas" in INDEX_SETTINGS["index"]

    def test_nested_objects_limit(self) -> None:
        assert INDEX_SETTINGS["index"]["mapping.nested_objects.limit"] == 100000

    def test_refresh_interval_default_is_1s(self) -> None:
        """通常運用の refresh_interval = 1s (bulk 中は -1 に切替)。"""
        assert INDEX_SETTINGS["index"]["refresh_interval"] == "1s"

    def test_number_of_shards_is_one(self) -> None:
        """単一 node 想定で shard 1 固定。"""
        assert INDEX_SETTINGS["index"]["number_of_shards"] == 1

    def test_number_of_replicas_is_zero(self) -> None:
        """単一 node 構成のため replica 0。"""
        assert INDEX_SETTINGS["index"]["number_of_replicas"] == 0


class TestBulkInsertSettings:
    """``BULK_INSERT_SETTINGS`` の実値を SSOT で pin する。
    docs/elasticsearch.md § bulk insert と一致させること。
    """

    def test_batch_size_is_5000(self) -> None:
        from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS

        assert BULK_INSERT_SETTINGS["batch_size"] == 5000

    def test_bulk_refresh_interval_is_minus_one(self) -> None:
        """bulk insert 中は refresh_interval = -1 (refresh 無効化)。"""
        from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS

        assert BULK_INSERT_SETTINGS["bulk_refresh_interval"] == "-1"

    def test_normal_refresh_interval_matches_index_settings(self) -> None:
        """通常時の refresh_interval は INDEX_SETTINGS の値と一致する。"""
        from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS

        assert BULK_INSERT_SETTINGS["normal_refresh_interval"] == INDEX_SETTINGS["index"]["refresh_interval"]

    def test_thread_count_positive(self) -> None:
        from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS

        assert BULK_INSERT_SETTINGS["thread_count"] >= 1

    def test_request_timeout_seconds(self) -> None:
        """大きい bulk 投入を吸収するため >= 60s を要求 (現状 600)。"""
        from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS

        assert BULK_INSERT_SETTINGS["request_timeout"] >= 60


class TestPublicationDbTypeKeyword:
    """``publication.dbType`` は全 index で ``keyword`` 型で統一されている。"""

    def _publication_props(self, mapping: dict[str, object]) -> dict[str, object]:
        outer = mapping["mappings"]["properties"]  # type: ignore[index]
        return outer["publication"]["properties"]  # type: ignore[index]

    def test_jga_publication_dbtype(self) -> None:
        from ddbj_search_converter.es.mappings.jga import get_jga_mapping

        props = self._publication_props(get_jga_mapping("jga-study"))
        assert props["dbType"]["type"] == "keyword"  # type: ignore[index]

    def test_gea_publication_dbtype(self) -> None:
        from ddbj_search_converter.es.mappings.gea import get_gea_mapping

        props = self._publication_props(get_gea_mapping())
        assert props["dbType"]["type"] == "keyword"  # type: ignore[index]

    def test_metabobank_publication_dbtype(self) -> None:
        from ddbj_search_converter.es.mappings.metabobank import get_metabobank_mapping

        props = self._publication_props(get_metabobank_mapping())
        assert props["dbType"]["type"] == "keyword"  # type: ignore[index]

    def test_bioproject_publication_dbtype(self) -> None:
        from ddbj_search_converter.es.mappings.bioproject import get_bioproject_mapping

        props = self._publication_props(get_bioproject_mapping())
        assert props["dbType"]["type"] == "keyword"  # type: ignore[index]

    def test_sra_publication_dbtype(self) -> None:
        from ddbj_search_converter.es.mappings.sra import get_sra_mapping

        # SRA は entity 別 mapping を持つので submission 代表で。
        props = self._publication_props(get_sra_mapping("sra-submission"))
        assert props["dbType"]["type"] == "keyword"  # type: ignore[index]
