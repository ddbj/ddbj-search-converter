"""Tests for ddbj_search_converter.jsonl.bs module."""

from __future__ import annotations

from typing import Any

import pytest

from ddbj_search_converter.jsonl.bs import (
    normalize_properties,
    parse_accessibility,
    parse_accession,
    parse_bs_package,
    parse_description,
    parse_model,
    parse_name,
    parse_organism,
    parse_organization,
    parse_same_as,
    parse_status,
    parse_title,
    xml_entry_to_bs_instance,
)
from ddbj_search_converter.schema import BioSample, BioSamplePackage, Organization


def _make_sample(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Minimal valid sample dict for testing."""
    base: dict[str, Any] = {
        "accession": "SAMN00000001",
        "Ids": {"Id": {"namespace": "BioSample", "content": "SAMD00000001"}},
        "Description": {},
        "Attributes": {},
    }
    if overrides:
        base.update(overrides)
    return base


class TestParseAccession:
    """Tests for parse_accession function."""

    def test_ncbi_accession(self) -> None:
        sample = _make_sample()
        assert parse_accession(sample, is_ddbj=False) == "SAMN00000001"

    def test_ddbj_accession_from_ids(self) -> None:
        sample = _make_sample()
        assert parse_accession(sample, is_ddbj=True) == "SAMD00000001"

    def test_ddbj_accession_from_list(self) -> None:
        sample = _make_sample()
        sample["Ids"]["Id"] = [
            {"namespace": "Other", "content": "X001"},
            {"namespace": "BioSample", "content": "SAMD00000002"},
        ]
        assert parse_accession(sample, is_ddbj=True) == "SAMD00000002"

    def test_no_ids_raises(self) -> None:
        sample: dict[str, Any] = {"Ids": None}
        with pytest.raises(ValueError):
            parse_accession(sample, is_ddbj=True)

    def test_no_accession_raises(self) -> None:
        sample: dict[str, Any] = {}
        with pytest.raises(ValueError):
            parse_accession(sample, is_ddbj=False)


class TestBug10ContentNone:
    """Bug #10 (fixed): parse_accession の content=None で ValueError を送出。"""

    def test_content_none_should_raise_value_error(self) -> None:
        sample = _make_sample()
        sample["Ids"]["Id"] = {"namespace": "BioSample", "content": None}
        with pytest.raises(ValueError):
            parse_accession(sample, is_ddbj=True)


class TestParseOrganism:
    """Tests for parse_organism function."""

    def test_ncbi_organism(self) -> None:
        sample = _make_sample()
        sample["Description"]["Organism"] = {
            "taxonomy_id": "9606",
            "taxonomy_name": "Homo sapiens",
        }
        result = parse_organism(sample, is_ddbj=False)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_ddbj_organism(self) -> None:
        sample = _make_sample()
        sample["Description"]["Organism"] = {
            "taxonomy_id": "9606",
            "OrganismName": "Homo sapiens",
        }
        result = parse_organism(sample, is_ddbj=True)
        assert result is not None
        assert result.name == "Homo sapiens"

    def test_no_organism(self) -> None:
        sample = _make_sample()
        result = parse_organism(sample, is_ddbj=False)
        assert result is None


class TestParseTitle:
    """Tests for parse_title function."""

    def test_normal_title(self) -> None:
        sample = _make_sample()
        sample["Description"]["Title"] = "Test Sample"
        assert parse_title(sample) == "Test Sample"

    def test_none_title(self) -> None:
        sample = _make_sample()
        assert parse_title(sample) is None


class TestParseName:
    """Tests for parse_name function."""

    def test_normal_name(self) -> None:
        sample = _make_sample()
        sample["Description"]["SampleName"] = "My Sample"
        assert parse_name(sample) == "My Sample"


class TestParseDescription:
    """Tests for parse_description function."""

    def test_string_comment(self) -> None:
        sample = _make_sample()
        sample["Description"]["Comment"] = "Simple comment"
        assert parse_description(sample) == "Simple comment"

    def test_dict_comment_with_paragraph_string(self) -> None:
        sample = _make_sample()
        sample["Description"]["Comment"] = {"Paragraph": "Para text"}
        assert parse_description(sample) == "Para text"

    def test_dict_comment_with_paragraph_list(self) -> None:
        sample = _make_sample()
        sample["Description"]["Comment"] = {"Paragraph": ["Para 1", "Para 2"]}
        assert parse_description(sample) == "Para 1 Para 2"

    def test_no_comment(self) -> None:
        sample = _make_sample()
        assert parse_description(sample) is None

    def test_paragraph_list_with_none(self) -> None:
        """Paragraph リストに None 要素がある場合。"""
        sample = _make_sample()
        sample["Description"]["Comment"] = {"Paragraph": ["Para 1", None, "Para 2"]}
        result = parse_description(sample)
        assert result is not None
        assert "Para 1" in result
        assert "Para 2" in result


class TestParseModel:
    """Tests for parse_model function."""

    def test_string_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": "Generic"}
        models = parse_model(sample)
        assert models == ["Generic"]

    def test_dict_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": "Generic"}}
        models = parse_model(sample)
        assert models == ["Generic"]

    def test_list_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": [{"content": "MIGS.ba"}, "Generic"]}
        models = parse_model(sample)
        assert models == ["MIGS.ba", "Generic"]

    def test_no_models(self) -> None:
        sample = _make_sample()
        assert parse_model(sample) == []

    def test_model_content_zero(self) -> None:
        """content=0 でも Model として取得できる。"""
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": 0}}
        models = parse_model(sample)
        assert models == ["0"]


class TestBug13ModelContentFalsy:
    """Bug #13 (fixed): parse_model の `if content:` が content=0 をスキップする。"""

    def test_content_zero_should_produce_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": 0}}
        models = parse_model(sample)
        assert models == ["0"]

    def test_content_empty_string_should_produce_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": ""}}
        models = parse_model(sample)
        assert models == [""]

    def test_content_none_should_be_skipped(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": None}}
        models = parse_model(sample)
        assert models == []


class TestBug14DescriptionFalsyParagraph:
    """Bug #14 (fixed): parse_description の `if p` が p=0 や p="" をスキップする。"""

    def test_paragraph_zero_should_be_included(self) -> None:
        sample = _make_sample()
        sample["Description"]["Comment"] = {"Paragraph": ["Para 1", 0, "Para 2"]}
        result = parse_description(sample)
        assert result == "Para 1 0 Para 2"

    def test_paragraph_empty_string_should_be_included(self) -> None:
        sample = _make_sample()
        sample["Description"]["Comment"] = {"Paragraph": ["Para 1", "", "Para 2"]}
        result = parse_description(sample)
        assert result == "Para 1  Para 2"

    def test_paragraph_none_should_be_excluded(self) -> None:
        sample = _make_sample()
        sample["Description"]["Comment"] = {"Paragraph": ["Para 1", None, "Para 2"]}
        result = parse_description(sample)
        assert result == "Para 1 Para 2"


class TestL5DdbjPackageNone:
    """§4.11 L5 regression: DDBJ BS の package は常に None。

    旧実装は Models[0] を name/display_name に合成していた (嘘情報) が、
    .claude/docs/es-field-design.md §4.10.4 で削除決定。
    """

    def test_ddbj_with_models_still_returns_none(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": "MIGS.ba"}
        assert parse_bs_package(sample, is_ddbj=True) is None

    def test_ddbj_without_models_returns_none(self) -> None:
        sample = _make_sample()
        assert parse_bs_package(sample, is_ddbj=True) is None

    def test_ddbj_ignores_any_package_element(self) -> None:
        """仮に DDBJ 側に Package 要素が紛れていても DDBJ 分岐では見ない。"""
        sample = _make_sample()
        sample["Package"] = {"content": "Generic", "display_name": "Generic"}
        assert parse_bs_package(sample, is_ddbj=True) is None


class TestParsePackageNcbi:
    """NCBI BS の Package 要素抽出 (string / dict 両形)。"""

    def test_ncbi_string_package(self) -> None:
        sample = _make_sample()
        sample["Package"] = "Generic.1.0"
        pkg = parse_bs_package(sample, is_ddbj=False)
        assert pkg == BioSamplePackage(name="Generic.1.0")

    def test_ncbi_dict_package_with_display_name(self) -> None:
        sample = _make_sample()
        sample["Package"] = {"content": "Generic", "display_name": "Generic.1.0"}
        pkg = parse_bs_package(sample, is_ddbj=False)
        assert pkg == BioSamplePackage(name="Generic", displayName="Generic.1.0")

    def test_ncbi_dict_package_without_display_name(self) -> None:
        sample = _make_sample()
        sample["Package"] = {"content": "Generic"}
        pkg = parse_bs_package(sample, is_ddbj=False)
        assert pkg == BioSamplePackage(name="Generic", displayName=None)

    def test_ncbi_no_package_returns_none(self) -> None:
        sample = _make_sample()
        assert parse_bs_package(sample, is_ddbj=False) is None

    def test_ncbi_dict_package_without_content_returns_none(self) -> None:
        sample = _make_sample()
        sample["Package"] = {"display_name": "Generic.1.0"}
        assert parse_bs_package(sample, is_ddbj=False) is None


class TestParseOrganization:
    """Tests for parse_organization function."""

    def test_ncbi_dict_owner_name_with_abbreviation(self) -> None:
        """NCBI: <Name abbreviation="WUGSC">...</Name>"""
        sample = _make_sample()
        sample["Owner"] = {
            "Name": {
                "content": "Washington University, Genome Sequencing Center",
                "abbreviation": "WUGSC",
            }
        }
        orgs = parse_organization(sample)
        assert orgs == [
            Organization(
                name="Washington University, Genome Sequencing Center",
                abbreviation="WUGSC",
            )
        ]

    def test_ddbj_string_owner_name(self) -> None:
        """DDBJ: <Name>...</Name> (string / 正規化前想定)"""
        sample = _make_sample()
        sample["Owner"] = {"Name": "Tokyo University of Agriculture and Technology"}
        orgs = parse_organization(sample)
        assert orgs == [Organization(name="Tokyo University of Agriculture and Technology")]

    def test_ddbj_normalized_owner_name(self) -> None:
        """_normalize_owner_name 適用後 (dict {content: ...}) でも抽出できる。"""
        sample = _make_sample()
        sample["Owner"] = {"Name": {"content": "DDBJ"}}
        orgs = parse_organization(sample)
        assert orgs == [Organization(name="DDBJ")]

    def test_no_owner_returns_empty(self) -> None:
        sample = _make_sample()
        assert parse_organization(sample) == []

    def test_owner_without_name_returns_empty(self) -> None:
        sample = _make_sample()
        sample["Owner"] = {}
        assert parse_organization(sample) == []

    def test_list_owner_names(self) -> None:
        sample = _make_sample()
        sample["Owner"] = {
            "Name": [
                {"content": "Org A", "abbreviation": "A"},
                "Org B",
            ]
        }
        orgs = parse_organization(sample)
        assert orgs == [
            Organization(name="Org A", abbreviation="A"),
            Organization(name="Org B"),
        ]

    def test_role_is_always_none(self) -> None:
        """BS では role 概念が無いため常に None。"""
        sample = _make_sample()
        sample["Owner"] = {"Name": {"content": "Org", "abbreviation": "A"}}
        orgs = parse_organization(sample)
        assert orgs[0].role is None
        assert orgs[0].organizationType is None


class TestParseSameAs:
    """Tests for parse_same_as function."""

    def test_sra_id(self) -> None:
        sample = _make_sample()
        sample["Ids"]["Id"] = [
            {"db": "BioSample", "content": "SAMD00000001"},
            {"db": "SRA", "content": "SRS123456"},
        ]
        xrefs = parse_same_as(sample)
        assert len(xrefs) == 1
        assert xrefs[0].identifier == "SRS123456"
        assert xrefs[0].type_ == "sra-sample"

    def test_no_sra_ids(self) -> None:
        sample = _make_sample()
        sample["Ids"]["Id"] = {"db": "BioSample", "content": "SAMD00000001"}
        assert parse_same_as(sample) == []


class TestParseStatus:
    """Tests for parse_status function."""

    def test_public_status(self) -> None:
        sample = _make_sample()
        assert parse_status(sample) == "public"

    def test_suppressed_status(self) -> None:
        sample = _make_sample()
        sample["Status"] = {"status": "suppressed"}
        assert parse_status(sample) == "suppressed"

    def test_no_status(self) -> None:
        sample = _make_sample()
        assert parse_status(sample) == "public"


class TestParseAccessibility:
    """Tests for parse_accessibility function."""

    def test_public(self) -> None:
        sample = _make_sample()
        sample["access"] = "public"
        assert parse_accessibility(sample) == "public-access"

    def test_controlled(self) -> None:
        sample = _make_sample()
        sample["access"] = "controlled"
        assert parse_accessibility(sample) == "controlled-access"

    def test_default(self) -> None:
        sample = _make_sample()
        assert parse_accessibility(sample) == "public-access"


class TestNormalizeProperties:
    """Tests for normalize_properties function."""

    def test_normalizes_owner_name(self) -> None:
        sample = _make_sample()
        sample["Owner"] = {"Name": "DDBJ"}
        normalize_properties(sample)
        assert sample["Owner"]["Name"] == {"content": "DDBJ"}

    def test_normalizes_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": "Generic"}
        normalize_properties(sample)
        assert sample["Models"]["Model"] == {"content": "Generic"}

    def test_no_crash_on_empty_sample(self) -> None:
        sample: dict[str, Any] = {}
        normalize_properties(sample)


def _make_bs_instance(identifier: str) -> BioSample:
    return BioSample(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="BioSample",
        type="biosample",
        name=None,
        url="https://example.com",
        organism=None,
        title=None,
        description=None,
        organization=[],
        model=[],
        package=None,
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


class TestFetchStatuses:
    """Tests for _fetch_statuses function."""

    def test_fetch_statuses_overwrites_status(self, tmp_path):
        from ddbj_search_converter.config import Config
        from ddbj_search_converter.jsonl.bs import _fetch_statuses
        from ddbj_search_converter.logging.logger import run_logger
        from ddbj_search_converter.status_cache.db import (
            finalize_status_cache_db,
            init_status_cache_db,
            insert_bs_statuses,
        )

        config = Config(result_dir=tmp_path)
        with run_logger(config=config):
            init_status_cache_db(config)
            insert_bs_statuses(config, [("SAMD00000001", "suppressed")])
            finalize_status_cache_db(config)

            docs = {"SAMD00000001": _make_bs_instance("SAMD00000001")}
            _fetch_statuses(config, docs)

        assert docs["SAMD00000001"].status == "suppressed"

    def test_fetch_statuses_skips_when_no_cache(self, tmp_path):
        from ddbj_search_converter.config import Config
        from ddbj_search_converter.jsonl.bs import _fetch_statuses

        config = Config(result_dir=tmp_path)
        docs = {"SAMD00000001": _make_bs_instance("SAMD00000001")}
        _fetch_statuses(config, docs)

        assert docs["SAMD00000001"].status == "public"


class TestXmlEntryToBsInstanceProperties:
    """xml_entry_to_bs_instance 実行後の properties 内 Attribute 正規化。"""

    def test_single_attribute_becomes_list(self) -> None:
        sample = _make_sample()
        sample["Attributes"]["Attribute"] = {"attribute_name": "host", "content": "Homo sapiens"}
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        attrs = bs.properties["BioSample"]["Attributes"]["Attribute"]
        assert isinstance(attrs, list)
        assert attrs == [{"attribute_name": "host", "content": "Homo sapiens"}]

    def test_multiple_attributes_stay_list(self) -> None:
        sample = _make_sample()
        sample["Attributes"]["Attribute"] = [
            {"attribute_name": "a", "content": "1"},
            {"attribute_name": "b", "content": "2"},
        ]
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        attrs = bs.properties["BioSample"]["Attributes"]["Attribute"]
        assert len(attrs) == 2

    def test_no_attribute_stays_no_op(self) -> None:
        sample = _make_sample()
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        assert bs.properties["BioSample"]["Attributes"] == {}

    def test_description_stays_dict(self) -> None:
        """Description は Attribute 相当ではないので配列化されない（回帰テスト）。"""
        sample = _make_sample()
        sample["Description"]["Title"] = "A title"
        sample["Attributes"]["Attribute"] = {"attribute_name": "x", "content": "y"}
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        assert isinstance(bs.properties["BioSample"]["Description"], dict)
        assert bs.properties["BioSample"]["Description"]["Title"] == "A title"

    def test_ids_stays_dict(self) -> None:
        """Ids も配列化されない（回帰テスト）。"""
        sample = _make_sample()
        sample["Attributes"]["Attribute"] = {"attribute_name": "x", "content": "y"}
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        assert isinstance(bs.properties["BioSample"]["Ids"], dict)

    def test_ddbj_package_is_none_even_with_models(self) -> None:
        """§4.11 L5 regression: DDBJ BS では Models があっても package=None。"""
        sample = _make_sample()
        sample["Models"] = {"Model": "MIGS.ba"}
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=True)
        assert bs.package is None
        assert bs.model == ["MIGS.ba"]

    def test_ncbi_package_is_parsed_from_element(self) -> None:
        sample = _make_sample()
        sample["Package"] = {"content": "Generic", "display_name": "Generic.1.0"}
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        assert bs.package == BioSamplePackage(name="Generic", displayName="Generic.1.0")

    def test_organization_is_parsed_from_owner(self) -> None:
        sample = _make_sample()
        sample["Owner"] = {"Name": {"content": "NCBI", "abbreviation": "NCBI"}}
        bs = xml_entry_to_bs_instance({"BioSample": sample}, is_ddbj=False)
        assert bs.organization == [Organization(name="NCBI", abbreviation="NCBI")]
