"""Tests for ddbj_search_converter.jsonl.bs module."""
from typing import Any, Dict, List

import pytest

from ddbj_search_converter.jsonl.bs import (
    normalize_properties,
    parse_accessibility,
    parse_accession,
    parse_attributes,
    parse_description,
    parse_model,
    parse_name,
    parse_organism,
    parse_package,
    parse_same_as,
    parse_status,
    parse_title,
)


def _make_sample(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Minimal valid sample dict for testing."""
    base: Dict[str, Any] = {
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
        sample: Dict[str, Any] = {"Ids": None}
        with pytest.raises(ValueError):
            parse_accession(sample, is_ddbj=True)

    def test_no_accession_raises(self) -> None:
        sample: Dict[str, Any] = {}
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
            "taxonomy_id": "9606", "taxonomy_name": "Homo sapiens",
        }
        result = parse_organism(sample, is_ddbj=False)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_ddbj_organism(self) -> None:
        sample = _make_sample()
        sample["Description"]["Organism"] = {
            "taxonomy_id": "9606", "OrganismName": "Homo sapiens",
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


class TestParseAttributes:
    """Tests for parse_attributes function."""

    def test_single_attribute(self) -> None:
        sample = _make_sample()
        sample["Attributes"]["Attribute"] = {
            "attribute_name": "geo_loc_name",
            "display_name": "Geographic Location",
            "harmonized_name": "geo_loc_name",
            "content": "Japan",
        }
        attrs = parse_attributes(sample)
        assert len(attrs) == 1
        assert attrs[0].attribute_name == "geo_loc_name"
        assert attrs[0].content == "Japan"

    def test_multiple_attributes(self) -> None:
        sample = _make_sample()
        sample["Attributes"]["Attribute"] = [
            {"attribute_name": "attr1", "content": "val1"},
            {"attribute_name": "attr2", "content": "val2"},
        ]
        attrs = parse_attributes(sample)
        assert len(attrs) == 2

    def test_string_attribute(self) -> None:
        sample = _make_sample()
        sample["Attributes"]["Attribute"] = "raw_value"
        attrs = parse_attributes(sample)
        assert len(attrs) == 1
        assert attrs[0].content == "raw_value"
        assert attrs[0].attribute_name is None

    def test_no_attributes(self) -> None:
        sample = _make_sample()
        assert parse_attributes(sample) == []


class TestParseModel:
    """Tests for parse_model function."""

    def test_string_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": "Generic"}
        models = parse_model(sample)
        assert len(models) == 1
        assert models[0].name == "Generic"

    def test_dict_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": "Generic"}}
        models = parse_model(sample)
        assert len(models) == 1
        assert models[0].name == "Generic"

    def test_no_models(self) -> None:
        sample = _make_sample()
        assert parse_model(sample) == []

    def test_model_content_zero(self) -> None:
        """content=0 でも Model として取得できる。"""
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": 0}}
        models = parse_model(sample)
        assert len(models) == 1
        assert models[0].name == "0"


class TestBug13ModelContentFalsy:
    """Bug #13 (fixed): parse_model の `if content:` が content=0 をスキップする。"""

    def test_content_zero_should_produce_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": 0}}
        models = parse_model(sample)
        assert len(models) == 1
        assert models[0].name == "0"

    def test_content_empty_string_should_produce_model(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": ""}}
        models = parse_model(sample)
        assert len(models) == 1
        assert models[0].name == ""

    def test_content_none_should_be_skipped(self) -> None:
        sample = _make_sample()
        sample["Models"] = {"Model": {"content": None}}
        models = parse_model(sample)
        assert len(models) == 0


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


class TestParsePackage:
    """Tests for parse_package function."""

    def test_ddbj_uses_model(self) -> None:
        from ddbj_search_converter.schema import Model
        model = [Model(name="Generic")]
        sample = _make_sample()
        pkg = parse_package(sample, model, is_ddbj=True)
        assert pkg is not None
        assert pkg.name == "Generic"

    def test_ncbi_string_package(self) -> None:
        sample = _make_sample()
        sample["Package"] = "Generic.1.0"
        pkg = parse_package(sample, [], is_ddbj=False)
        assert pkg is not None
        assert pkg.name == "Generic.1.0"

    def test_ncbi_dict_package(self) -> None:
        sample = _make_sample()
        sample["Package"] = {"content": "Generic.1.0", "display_name": "Generic"}
        pkg = parse_package(sample, [], is_ddbj=False)
        assert pkg is not None
        assert pkg.name == "Generic.1.0"
        assert pkg.display_name == "Generic"


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

    def test_live_status(self) -> None:
        sample = _make_sample()
        assert parse_status(sample) == "live"

    def test_suppressed_status(self) -> None:
        sample = _make_sample()
        sample["Status"] = {"status": "suppressed"}
        assert parse_status(sample) == "suppressed"

    def test_no_status(self) -> None:
        sample = _make_sample()
        assert parse_status(sample) == "live"


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
        sample: Dict[str, Any] = {}
        normalize_properties(sample)
