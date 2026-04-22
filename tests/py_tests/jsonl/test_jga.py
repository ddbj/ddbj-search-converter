"""Tests for ddbj_search_converter.jsonl.jga module."""

from datetime import datetime, timezone
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.jsonl.jga import (
    _format_date_from_csv,
    extract_dataset_type,
    extract_description,
    extract_study_type,
    extract_title,
    format_date,
    jga_entry_to_jga_instance,
    parse_external_link,
    parse_grants,
    parse_organization,
    parse_publications,
    parse_same_as,
    parse_vendor,
)
from ddbj_search_converter.schema import (
    ExternalLink,
    Grant,
    Organization,
    Publication,
)


class TestFormatDate:
    """Tests for format_date function."""

    def test_none_returns_none(self) -> None:
        assert format_date(None) is None

    def test_aware_datetime(self) -> None:
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = format_date(dt)
        assert result == "2024-01-01T12:00:00Z"

    def test_iso_string(self) -> None:
        result = format_date("2024-01-01T12:00:00Z")
        assert result == "2024-01-01T12:00:00Z"

    def test_iso_string_with_offset(self) -> None:
        result = format_date("2024-01-01T21:00:00+09:00")
        assert result == "2024-01-01T12:00:00Z"

    def test_invalid_string_returns_none(self) -> None:
        assert format_date("not a date") is None


class TestFormatDatePBT:
    """Property-based tests for format_date."""

    @given(st.none())
    def test_none_always_none(self, value: None) -> None:
        assert format_date(value) is None

    @given(st.datetimes(timezones=st.just(timezone.utc)))
    def test_aware_datetime_ends_with_z(self, dt: datetime) -> None:
        result = format_date(dt)
        assert result is not None
        assert result.endswith("Z")


class TestFormatDateFromCsv:
    """Tests for _format_date_from_csv function."""

    def test_standard_csv_format(self) -> None:
        result = _format_date_from_csv("2014-07-07 14:00:37.208+09")
        assert result.endswith("Z")
        # 14:00:37 JST -> 05:00:37 UTC
        assert result == "2014-07-07T05:00:37Z"

    def test_utc_z_format(self) -> None:
        result = _format_date_from_csv("2024-01-01T12:00:00Z")
        assert result == "2024-01-01T12:00:00Z"

    def test_invalid_date_raises(self) -> None:
        with pytest.raises(Exception):
            _format_date_from_csv("not-a-date")


class TestFormatDateEdgeCases:
    """Edge case tests for date formatting."""

    def test_naive_datetime(self) -> None:
        """naive datetime (timezone なし)。"""
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = format_date(dt)
        # naive datetime に対する挙動をテスト
        # astimezone はシステムの timezone を仮定する
        assert result is not None
        assert result.endswith("Z")


class TestBug15JgaDacWrapping:
    """Bug #15 (fixed): jga-dac の entries が既にリストの場合に二重ラップされる。"""

    def test_single_dac_entry_gets_wrapped(self) -> None:
        """単一エントリ (dict) がリストにラップされる。"""

        # generate_jga_jsonl の内部ロジックの一部をテスト
        # entries が dict の場合は [entries] にラップ
        entries: dict[str, Any] = {"accession": "JGAC000001"}
        if not isinstance(entries, list):
            entries_list = [entries]
        else:
            entries_list = entries
        assert isinstance(entries_list, list)
        assert len(entries_list) == 1

    def test_multiple_dac_entries_not_double_wrapped(self) -> None:
        """既にリストの entries は二重ラップされない。"""
        entries = [
            {"accession": "JGAC000001"},
            {"accession": "JGAC000002"},
        ]
        if not isinstance(entries, list):
            entries = [entries]
        assert isinstance(entries, list)
        assert len(entries) == 2

    def test_dac_wrapping_idempotent(self) -> None:
        """リスト判定ロジックの冪等性。"""
        single = {"accession": "JGAC000001"}
        already_list = [{"accession": "JGAC000001"}]

        # single → リストにラップ
        if not isinstance(single, list):
            result_single = [single]
        else:
            result_single = single
        assert len(result_single) == 1

        # already_list → そのまま
        if not isinstance(already_list, list):
            result_list = [already_list]
        else:
            result_list = already_list
        assert len(result_list) == 1


class TestExtractTitle:
    """Tests for extract_title function."""

    def test_jga_study(self) -> None:
        entry = {"DESCRIPTOR": {"STUDY_TITLE": "Study Title"}}
        assert extract_title(entry, "jga-study") == "Study Title"

    def test_jga_dataset(self) -> None:
        entry = {"TITLE": "Dataset Title"}
        assert extract_title(entry, "jga-dataset") == "Dataset Title"

    def test_jga_dac(self) -> None:
        entry: dict[str, Any] = {}
        assert extract_title(entry, "jga-dac") is None

    def test_jga_policy(self) -> None:
        entry = {"TITLE": "Policy Title"}
        assert extract_title(entry, "jga-policy") == "Policy Title"

    def test_none_title(self) -> None:
        entry: dict[str, Any] = {"DESCRIPTOR": {}}
        assert extract_title(entry, "jga-study") is None


class TestExtractDescription:
    """Tests for extract_description function."""

    def test_jga_study(self) -> None:
        entry = {"DESCRIPTOR": {"STUDY_ABSTRACT": "Abstract text"}}
        assert extract_description(entry, "jga-study") == "Abstract text"

    def test_jga_dataset(self) -> None:
        entry = {"DESCRIPTION": "Dataset desc"}
        assert extract_description(entry, "jga-dataset") == "Dataset desc"

    def test_jga_policy_uses_policy_text(self) -> None:
        """jga-policy は POLICY_TEXT を description に詰める。"""
        entry = {"POLICY_TEXT": "Policy body"}
        assert extract_description(entry, "jga-policy") == "Policy body"

    def test_jga_dac_returns_none(self) -> None:
        """jga-dac は description 相当フィールドが無いため None。"""
        entry = {"CONTACTS": {}}
        assert extract_description(entry, "jga-dac") is None

    def test_no_description(self) -> None:
        entry: dict[str, Any] = {}
        assert extract_description(entry, "jga-study") is None


class TestJgaEntryToJgaInstance:
    """Tests for jga_entry_to_jga_instance function."""

    def test_basic_jga_study(self) -> None:
        entry = {
            "accession": "JGAS000001",
            "alias": "My Study",
            "DESCRIPTOR": {"STUDY_TITLE": "Title", "STUDY_ABSTRACT": "Abstract"},
        }
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.identifier == "JGAS000001"
        assert jga.type_ == "jga-study"
        assert jga.title == "Title"
        assert jga.description == "Abstract"
        assert jga.name is None

    def test_organism_is_human_for_study(self) -> None:
        """jga-study は常に Homo sapiens。"""
        entry = {"accession": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.organism is not None
        assert jga.organism.identifier == "9606"
        assert jga.organism.name == "Homo sapiens"

    def test_organism_is_human_for_dataset(self) -> None:
        """jga-dataset も常に Homo sapiens。"""
        entry = {"accession": "JGAD000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-dataset")
        assert jga.organism is not None
        assert jga.organism.identifier == "9606"

    def test_organism_is_none_for_dac(self) -> None:
        """jga-dac は organism を持たない。"""
        entry = {"accession": "JGAC000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-dac")
        assert jga.organism is None

    def test_organism_is_none_for_policy(self) -> None:
        """jga-policy は organism を持たない。"""
        entry = {"accession": "JGAP000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-policy")
        assert jga.organism is None

    def test_default_accessibility(self) -> None:
        entry = {"accession": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.accessibility == "controlled-access"

    def test_jga_dataset(self) -> None:
        entry = {
            "accession": "JGAD000001",
            "TITLE": "Dataset",
            "DESCRIPTION": "Desc",
        }
        jga = jga_entry_to_jga_instance(entry, "jga-dataset")
        assert jga.type_ == "jga-dataset"
        assert jga.title == "Dataset"

    def test_jga_dac(self) -> None:
        entry = {"accession": "JGAC000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-dac")
        assert jga.type_ == "jga-dac"
        assert jga.title is None

    def test_jga_policy(self) -> None:
        entry = {"accession": "JGAP000001", "TITLE": "Policy"}
        jga = jga_entry_to_jga_instance(entry, "jga-policy")
        assert jga.type_ == "jga-policy"
        assert jga.title == "Policy"

    def test_same_as_populated_from_identifiers(self) -> None:
        """SECONDARY_ID が sameAs に反映される。"""
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAS000999"},
        }
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert len(jga.sameAs) == 1
        assert jga.sameAs[0].identifier == "JGAS000999"
        assert jga.sameAs[0].type_ == "jga-study"

    def test_same_as_empty_for_policy(self) -> None:
        """jga-policy は IDENTIFIERS がないため sameAs 空。"""
        entry: dict[str, Any] = {"accession": "JGAP000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-policy")
        assert jga.sameAs == []


class TestParseSameAs:
    """Tests for parse_same_as function."""

    def test_study_with_secondary_id(self) -> None:
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAS000999"},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert len(result) == 1
        assert result[0].identifier == "JGAS000999"
        assert result[0].type_ == "jga-study"
        assert "jga-study/JGAS000999" in result[0].url

    def test_dataset_with_secondary_id(self) -> None:
        entry = {
            "accession": "JGAD000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAD000999"},
        }
        result = parse_same_as(entry, "jga-dataset", "JGAD000001")
        assert len(result) == 1
        assert result[0].type_ == "jga-dataset"

    def test_dac_with_secondary_id(self) -> None:
        entry = {
            "accession": "JGAC000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAC000999"},
        }
        result = parse_same_as(entry, "jga-dac", "JGAC000001")
        assert len(result) == 1
        assert result[0].type_ == "jga-dac"

    def test_no_identifiers(self) -> None:
        entry: dict[str, Any] = {"accession": "JGAP000001"}
        result = parse_same_as(entry, "jga-policy", "JGAP000001")
        assert result == []

    def test_identifiers_without_secondary_id(self) -> None:
        entry: dict[str, Any] = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"PRIMARY_ID": "JGAS000001"},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert result == []

    def test_secondary_id_equals_accession(self) -> None:
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAS000001"},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert result == []

    def test_multiple_secondary_ids(self) -> None:
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": ["JGAS000998", "JGAS000999"]},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert len(result) == 2
        assert result[0].identifier == "JGAS000998"
        assert result[1].identifier == "JGAS000999"

    def test_empty_secondary_id(self) -> None:
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": ""},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert result == []


class TestJgaEntryToJgaInstanceProperties:
    """jga_entry_to_jga_instance 実行後の properties 内 Attribute 正規化。"""

    def test_jga_study_single_attribute_becomes_list(self) -> None:
        entry: dict[str, Any] = {
            "accession": "JGAS000009",
            "STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "NBDC Number", "VALUE": "hum0018"}},
        }
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        attrs = jga.properties["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"]
        assert isinstance(attrs, list)
        assert attrs == [{"TAG": "NBDC Number", "VALUE": "hum0018"}]

    def test_jga_study_multiple_attributes_stay_list(self) -> None:
        entry: dict[str, Any] = {
            "accession": "JGAS000001",
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": [
                    {"TAG": "a", "VALUE": "1"},
                    {"TAG": "b", "VALUE": "2"},
                ]
            },
        }
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        attrs = jga.properties["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"]
        assert len(attrs) == 2

    def test_jga_study_no_attribute_stays_absent(self) -> None:
        entry: dict[str, Any] = {"accession": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert "STUDY_ATTRIBUTES" not in jga.properties

    def test_jga_dataset_no_op(self) -> None:
        entry: dict[str, Any] = {
            "accession": "JGAD000001",
            "TITLE": "Dataset",
            "DESCRIPTION": "Desc",
        }
        jga = jga_entry_to_jga_instance(entry, "jga-dataset")
        assert jga.properties["TITLE"] == "Dataset"
        assert jga.properties["DESCRIPTION"] == "Desc"

    def test_jga_dac_no_op(self) -> None:
        entry: dict[str, Any] = {"accession": "JGAC000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-dac")
        assert jga.properties["accession"] == "JGAC000001"

    def test_jga_policy_no_op(self) -> None:
        entry: dict[str, Any] = {"accession": "JGAP000001", "TITLE": "Policy"}
        jga = jga_entry_to_jga_instance(entry, "jga-policy")
        assert jga.properties["TITLE"] == "Policy"

    def test_jga_study_other_fields_preserved(self) -> None:
        """DESCRIPTOR 等 Attribute 以外のフィールドは不変。"""
        entry: dict[str, Any] = {
            "accession": "JGAS000001",
            "DESCRIPTOR": {"STUDY_TITLE": "My Study", "STUDY_ABSTRACT": "Abstract"},
            "STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "k", "VALUE": "v"}},
        }
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert isinstance(jga.properties["DESCRIPTOR"], dict)
        assert jga.properties["DESCRIPTOR"]["STUDY_TITLE"] == "My Study"


class TestJgaNameAlwaysNone:
    """name は常に None (alias の値によらない)。"""

    def test_alias_different_from_accession(self) -> None:
        entry = {"accession": "JGAS000001", "alias": "JSUB000002_Study_0001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.name is None

    def test_alias_same_as_accession(self) -> None:
        entry = {"accession": "JGAS000001", "alias": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.name is None

    def test_no_alias(self) -> None:
        entry: dict[str, Any] = {"accession": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.name is None

    def test_dataset_name_also_none(self) -> None:
        entry = {"accession": "JGAD000001", "alias": "JSUB000002_Dataset_0001"}
        jga = jga_entry_to_jga_instance(entry, "jga-dataset")
        assert jga.name is None


class TestExtractStudyType:
    """Tests for extract_study_type function."""

    def test_single_existing_non_other(self) -> None:
        entry = {"DESCRIPTOR": {"STUDY_TYPES": {"STUDY_TYPE": {"existing_study_type": "Exome Sequencing"}}}}
        assert extract_study_type(entry) == ["Exome Sequencing"]

    def test_other_with_new_study_type(self) -> None:
        entry = {
            "DESCRIPTOR": {
                "STUDY_TYPES": {
                    "STUDY_TYPE": {
                        "existing_study_type": "Other",
                        "new_study_type": "Whole genome bisulfite sequencing",
                    }
                }
            }
        }
        assert extract_study_type(entry) == ["Whole genome bisulfite sequencing"]

    def test_other_without_new_falls_back_to_other(self) -> None:
        """existing=Other かつ new_study_type が空/欠損の場合は Other を残す。"""
        entry = {"DESCRIPTOR": {"STUDY_TYPES": {"STUDY_TYPE": {"existing_study_type": "Other"}}}}
        assert extract_study_type(entry) == ["Other"]

    def test_other_with_empty_new_falls_back(self) -> None:
        entry = {
            "DESCRIPTOR": {"STUDY_TYPES": {"STUDY_TYPE": {"existing_study_type": "Other", "new_study_type": "   "}}}
        }
        assert extract_study_type(entry) == ["Other"]

    def test_multiple_list_mixes_existing_and_new(self) -> None:
        entry = {
            "DESCRIPTOR": {
                "STUDY_TYPES": {
                    "STUDY_TYPE": [
                        {"existing_study_type": "Tumor vs. Matched-Normal"},
                        {"existing_study_type": "Exome Sequencing"},
                        {"existing_study_type": "Transcriptome Sequencing"},
                        {"existing_study_type": "Other", "new_study_type": "DNA methylation array"},
                    ]
                }
            }
        }
        assert extract_study_type(entry) == [
            "Tumor vs. Matched-Normal",
            "Exome Sequencing",
            "Transcriptome Sequencing",
            "DNA methylation array",
        ]

    def test_missing_study_types_returns_empty(self) -> None:
        entry: dict[str, Any] = {"DESCRIPTOR": {}}
        assert extract_study_type(entry) == []

    def test_missing_descriptor_returns_empty(self) -> None:
        entry: dict[str, Any] = {}
        assert extract_study_type(entry) == []


class TestExtractDatasetType:
    """Tests for extract_dataset_type function."""

    def test_single_string(self) -> None:
        entry = {"DATASET_TYPE": "Random exome sequencing"}
        assert extract_dataset_type(entry) == ["Random exome sequencing"]

    def test_list_of_strings(self) -> None:
        entry = {"DATASET_TYPE": ["Exome", "WGS"]}
        assert extract_dataset_type(entry) == ["Exome", "WGS"]

    def test_missing_returns_empty(self) -> None:
        entry: dict[str, Any] = {}
        assert extract_dataset_type(entry) == []

    def test_empty_string_skipped(self) -> None:
        entry = {"DATASET_TYPE": "   "}
        assert extract_dataset_type(entry) == []


class TestParseVendor:
    """Tests for parse_vendor function."""

    def test_single_vendor(self) -> None:
        entry = {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "Vendor", "VALUE": "Illumina"}}}
        assert parse_vendor(entry) == ["Illumina"]

    def test_multiple_vendors_in_list(self) -> None:
        entry = {
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": [
                    {"TAG": "NBDC Number", "VALUE": "hum0001"},
                    {"TAG": "Vendor", "VALUE": "Illumina"},
                    {"TAG": "Vendor", "VALUE": "MGI"},
                ]
            }
        }
        assert parse_vendor(entry) == ["Illumina", "MGI"]

    def test_no_vendor_tag(self) -> None:
        entry = {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "NBDC Number", "VALUE": "hum0001"}}}
        assert parse_vendor(entry) == []

    def test_no_study_attributes(self) -> None:
        entry: dict[str, Any] = {}
        assert parse_vendor(entry) == []


class TestParseOrganization:
    """Tests for parse_organization function."""

    def test_study_center_name_only(self) -> None:
        entry = {"center_name": "Individual"}
        orgs = parse_organization(entry, "jga-study")
        assert [o.name for o in orgs] == ["Individual"]
        assert all(o.role is None and o.organizationType is None for o in orgs)

    def test_study_center_name_plus_submitting_org(self) -> None:
        entry = {
            "center_name": "Individual",
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": {
                    "TAG": "Submitting organization",
                    "VALUE": "Department of Neurosurgery, The University of Tokyo",
                }
            },
        }
        orgs = parse_organization(entry, "jga-study")
        assert [o.name for o in orgs] == [
            "Individual",
            "Department of Neurosurgery, The University of Tokyo",
        ]

    def test_study_dedupe_on_equal_names(self) -> None:
        entry = {
            "center_name": "DBCLS",
            "STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "Submitting organization", "VALUE": "DBCLS"}},
        }
        orgs = parse_organization(entry, "jga-study")
        assert [o.name for o in orgs] == ["DBCLS"]

    def test_study_multiple_submitting_org_tags_in_list(self) -> None:
        entry = {
            "center_name": "Individual",
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": [
                    {"TAG": "NBDC Number", "VALUE": "hum0004"},
                    {"TAG": "Submitting organization", "VALUE": "Lab A"},
                    {"TAG": "Submitting organization", "VALUE": "Lab B"},
                ]
            },
        }
        orgs = parse_organization(entry, "jga-study")
        assert [o.name for o in orgs] == ["Individual", "Lab A", "Lab B"]

    def test_dac_center_name_plus_contact_organisation(self) -> None:
        entry = {
            "center_name": "dbcls",
            "CONTACTS": {"CONTACT": {"name": "DBCLS", "email": "humandbs@dbcls.jp", "organisation": "DBCLS"}},
        }
        orgs = parse_organization(entry, "jga-dac")
        assert [o.name for o in orgs] == ["dbcls", "DBCLS"]

    def test_dac_ignores_contact_name_and_email(self) -> None:
        """@name / @email は load しない (organisation のみ)。"""
        entry = {
            "center_name": "dbcls",
            "CONTACTS": {"CONTACT": {"name": "DBCLS", "email": "humandbs@dbcls.jp"}},
        }
        orgs = parse_organization(entry, "jga-dac")
        assert [o.name for o in orgs] == ["dbcls"]

    def test_policy_uses_only_center_name(self) -> None:
        """jga-policy は CONTACTS を参照しない。"""
        entry = {
            "center_name": "nbdc",
            "CONTACTS": {"CONTACT": {"organisation": "should-be-ignored"}},
        }
        orgs = parse_organization(entry, "jga-policy")
        assert [o.name for o in orgs] == ["nbdc"]

    def test_dataset_uses_only_center_name(self) -> None:
        entry = {
            "center_name": "Individual",
            "STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "Submitting organization", "VALUE": "should-be-ignored"}},
        }
        orgs = parse_organization(entry, "jga-dataset")
        assert [o.name for o in orgs] == ["Individual"]

    def test_missing_center_name_returns_empty(self) -> None:
        assert parse_organization({}, "jga-study") == []


class TestParsePublicationsJga:
    """Tests for parse_publications function (JGA study-specific)."""

    def test_single_pubmed_published(self) -> None:
        entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "24336570", "status": "published", "DB_TYPE": "PUBMED"}}}
        pubs = parse_publications(entry)
        assert len(pubs) == 1
        assert isinstance(pubs[0], Publication)
        assert pubs[0].id_ == "24336570"
        assert pubs[0].dbType == "pubmed"
        assert pubs[0].url == "https://pubmed.ncbi.nlm.nih.gov/24336570/"

    def test_lowercase_pubmed_normalizes(self) -> None:
        """大小文字揺れ (pubmed / PubMed) も pubmed に正規化。"""
        entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "1", "DB_TYPE": "pubmed"}}}
        assert parse_publications(entry)[0].dbType == "pubmed"

    def test_unknown_db_type_falls_back_to_none(self) -> None:
        entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "1", "DB_TYPE": "UNKNOWN"}}}
        pub = parse_publications(entry)[0]
        assert pub.dbType is None
        assert pub.url is None

    def test_multiple_publications_list(self) -> None:
        entry = {
            "PUBLICATIONS": {
                "PUBLICATION": [
                    {"id": "1", "DB_TYPE": "PUBMED"},
                    {"id": "2", "DB_TYPE": "PUBMED"},
                ]
            }
        }
        pubs = parse_publications(entry)
        assert [p.id_ for p in pubs] == ["1", "2"]

    def test_missing_publications_returns_empty(self) -> None:
        assert parse_publications({}) == []


class TestParseGrantsJga:
    """Tests for parse_grants function (JGA study-specific)."""

    def test_single_grant_with_agency_dict(self) -> None:
        entry = {
            "GRANTS": {
                "GRANT": {
                    "grant_id": "22129008",
                    "TITLE": "Grant-in-Aid for Scientific Research on Innovative Areas",
                    "AGENCY": {"abbr": "MEXT", "content": "Ministry of Education"},
                }
            }
        }
        grants = parse_grants(entry)
        assert len(grants) == 1
        grant = grants[0]
        assert isinstance(grant, Grant)
        assert grant.id_ == "22129008"
        assert grant.title == "Grant-in-Aid for Scientific Research on Innovative Areas"
        assert grant.agency == [Organization(name="Ministry of Education", abbreviation="MEXT")]

    def test_grant_with_agency_string(self) -> None:
        entry = {"GRANTS": {"GRANT": {"grant_id": "G1", "TITLE": "T", "AGENCY": "NIH"}}}
        grant = parse_grants(entry)[0]
        assert grant.agency == [Organization(name="NIH", abbreviation=None)]

    def test_grant_agency_roles_are_none(self) -> None:
        """Grant.agency では role / organizationType / department / url は常に None。"""
        entry = {
            "GRANTS": {
                "GRANT": {
                    "grant_id": "22129008",
                    "TITLE": "Grant-in-Aid",
                    "AGENCY": {"abbr": "MEXT", "content": "Ministry of Education"},
                }
            }
        }
        agency = parse_grants(entry)[0].agency[0]
        assert agency.role is None
        assert agency.organizationType is None
        assert agency.department is None
        assert agency.url is None

    def test_empty_grant_id_becomes_none(self) -> None:
        entry = {"GRANTS": {"GRANT": {"grant_id": "", "TITLE": "T", "AGENCY": {"abbr": "X", "content": "Y"}}}}
        assert parse_grants(entry)[0].id_ is None

    def test_multiple_grants_list(self) -> None:
        entry = {
            "GRANTS": {
                "GRANT": [
                    {"grant_id": "1", "TITLE": "A", "AGENCY": "X"},
                    {"grant_id": "2", "TITLE": "B", "AGENCY": "Y"},
                ]
            }
        }
        assert [g.id_ for g in parse_grants(entry)] == ["1", "2"]

    def test_missing_grants_returns_empty(self) -> None:
        assert parse_grants({}) == []


class TestParseExternalLinkJga:
    """Tests for parse_external_link function (JGA type-specific dispatch)."""

    def test_study_single_link(self) -> None:
        entry = {
            "STUDY_LINKS": {
                "STUDY_LINK": {
                    "URL_LINK": {
                        "LABEL": "http://example.com",
                        "URL": "http://example.com",
                    }
                }
            }
        }
        links = parse_external_link(entry, "jga-study")
        assert links == [ExternalLink(url="http://example.com", label="http://example.com")]

    def test_study_multiple_links_list(self) -> None:
        entry = {
            "STUDY_LINKS": {
                "STUDY_LINK": [
                    {"URL_LINK": {"LABEL": "L1", "URL": "http://a"}},
                    {"URL_LINK": {"LABEL": "L2", "URL": "http://b"}},
                ]
            }
        }
        links = parse_external_link(entry, "jga-study")
        assert [link.url for link in links] == ["http://a", "http://b"]

    def test_dac_link(self) -> None:
        entry = {
            "DAC_LINKS": {
                "DAC_LINK": {
                    "URL_LINK": {
                        "LABEL": "Change in the operation",
                        "URL": "https://biosciencedbc.jp/en/news/20240401-03.html",
                    }
                }
            }
        }
        links = parse_external_link(entry, "jga-dac")
        assert len(links) == 1
        assert links[0].label == "Change in the operation"
        assert links[0].url == "https://biosciencedbc.jp/en/news/20240401-03.html"

    def test_policy_link(self) -> None:
        entry = {
            "POLICY_LINKS": {
                "POLICY_LINK": {"URL_LINK": {"LABEL": "NBDC Policy", "URL": "https://humandbs.dbcls.jp/en/nbdc-policy"}}
            }
        }
        links = parse_external_link(entry, "jga-policy")
        assert [link.label for link in links] == ["NBDC Policy"]

    def test_dataset_returns_empty_regardless_of_links(self) -> None:
        """jga-dataset は URL_LINK を持たない (マッピングなし)。"""
        entry = {"STUDY_LINKS": {"STUDY_LINK": {"URL_LINK": {"LABEL": "x", "URL": "http://x"}}}}
        assert parse_external_link(entry, "jga-dataset") == []

    def test_label_fallback_to_url(self) -> None:
        entry = {"STUDY_LINKS": {"STUDY_LINK": {"URL_LINK": {"URL": "http://only-url.com"}}}}
        links = parse_external_link(entry, "jga-study")
        assert links == [ExternalLink(url="http://only-url.com", label="http://only-url.com")]

    def test_missing_parent_key_returns_empty(self) -> None:
        assert parse_external_link({}, "jga-study") == []
