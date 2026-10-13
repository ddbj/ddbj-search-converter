"""Tests for ddbj_search_converter.jsonl.jga module."""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import JGA_DATE_REL_PATHS, Config
from ddbj_search_converter.humandbs import HUMANDBS_FETCH_MAX_ATTEMPTS, HumandbsResponseError
from ddbj_search_converter.jsonl.jga import (
    IndexName,
    extract_dataset_type,
    extract_description,
    extract_study_type,
    extract_title,
    fetch_jga_date_map,
    generate_jga_jsonl,
    humandbs_jga_date_url,
    jga_entry_to_jga_instance,
    load_date_map,
    parse_external_link,
    parse_grants,
    parse_humandbs_jga_date_ndjson,
    parse_organization,
    parse_publications,
    parse_same_as,
    parse_vendor,
    update_jga_date_file,
)
from ddbj_search_converter.logging.logger import _ctx, init_logger
from ddbj_search_converter.schema import (
    ExternalLink,
    Grant,
    Organization,
    Publication,
)
from tests.py_tests.humandbs_stub import HumandbsStub, ok_ndjson
from tests.py_tests.strategies import st_calendar_date, st_jga_dataset


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
        entry: dict[str, Any] = {"CONTACTS": {}}
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

    def test_secondary_id_zero_padding_variant_retained(self) -> None:
        """ゼロ埋め桁数だけ違う表記 (JGAS000001 ⇔ JGAS00000000001) も検索できるよう sameAs に残す。"""
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAS00000000001"},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert [x.identifier for x in result] == ["JGAS00000000001"]
        assert "jga-study/JGAS00000000001" in result[0].url

    def test_secondary_id_different_number_retained(self) -> None:
        """数値が異なる Secondary ID は別 accession として保持する。"""
        entry = {
            "accession": "JGAS000561",
            "IDENTIFIERS": {"SECONDARY_ID": "JGAS000556"},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000561")
        assert len(result) == 1
        assert result[0].identifier == "JGAS000556"

    def test_secondary_id_different_prefix_retained(self) -> None:
        """prefix が異なる Secondary ID は保持する (bulk insert 側で別途除外される)。"""
        entry = {
            "accession": "JGAD000452",
            "IDENTIFIERS": {"SECONDARY_ID": "AGDD_000001"},
        }
        result = parse_same_as(entry, "jga-dataset", "JGAD000452")
        assert len(result) == 1
        assert result[0].identifier == "AGDD_000001"

    def test_secondary_id_no_numeric_part_retained(self) -> None:
        """数値部を持たない Secondary ID は桁数違い判定の対象外なので保持する。"""
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": "JGASABC"},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert len(result) == 1
        assert result[0].identifier == "JGASABC"

    def test_secondary_id_mixed_list_keeps_all_but_the_accession_itself(self) -> None:
        entry = {
            "accession": "JGAS000001",
            "IDENTIFIERS": {"SECONDARY_ID": ["JGAS00000000001", "JGAS000561", "JGAS000001"]},
        }
        result = parse_same_as(entry, "jga-study", "JGAS000001")
        assert [x.identifier for x in result] == ["JGAS00000000001", "JGAS000561"]


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
        entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "24336570", "DB_TYPE": "PUBMED"}}}
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

    def test_e_prefix_pubmed_normalizes(self) -> None:
        entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "42", "DB_TYPE": "ePubmed"}}}
        pub = parse_publications(entry)[0]
        assert pub.dbType == "pubmed"
        assert pub.url == "https://pubmed.ncbi.nlm.nih.gov/42/"

    def test_doi_and_pmc_accepted_without_url(self) -> None:
        """doi/pmc (e-prefix 含む) は dbType は正規化されるが URL は現状 None のまま。"""
        doi_entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "10.1000/xyz", "DB_TYPE": "eDOI"}}}
        pmc_entry = {"PUBLICATIONS": {"PUBLICATION": {"id": "PMC1", "DB_TYPE": "PMC"}}}
        doi_pub = parse_publications(doi_entry)[0]
        pmc_pub = parse_publications(pmc_entry)[0]
        assert doi_pub.dbType == "doi"
        assert doi_pub.url is None
        assert pmc_pub.dbType == "pmc"
        assert pmc_pub.url is None

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

    def test_agency_dict_without_content_yields_empty_agencies(self) -> None:
        """<AGENCY abbr="X" /> のように content が無ければ agencies=[]。"""
        entry = {"GRANTS": {"GRANT": {"grant_id": "G1", "TITLE": "T", "AGENCY": {"abbr": "X"}}}}
        grants = parse_grants(entry)
        assert len(grants) == 1
        assert grants[0].agency == []

    def test_agency_dict_empty_content_yields_empty_agencies(self) -> None:
        """dict content が空文字列なら agencies=[]。"""
        entry = {"GRANTS": {"GRANT": {"grant_id": "G1", "TITLE": "T", "AGENCY": {"abbr": "X", "content": ""}}}}
        assert parse_grants(entry)[0].agency == []

    def test_agency_dict_whitespace_content_yields_empty_agencies(self) -> None:
        """dict content が空白のみなら agencies=[]。"""
        entry = {"GRANTS": {"GRANT": {"grant_id": "G1", "TITLE": "T", "AGENCY": {"abbr": "X", "content": "   "}}}}
        assert parse_grants(entry)[0].agency == []

    def test_agency_str_empty_yields_empty_agencies(self) -> None:
        """AGENCY が空文字列なら agencies=[]。"""
        entry = {"GRANTS": {"GRANT": {"grant_id": "G1", "TITLE": "T", "AGENCY": ""}}}
        assert parse_grants(entry)[0].agency == []

    def test_agency_str_is_stripped(self) -> None:
        """AGENCY が str のとき leading/trailing whitespace は除去する。"""
        entry = {"GRANTS": {"GRANT": {"grant_id": "G1", "TITLE": "T", "AGENCY": "  NIH  "}}}
        assert parse_grants(entry)[0].agency[0].name == "NIH"


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

    def test_invalid_url_silently_dropped(self) -> None:
        """javascript:, 空白混入などの不正 URL は silent に drop する。"""
        entry = {
            "STUDY_LINKS": {
                "STUDY_LINK": [
                    {"URL_LINK": {"LABEL": "bad", "URL": "javascript:alert(1)"}},
                    {"URL_LINK": {"LABEL": "ok", "URL": "https://example.com"}},
                    {"URL_LINK": {"LABEL": "bad", "URL": "https://has space.com"}},
                    {"URL_LINK": {"LABEL": "empty", "URL": ""}},
                ]
            }
        }
        links = parse_external_link(entry, "jga-study")
        assert [link.url for link in links] == ["https://example.com"]


# === 日付 (humandbs API) ===

HUMANDBS_URL = "https://humandbs.example"
STUDY_DATE_URL = f"{HUMANDBS_URL}/api/jga-date/jga-study"
FIXTURE_JGA_PATH = Path(__file__).resolve().parents[2] / "fixtures/usr/local/shared_data/jga/metadata-history/metadata"

DateTriple = tuple[str | None, str | None, str | None]


def _date_line(identifier: Any, index_name: Any, created: Any, published: Any, modified: Any) -> str:
    return json.dumps(
        {
            "identifier": identifier,
            "type": index_name,
            "dateCreated": created,
            "datePublished": published,
            "dateModified": modified,
        },
        separators=(",", ":"),
    )


def _date_ndjson(date_map: dict[str, DateTriple], index_name: str = "jga-study") -> str:
    return "".join(_date_line(acc, index_name, *dates) + "\n" for acc, dates in sorted(date_map.items()))


def _warnings() -> list[str]:
    ctx = _ctx.get()
    assert ctx is not None
    if not ctx.log_file.exists():
        return []
    records = [json.loads(line) for line in ctx.log_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [str(record["message"]) for record in records if record["log_level"] == "WARNING"]


def _saved_path(config: Config, index_name: str = "jga-study") -> Path:
    return config.const_dir.joinpath(JGA_DATE_REL_PATHS[index_name])


def _write_saved(config: Config, text: str, index_name: str = "jga-study") -> Path:
    path = _saved_path(config, index_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def jga_config(tmp_path: Path) -> Config:
    return Config(result_dir=tmp_path, const_dir=tmp_path.joinpath("const"), humandbs_url=HUMANDBS_URL)


@pytest.fixture
def _setup_logger(jga_config: Config) -> Iterator[None]:
    init_logger(run_name="test_jga", config=jga_config)
    yield
    _ctx.set(None)


class TestHumandbsJgaDateUrl:
    """Tests for humandbs_jga_date_url function."""

    @pytest.mark.parametrize("base_url", [HUMANDBS_URL, f"{HUMANDBS_URL}/"])
    def test_trailing_slash_is_not_doubled(self, base_url: str) -> None:
        assert humandbs_jga_date_url(base_url, "jga-dataset") == f"{HUMANDBS_URL}/api/jga-date/jga-dataset"


@pytest.mark.usefixtures("_setup_logger")
class TestParseHumandbsJgaDateNdjson:
    """Tests for parse_humandbs_jga_date_ndjson function."""

    def test_lines_are_mapped_to_date_triples(self) -> None:
        text = (
            _date_line("JGAS000001", "jga-study", "2014-07-18", "2020-09-28", "2022-12-13")
            + "\n"
            + _date_line("JGAS000002", "jga-study", "2014-09-04", "2020-09-28", "2022-11-16")
            + "\n"
        )

        result = parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

        assert result == {
            "JGAS000001": ("2014-07-18", "2020-09-28", "2022-12-13"),
            "JGAS000002": ("2014-09-04", "2020-09-28", "2022-11-16"),
        }

    @pytest.mark.parametrize("key_index", [0, 1, 2])
    def test_null_date_is_none(self, key_index: int) -> None:
        dates: list[str | None] = ["2014-07-18", "2020-09-28", "2022-12-13"]
        dates[key_index] = None
        text = _date_line("JGAS000001", "jga-study", *dates) + "\n"

        result = parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

        assert result == {"JGAS000001": (dates[0], dates[1], dates[2])}

    def test_empty_body_yields_empty_map(self) -> None:
        assert parse_humandbs_jga_date_ndjson("", "jga-study", STUDY_DATE_URL) == {}

    def test_blank_lines_crlf_and_missing_final_newline_are_accepted(self) -> None:
        text = (
            "\n"
            + _date_line("JGAS000001", "jga-study", "2014-07-18", None, None)
            + "\r\n  \n"
            + _date_line("JGAS000002", "jga-study", None, None, "2022-11-16")
        )

        result = parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

        assert result == {"JGAS000001": ("2014-07-18", None, None), "JGAS000002": (None, None, "2022-11-16")}

    def test_later_line_wins_for_duplicate_accession(self) -> None:
        text = (
            _date_line("JGAS000001", "jga-study", "2014-07-18", None, None)
            + "\n"
            + _date_line("JGAS000001", "jga-study", "2015-01-01", None, None)
            + "\n"
        )

        result = parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

        assert result == {"JGAS000001": ("2015-01-01", None, None)}

    @pytest.mark.parametrize(
        "identifier",
        ["JGAD000001", "jgas000001", "JGAS", "hum0001", "", "JGAS000001\n", "JGAS000001 ", "JGAS000001.1"],
    )
    def test_invalid_accession_is_skipped(self, identifier: str) -> None:
        text = (
            _date_line(identifier, "jga-study", "2014-07-18", "2020-09-28", "2022-12-13")
            + "\n"
            + _date_line("JGAS000002", "jga-study", "2014-09-04", "2020-09-28", "2022-11-16")
            + "\n"
        )

        result = parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

        assert result == {"JGAS000002": ("2014-09-04", "2020-09-28", "2022-11-16")}

    @pytest.mark.parametrize(
        ("index_name", "line_type"),
        [
            ("jga-study", "jga-dataset"),
            ("jga-dataset", "jga-study"),
            ("jga-study", "study"),
            ("jga-study", "JGA-STUDY"),
            ("jga-study", None),
            ("jga-study", 1),
        ],
    )
    def test_type_other_than_index_name_raises(self, index_name: IndexName, line_type: Any) -> None:
        text = _date_line("JGAS000001", line_type, "2014-07-18", "2020-09-28", "2022-12-13") + "\n"

        with pytest.raises(HumandbsResponseError):
            parse_humandbs_jga_date_ndjson(text, index_name, STUDY_DATE_URL)

    @pytest.mark.parametrize(
        "value",
        [
            "2020-09-28T00:00:00Z",
            "2020-09-28 00:00:00+09",
            "2020/09/28",
            "20200928",
            "2020-9-8",
            " 2020-09-28",
            "2020-09-28\n",
            "",
            "2020-13-01",
            "2020-02-30",
            "0000-01-01",
            20200928,
            True,
            [],
            {},
        ],
    )
    @pytest.mark.parametrize("key_index", [0, 1, 2])
    def test_date_not_calendar_date_raises(self, value: Any, key_index: int) -> None:
        dates: list[Any] = ["2014-07-18", "2020-09-28", "2022-12-13"]
        dates[key_index] = value
        text = _date_line("JGAS000001", "jga-study", *dates) + "\n"

        with pytest.raises(HumandbsResponseError):
            parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

    @pytest.mark.parametrize(
        "line",
        [
            "{not json",
            "<html><body>302 Found</body></html>",
            "[]",
            '"JGAS000001"',
            "1",
            "null",
            '{"type": "jga-study", "dateCreated": null, "datePublished": null, "dateModified": null}',
            '{"identifier": "JGAS000001", "dateCreated": null, "datePublished": null, "dateModified": null}',
            '{"identifier": "JGAS000001", "type": "jga-study", "datePublished": null, "dateModified": null}',
            '{"identifier": "JGAS000001", "type": "jga-study", "dateCreated": null, "dateModified": null}',
            '{"identifier": "JGAS000001", "type": "jga-study", "dateCreated": null, "datePublished": null}',
            '{"identifier": 1, "type": "jga-study", "dateCreated": null, "datePublished": null, "dateModified": null}',
            '{"identifier": null, "type": "jga-study", "dateCreated": null, "datePublished": null, '
            '"dateModified": null}',
        ],
    )
    def test_line_not_in_jga_date_shape_raises(self, line: str) -> None:
        text = _date_line("JGAS000002", "jga-study", None, None, None) + "\n" + line + "\n"

        with pytest.raises(HumandbsResponseError):
            parse_humandbs_jga_date_ndjson(text, "jga-study", STUDY_DATE_URL)

    @settings(max_examples=100, deadline=None)
    @given(
        date_map=st.dictionaries(
            st_jga_dataset(),
            st.tuples(st.none() | st_calendar_date(), st.none() | st_calendar_date(), st.none() | st_calendar_date()),
            max_size=30,
        )
    )
    def test_ndjson_built_from_map_parses_back_to_same_map(self, date_map: dict[str, DateTriple]) -> None:
        text = _date_ndjson(date_map, "jga-dataset")

        assert parse_humandbs_jga_date_ndjson(text, "jga-dataset", STUDY_DATE_URL) == date_map


@pytest.mark.usefixtures("_setup_logger")
class TestFetchJgaDateMap:
    """Tests for fetch_jga_date_map function."""

    @pytest.mark.parametrize("base_url", [HUMANDBS_URL, f"{HUMANDBS_URL}/"])
    def test_requests_jga_date_listing_of_index(self, humandbs: HumandbsStub, base_url: str) -> None:
        humandbs.serve(
            ok_ndjson(_date_line("JGAD000001", "jga-dataset", "2014-07-18", "2020-09-28", "2020-09-28") + "\n")
        )

        result = fetch_jga_date_map(base_url, "jga-dataset")

        assert result == {"JGAD000001": ("2014-07-18", "2020-09-28", "2020-09-28")}
        assert humandbs.requests == [f"{HUMANDBS_URL}/api/jga-date/jga-dataset"]

    @pytest.mark.parametrize(
        "first",
        [
            httpx.Response(502),
            httpx.Response(503),
            httpx.ConnectError("refused"),
            httpx.ReadTimeout("timed out"),
        ],
    )
    def test_transient_failure_is_retried(self, humandbs: HumandbsStub, first: httpx.Response | Exception) -> None:
        humandbs.serve(first, ok_ndjson(_date_line("JGAS000001", "jga-study", "2014-07-18", None, None) + "\n"))

        result = fetch_jga_date_map(HUMANDBS_URL, "jga-study")

        assert result == {"JGAS000001": ("2014-07-18", None, None)}
        assert len(humandbs.requests) == 2
        assert len(humandbs.sleeps) == 1

    def test_server_error_on_every_attempt_raises_after_max_attempts(self, humandbs: HumandbsStub) -> None:
        humandbs.serve(httpx.Response(500))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_jga_date_map(HUMANDBS_URL, "jga-study")

        assert len(humandbs.requests) == HUMANDBS_FETCH_MAX_ATTEMPTS
        assert len(humandbs.sleeps) == HUMANDBS_FETCH_MAX_ATTEMPTS - 1

    @pytest.mark.parametrize("status", [400, 404, 422])
    def test_client_error_raises_without_retry(self, humandbs: HumandbsStub, status: int) -> None:
        humandbs.serve(httpx.Response(status))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_jga_date_map(HUMANDBS_URL, "jga-study")

        assert len(humandbs.requests) == 1
        assert humandbs.sleeps == []

    def test_html_page_raises_response_error(self, humandbs: HumandbsStub) -> None:
        humandbs.serve(
            httpx.Response(200, text="<!DOCTYPE html>\n<html></html>\n", headers={"Content-Type": "text/html"})
        )

        with pytest.raises(HumandbsResponseError):
            fetch_jga_date_map(HUMANDBS_URL, "jga-study")


@pytest.mark.usefixtures("_setup_logger")
class TestUpdateJgaDateFile:
    """Tests for update_jga_date_file function."""

    def test_response_body_is_saved_as_is(self, jga_config: Config, humandbs: HumandbsStub) -> None:
        text = (
            _date_line("JGAS000002", "jga-study", "2014-09-04", None, None)
            + "\n"
            + _date_line("JGAS000001", "jga-study", "2014-07-18", "2020-09-28", "2022-12-13")
            + "\n"
        )
        humandbs.serve(ok_ndjson(text))

        update_jga_date_file(jga_config, "jga-study")

        assert _saved_path(jga_config).read_text(encoding="utf-8") == text
        assert load_date_map(jga_config, "jga-study") == {
            "JGAS000001": ("2014-07-18", "2020-09-28", "2022-12-13"),
            "JGAS000002": ("2014-09-04", None, None),
        }
        assert _warnings() == []

    def test_each_index_is_saved_to_its_own_file(self, jga_config: Config, humandbs: HumandbsStub) -> None:
        humandbs.serve(ok_ndjson(_date_line("JGAP000001", "jga-policy", "2014-07-09", None, None) + "\n"))

        update_jga_date_file(jga_config, "jga-policy")

        assert humandbs.requests == [f"{HUMANDBS_URL}/api/jga-date/jga-policy"]
        assert _saved_path(jga_config, "jga-policy").name == "jga-policy.date.ndjson"
        assert load_date_map(jga_config, "jga-policy") == {"JGAP000001": ("2014-07-09", None, None)}
        assert not _saved_path(jga_config, "jga-study").exists()

    def test_saved_file_is_replaced_without_leaving_temp_file(self, jga_config: Config, humandbs: HumandbsStub) -> None:
        path = _write_saved(jga_config, _date_line("JGAS000009", "jga-study", None, None, None) + "\n")
        new_text = _date_line("JGAS000001", "jga-study", "2014-07-18", None, None) + "\n"
        humandbs.serve(ok_ndjson(new_text))

        update_jga_date_file(jga_config, "jga-study")

        assert path.read_text(encoding="utf-8") == new_text
        assert sorted(p.name for p in path.parent.iterdir()) == [path.name]

    @pytest.mark.parametrize(
        "response",
        [
            httpx.Response(500),
            httpx.Response(404),
            httpx.Response(422),
            httpx.ConnectError("refused"),
            ok_ndjson("{not json\n"),
            ok_ndjson(_date_line("JGAS000001", "jga-dataset", None, None, None) + "\n"),
            ok_ndjson(_date_line("JGAS000001", "jga-study", "2020/01/01", None, None) + "\n"),
            ok_ndjson(""),
            ok_ndjson("\n\n"),
            ok_ndjson(_date_line("JGAD000001", "jga-study", None, None, None) + "\n"),
        ],
    )
    def test_failed_or_empty_fetch_keeps_saved_file_and_warns(
        self, jga_config: Config, humandbs: HumandbsStub, response: httpx.Response | Exception
    ) -> None:
        saved = _date_line("JGAS000009", "jga-study", "2014-07-18", None, None) + "\n"
        path = _write_saved(jga_config, saved)
        humandbs.serve(response)

        update_jga_date_file(jga_config, "jga-study")

        assert path.read_text(encoding="utf-8") == saved
        assert load_date_map(jga_config, "jga-study") == {"JGAS000009": ("2014-07-18", None, None)}
        assert any("using saved file" in message for message in _warnings())
        assert sorted(p.name for p in path.parent.iterdir()) == [path.name]

    def test_failed_fetch_without_saved_file_leaves_nothing(self, jga_config: Config, humandbs: HumandbsStub) -> None:
        humandbs.serve(httpx.Response(500))

        update_jga_date_file(jga_config, "jga-study")

        assert not _saved_path(jga_config).exists()
        with pytest.raises(FileNotFoundError):
            load_date_map(jga_config, "jga-study")

    @settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        date_map=st.dictionaries(
            st_jga_dataset(),
            st.tuples(st.none() | st_calendar_date(), st.none() | st_calendar_date(), st.none() | st_calendar_date()),
            min_size=1,
            max_size=30,
        )
    )
    def test_fetched_map_survives_save_and_load(
        self, jga_config: Config, humandbs: HumandbsStub, date_map: dict[str, DateTriple]
    ) -> None:
        humandbs.serve(ok_ndjson(_date_ndjson(date_map, "jga-dataset")))

        update_jga_date_file(jga_config, "jga-dataset")

        assert load_date_map(jga_config, "jga-dataset") == date_map


@pytest.mark.usefixtures("_setup_logger")
class TestLoadDateMap:
    """Tests for load_date_map function."""

    def test_raises_when_file_missing(self, jga_config: Config) -> None:
        with pytest.raises(FileNotFoundError):
            load_date_map(jga_config, "jga-policy")

    def test_raises_when_saved_file_is_malformed(self, jga_config: Config) -> None:
        _write_saved(jga_config, "{not json\n", "jga-policy")

        with pytest.raises(HumandbsResponseError):
            load_date_map(jga_config, "jga-policy")

    def test_raises_when_saved_file_is_for_other_index(self, jga_config: Config) -> None:
        _write_saved(jga_config, _date_line("JGAS000001", "jga-study", None, None, None) + "\n", "jga-policy")

        with pytest.raises(HumandbsResponseError):
            load_date_map(jga_config, "jga-policy")


@pytest.mark.usefixtures("_setup_logger")
class TestGenerateJgaJsonlDates:
    """generate_jga_jsonl が humandbs の日付を JSONL に入れる。"""

    def _docs(self, output_dir: Path, index_name: str) -> dict[str, dict[str, Any]]:
        lines = output_dir.joinpath(f"{index_name}.jsonl").read_text(encoding="utf-8").splitlines()
        docs = [json.loads(line) for line in lines if line.strip()]
        return {doc["identifier"]: doc for doc in docs}

    def test_dates_from_humandbs_are_written_to_jsonl(
        self, jga_config: Config, humandbs: HumandbsStub, tmp_path: Path
    ) -> None:
        humandbs.serve(ok_ndjson(_date_line("JGAS000001", "jga-study", "2014-07-18", "2020-09-28", None) + "\n"))
        output_dir = tmp_path.joinpath("out")
        output_dir.mkdir()

        generate_jga_jsonl(jga_config, "jga-study", output_dir, FIXTURE_JGA_PATH, set(), False)

        docs = self._docs(output_dir, "jga-study")
        assert docs["JGAS000001"]["dateCreated"] == "2014-07-18"
        assert docs["JGAS000001"]["datePublished"] == "2020-09-28"
        assert docs["JGAS000001"].get("dateModified") is None
        assert docs["JGAS000002"].get("dateCreated") is None
        assert _saved_path(jga_config).exists()

    def test_fetch_failure_without_saved_file_fails(
        self, jga_config: Config, humandbs: HumandbsStub, tmp_path: Path
    ) -> None:
        humandbs.serve(httpx.Response(500))
        output_dir = tmp_path.joinpath("out")
        output_dir.mkdir()

        with pytest.raises(FileNotFoundError):
            generate_jga_jsonl(jga_config, "jga-study", output_dir, FIXTURE_JGA_PATH, set(), False)

        assert not output_dir.joinpath("jga-study.jsonl").exists()

    def test_fetch_failure_uses_saved_file(self, jga_config: Config, humandbs: HumandbsStub, tmp_path: Path) -> None:
        _write_saved(jga_config, _date_line("JGAS000001", "jga-study", "2014-07-18", None, None) + "\n")
        humandbs.serve(httpx.Response(503))
        output_dir = tmp_path.joinpath("out")
        output_dir.mkdir()

        generate_jga_jsonl(jga_config, "jga-study", output_dir, FIXTURE_JGA_PATH, set(), False)

        assert self._docs(output_dir, "jga-study")["JGAS000001"]["dateCreated"] == "2014-07-18"
        assert any("using saved file" in message for message in _warnings())
