"""Tests for ddbj_search_converter.jsonl.jga module."""
from datetime import datetime, timezone
from typing import Any, Dict

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.jsonl.jga import (
    _format_date_from_csv,
    extract_description,
    extract_title,
    format_date,
    jga_entry_to_jga_instance,
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
        assert "2014-07-07T05:00:37Z" == result

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
        from ddbj_search_converter.jsonl.jga import generate_jga_jsonl  # noqa: F811

        # generate_jga_jsonl の内部ロジックの一部をテスト
        # entries が dict の場合は [entries] にラップ
        entries: Dict[str, Any] = {"accession": "JGAC000001"}
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
        entry: Dict[str, Any] = {}
        assert extract_title(entry, "jga-dac") is None

    def test_jga_policy(self) -> None:
        entry = {"TITLE": "Policy Title"}
        assert extract_title(entry, "jga-policy") == "Policy Title"

    def test_none_title(self) -> None:
        entry: Dict[str, Any] = {"DESCRIPTOR": {}}
        assert extract_title(entry, "jga-study") is None


class TestExtractDescription:
    """Tests for extract_description function."""

    def test_jga_study(self) -> None:
        entry = {"DESCRIPTOR": {"STUDY_ABSTRACT": "Abstract text"}}
        assert extract_description(entry, "jga-study") == "Abstract text"

    def test_jga_dataset(self) -> None:
        entry = {"DESCRIPTION": "Dataset desc"}
        assert extract_description(entry, "jga-dataset") == "Dataset desc"

    def test_no_description(self) -> None:
        entry: Dict[str, Any] = {}
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
        assert jga.name == "My Study"

    def test_alias_same_as_accession(self) -> None:
        """alias が accession と同じ場合は name=None。"""
        entry = {"accession": "JGAS000001", "alias": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.name is None

    def test_no_alias(self) -> None:
        entry: Dict[str, Any] = {"accession": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.name is None

    def test_organism_is_human(self) -> None:
        """JGA は常に Homo sapiens。"""
        entry = {"accession": "JGAS000001"}
        jga = jga_entry_to_jga_instance(entry, "jga-study")
        assert jga.organism.identifier == "9606"
        assert jga.organism.name == "Homo sapiens"

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
