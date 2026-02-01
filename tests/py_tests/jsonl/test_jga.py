"""Tests for ddbj_search_converter.jsonl.jga module."""
from pathlib import Path
from typing import Any, Dict

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.jga import (_format_date_from_csv,
                                             extract_description,
                                             extract_title,
                                             jga_entry_to_jga_instance,
                                             load_date_map, load_jga_xml)
from ddbj_search_converter.jsonl.utils import write_jsonl
from ddbj_search_converter.schema import JGA


class TestLoadJgaXml:
    """Tests for load_jga_xml function."""

    def test_loads_valid_xml(self, tmp_path: Path) -> None:
        """正常な XML を読み込む。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
    <STUDY accession="JGAS000001" alias="Test Study">
        <DESCRIPTOR>
            <STUDY_TITLE>Test Title</STUDY_TITLE>
            <STUDY_ABSTRACT>Test Abstract</STUDY_ABSTRACT>
        </DESCRIPTOR>
    </STUDY>
</STUDY_SET>
"""
        xml_path = tmp_path / "jga-study.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = load_jga_xml(xml_path)

        assert "STUDY_SET" in result
        assert "STUDY" in result["STUDY_SET"]


class TestFormatDateFromCsv:
    """Tests for _format_date_from_csv function."""

    def test_formats_date_with_timezone(self) -> None:
        """タイムゾーン付き日付をフォーマットする。"""
        result = _format_date_from_csv("2014-07-07 14:00:37.208+09")

        assert result == "2014-07-07T05:00:37Z"

    def test_formats_date_with_full_timezone(self) -> None:
        """完全なタイムゾーン形式の日付をフォーマットする。"""
        result = _format_date_from_csv("2014-07-07 14:00:37+09:00")

        assert result == "2014-07-07T05:00:37Z"

    def test_formats_date_with_z_timezone(self) -> None:
        """Z タイムゾーンの日付をフォーマットする。"""
        result = _format_date_from_csv("2014-07-07T14:00:37Z")

        assert result == "2014-07-07T14:00:37Z"


class TestLoadDateMap:
    """Tests for load_date_map function."""

    def test_loads_valid_csv(self, tmp_path: Path) -> None:
        """正常な CSV を読み込む。"""
        csv_content = """accession,dateCreated,datePublished,dateModified
JGAS000001,2014-07-07 14:00:37+09:00,2014-07-08 15:00:00+09:00,2014-07-09 16:00:00+09:00
"""
        csv_path = tmp_path / "study.date.csv"
        csv_path.write_text(csv_content, encoding="utf-8")

        result = load_date_map(tmp_path, "jga-study")

        assert "JGAS000001" in result
        assert result["JGAS000001"][0] == "2014-07-07T05:00:37Z"
        assert result["JGAS000001"][1] == "2014-07-08T06:00:00Z"
        assert result["JGAS000001"][2] == "2014-07-09T07:00:00Z"

    def test_raises_when_file_not_exists(self, tmp_path: Path) -> None:
        """ファイルが存在しない場合は FileNotFoundError。"""
        with pytest.raises(FileNotFoundError):
            load_date_map(tmp_path, "jga-study")


class TestExtractTitle:
    """Tests for extract_title function."""

    def test_extracts_study_title(self) -> None:
        """jga-study からタイトルを抽出する。"""
        entry: Dict[str, Any] = {
            "accession": "JGAS000001",
            "DESCRIPTOR": {"STUDY_TITLE": "Test Title"},
        }

        result = extract_title(entry, "jga-study")

        assert result == "Test Title"

    def test_extracts_dataset_title(self) -> None:
        """jga-dataset からタイトルを抽出する。"""
        entry: Dict[str, Any] = {
            "accession": "JGAD000001",
            "TITLE": "Dataset Title",
        }

        result = extract_title(entry, "jga-dataset")

        assert result == "Dataset Title"

    def test_extracts_policy_title(self) -> None:
        """jga-policy からタイトルを抽出する。"""
        entry: Dict[str, Any] = {
            "accession": "JGAP000001",
            "TITLE": "Policy Title",
        }

        result = extract_title(entry, "jga-policy")

        assert result == "Policy Title"

    def test_returns_none_for_dac(self) -> None:
        """jga-dac は None を返す。"""
        entry: Dict[str, Any] = {"accession": "JGAC000001"}

        result = extract_title(entry, "jga-dac")

        assert result is None


class TestExtractDescription:
    """Tests for extract_description function."""

    def test_extracts_study_abstract(self) -> None:
        """jga-study から説明を抽出する。"""
        entry: Dict[str, Any] = {
            "accession": "JGAS000001",
            "DESCRIPTOR": {"STUDY_ABSTRACT": "Test Abstract"},
        }

        result = extract_description(entry, "jga-study")

        assert result == "Test Abstract"

    def test_extracts_dataset_description(self) -> None:
        """jga-dataset から説明を抽出する。"""
        entry: Dict[str, Any] = {
            "accession": "JGAD000001",
            "DESCRIPTION": "Dataset Description",
        }

        result = extract_description(entry, "jga-dataset")

        assert result == "Dataset Description"

    def test_returns_none_for_policy(self) -> None:
        """jga-policy は None を返す。"""
        entry: Dict[str, Any] = {"accession": "JGAP000001"}

        result = extract_description(entry, "jga-policy")

        assert result is None


class TestJgaEntryToJgaInstance:
    """Tests for jga_entry_to_jga_instance function."""

    def test_converts_study_entry(self) -> None:
        """jga-study エントリを変換する。"""
        entry: Dict[str, Any] = {
            "accession": "JGAS000001",
            "alias": "Test Study",
            "DESCRIPTOR": {
                "STUDY_TITLE": "Test Title",
                "STUDY_ABSTRACT": "Test Abstract",
            },
        }

        result = jga_entry_to_jga_instance(entry, "jga-study")

        assert result.identifier == "JGAS000001"
        assert result.name == "Test Study"
        assert result.type_ == "jga-study"
        assert result.title == "Test Title"
        assert result.description == "Test Abstract"
        assert result.organism is not None
        assert result.organism.identifier == "9606"
        assert result.accessibility == "controlled-access"

    def test_uses_accession_as_name_when_no_alias(self) -> None:
        """alias がない場合は accession を name とする。"""
        entry: Dict[str, Any] = {"accession": "JGAS000001"}

        result = jga_entry_to_jga_instance(entry, "jga-study")

        assert result.name == "JGAS000001"


class TestWriteJsonl:
    """Tests for write_jsonl function."""

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        """JSONL ファイルを書き込む。"""
        jga = JGA(
            identifier="JGAS000001",
            properties={},
            distribution=[],
            isPartOf="jga",
            type="jga-study",
            name="Test",
            url="https://example.com",
            organism=None,
            title=None,
            description=None,
            dbXrefs=[],
            sameAs=[],
            status="live",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [jga])

        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "JGAS000001" in content
        assert '"type":"jga-study"' in content

    def test_writes_multiple_entries(self, tmp_path: Path) -> None:
        """複数エントリを書き込む。"""
        jga1 = JGA(
            identifier="JGAS000001",
            properties={},
            distribution=[],
            isPartOf="jga",
            type="jga-study",
            name="Test1",
            url="https://example.com/1",
            organism=None,
            title=None,
            description=None,
            dbXrefs=[],
            sameAs=[],
            status="live",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )
        jga2 = JGA(
            identifier="JGAS000002",
            properties={},
            distribution=[],
            isPartOf="jga",
            type="jga-study",
            name="Test2",
            url="https://example.com/2",
            organism=None,
            title=None,
            description=None,
            dbXrefs=[],
            sameAs=[],
            status="live",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [jga1, jga2])

        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert "JGAS000001" in lines[0]
        assert "JGAS000002" in lines[1]
