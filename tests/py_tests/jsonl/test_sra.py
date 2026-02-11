"""Tests for ddbj_search_converter.jsonl.sra module.

SRA モジュールは tar ファイル読み込みに強く依存するため、
ここではパース関数と正規化関数のユニットテストを中心に行う。
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.sra import (
    XML_TYPES,
    _normalize_accessibility,
    _normalize_status,
    create_sra_entry,
    parse_study,
    parse_submission,
    process_submission_xml,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger
from ddbj_search_converter.schema import SRA
from ddbj_search_converter.sra.tar_reader import SraXmlType


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


class TestNormalizeStatus:
    """Tests for _normalize_status function."""

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            (None, "live"),
            ("live", "live"),
            ("Live", "live"),
            ("unpublished", "unpublished"),
            ("suppressed", "suppressed"),
            ("withdrawn", "withdrawn"),
            ("public", "live"),
            ("replaced", "withdrawn"),
            ("killed", "withdrawn"),
            ("LIVE", "live"),
            ("PUBLIC", "live"),
            ("unknown_status", "live"),
            ("", "live"),
        ],
    )
    def test_normalize_status(self, input_val: str | None, expected: str) -> None:
        result = _normalize_status(input_val)
        assert result == expected


class TestNormalizeAccessibility:
    """Tests for _normalize_accessibility function."""

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            (None, "public-access"),
            ("public", "public-access"),
            ("controlled", "controlled-access"),
            ("controlled-access", "controlled-access"),
            ("controlled_access", "controlled-access"),
            ("Public", "public-access"),
            ("CONTROLLED", "controlled-access"),
            ("unknown", "public-access"),
            ("", "public-access"),
        ],
    )
    def test_normalize_accessibility(self, input_val: str | None, expected: str) -> None:
        result = _normalize_accessibility(input_val)
        assert result == expected


class TestParseSubmission:
    """Tests for parse_submission function."""

    def test_valid_submission(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="SRA123456" submission_date="2024-01-01" submission_comment="A comment">
  <TITLE>Test Submission</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml_bytes, "SRA123456")
        assert result is not None
        assert result["accession"] == "SRA123456"
        assert result["title"] == "Test Submission"
        assert result["description"] == "A comment"

    def test_empty_submission(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="SRA123456">
</SUBMISSION>
"""
        result = parse_submission(xml_bytes, "SRA123456")
        assert result is not None
        assert result["title"] is None

    def test_invalid_xml(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = parse_submission(b"<invalid", "X001")
            assert result is None

    def test_no_submission_element(self) -> None:
        xml_bytes = b"<OTHER>content</OTHER>"
        result = parse_submission(xml_bytes, "X001")
        assert result is None


class TestParseStudy:
    """Tests for parse_study function."""

    def test_valid_study(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
  <STUDY accession="SRP123456" alias="my_study">
    <DESCRIPTOR>
      <STUDY_TITLE>My Study Title</STUDY_TITLE>
      <STUDY_ABSTRACT>My study abstract</STUDY_ABSTRACT>
    </DESCRIPTOR>
  </STUDY>
</STUDY_SET>
"""
        results = parse_study(xml_bytes, "SRA123456")
        assert len(results) == 1
        assert results[0]["accession"] == "SRP123456"
        assert results[0]["title"] == "My Study Title"
        assert results[0]["description"] == "My study abstract"

    def test_empty_study_set(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
</STUDY_SET>
"""
        results = parse_study(xml_bytes, "SRA123456")
        assert results == []

    def test_invalid_xml(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            results = parse_study(b"<invalid", "X001")
            assert results == []


class TestEdgeCases:
    """Edge case tests."""

    def test_status_case_insensitive(self) -> None:
        """大文字小文字を問わない。"""
        assert _normalize_status("LIVE") == "live"
        assert _normalize_status("Suppressed") == "suppressed"
        assert _normalize_status("WITHDRAWN") == "withdrawn"

    def test_accessibility_underscore_handling(self) -> None:
        """アンダースコアがハイフンに変換される。"""
        assert _normalize_accessibility("controlled_access") == "controlled-access"
        assert _normalize_accessibility("CONTROLLED_ACCESS") == "controlled-access"


class TestProcessSubmissionXml:
    """Tests for process_submission_xml function."""

    _SUBMISSION_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="DRA000001" submission_date="2020-01-01">
  <TITLE>Test DRA Submission</TITLE>
</SUBMISSION>
"""

    _STUDY_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
  <STUDY accession="DRP000001">
    <DESCRIPTOR>
      <STUDY_TITLE>Test Study</STUDY_TITLE>
    </DESCRIPTOR>
  </STUDY>
</STUDY_SET>
"""

    def test_dra_uses_received_from_accessions_tab(self) -> None:
        """DRA でも Accessions.tab の Received が dateCreated に使われる。"""
        accession_info: dict[str, tuple[str, str, str | None, str | None, str | None, str]] = {
            "DRA000001": ("live", "public", "2024-06-15", "2024-07-01", "2024-08-01", "submission"),
            "DRP000001": ("live", "public", "2024-06-15", "2024-07-01", "2024-08-01", "study"),
        }
        xml_cache: dict[SraXmlType, bytes | None] = {
            "submission": self._SUBMISSION_XML,
            "study": self._STUDY_XML,
            "experiment": None,
            "run": None,
            "sample": None,
            "analysis": None,
        }

        results = process_submission_xml(
            submission="DRA000001",
            blacklist=set(),
            accession_info=accession_info,
            xml_cache=xml_cache,
        )

        # submission の dateCreated が Accessions.tab の Received であること
        assert len(results["submission"]) == 1
        assert results["submission"][0].dateCreated == "2024-06-15"

        # study も同様に Accessions.tab の Received であること
        assert len(results["study"]) == 1
        assert results["study"][0].dateCreated == "2024-06-15"

    def test_received_none_results_in_date_created_none(self) -> None:
        """Received が None の場合、dateCreated が None になる。"""
        accession_info: dict[str, tuple[str, str, str | None, str | None, str | None, str]] = {
            "DRA000001": ("live", "public", None, None, None, "submission"),
            "DRP000001": ("live", "public", None, None, None, "study"),
        }
        xml_cache: dict[SraXmlType, bytes | None] = {
            "submission": self._SUBMISSION_XML,
            "study": self._STUDY_XML,
            "experiment": None,
            "run": None,
            "sample": None,
            "analysis": None,
        }

        results = process_submission_xml(
            submission="DRA000001",
            blacklist=set(),
            accession_info=accession_info,
            xml_cache=xml_cache,
        )

        assert len(results["submission"]) == 1
        assert results["submission"][0].dateCreated is None

        assert len(results["study"]) == 1
        assert results["study"][0].dateCreated is None


def _make_sra_entry(identifier: str, sra_type: SraXmlType = "study") -> SRA:
    """テスト用の SRA エントリを作成するヘルパー。"""
    parsed: dict[str, Any] = {
        "accession": identifier,
        "properties": {},
        "alias": None,
        "title": f"Title for {identifier}",
        "description": None,
    }
    return create_sra_entry(
        sra_type=sra_type,
        parsed=parsed,
        status="live",
        accessibility="public-access",
        date_created=None,
        date_modified=None,
        date_published=None,
    )


class TestBatchDedup:
    """_process_batch_worker の重複排除ロジックを検証する。

    実際の _process_batch_worker は tar 読み込みや DB 依存が大きいため、
    Step 3 の重複排除ロジック部分を単体で再現してテストする。
    """

    def test_dedup_removes_duplicates_within_same_type(self) -> None:
        """同一 xml_type 内で重複する identifier が排除される。"""
        entries = [
            _make_sra_entry("SRP000001", "study"),
            _make_sra_entry("SRP000002", "study"),
            _make_sra_entry("SRP000001", "study"),  # duplicate
            _make_sra_entry("SRP000003", "study"),
        ]

        batch_entries: list[SRA] = []
        seen_ids: set[str] = set()
        for entry in entries:
            if entry.identifier not in seen_ids:
                batch_entries.append(entry)
                seen_ids.add(entry.identifier)

        assert len(batch_entries) == 3
        ids = [e.identifier for e in batch_entries]
        assert ids == ["SRP000001", "SRP000002", "SRP000003"]

    def test_dedup_across_submissions(self) -> None:
        """異なる submission から生成された同一 identifier が排除される。"""
        sub1_results = [
            _make_sra_entry("SRP000001", "study"),
            _make_sra_entry("SRP000002", "study"),
        ]
        sub2_results = [
            _make_sra_entry("SRP000001", "study"),  # same as sub1
            _make_sra_entry("SRP000003", "study"),
        ]

        batch_entries: dict[SraXmlType, list[SRA]] = {t: [] for t in XML_TYPES}
        seen_ids: dict[SraXmlType, set[str]] = {t: set() for t in XML_TYPES}

        for results in [sub1_results, sub2_results]:
            for entry in results:
                if entry.identifier not in seen_ids["study"]:
                    batch_entries["study"].append(entry)
                    seen_ids["study"].add(entry.identifier)

        assert len(batch_entries["study"]) == 3
        ids = [e.identifier for e in batch_entries["study"]]
        assert ids == ["SRP000001", "SRP000002", "SRP000003"]

    def test_dedup_independent_across_types(self) -> None:
        """異なる xml_type 間では重複排除が独立して行われる。"""
        study_entries = [_make_sra_entry("SRP000001", "study")]
        sample_entries = [_make_sra_entry("SRS000001", "sample")]

        batch_entries: dict[SraXmlType, list[SRA]] = {t: [] for t in XML_TYPES}
        seen_ids: dict[SraXmlType, set[str]] = {t: set() for t in XML_TYPES}

        for entry in study_entries:
            if entry.identifier not in seen_ids["study"]:
                batch_entries["study"].append(entry)
                seen_ids["study"].add(entry.identifier)

        for entry in sample_entries:
            if entry.identifier not in seen_ids["sample"]:
                batch_entries["sample"].append(entry)
                seen_ids["sample"].add(entry.identifier)

        assert len(batch_entries["study"]) == 1
        assert len(batch_entries["sample"]) == 1

    def test_dedup_preserves_first_occurrence(self) -> None:
        """重複がある場合、最初の出現が保持される。"""
        entry1 = _make_sra_entry("SRP000001", "study")
        entry1_dup = _make_sra_entry("SRP000001", "study")

        batch_entries: list[SRA] = []
        seen_ids: set[str] = set()
        for entry in [entry1, entry1_dup]:
            if entry.identifier not in seen_ids:
                batch_entries.append(entry)
                seen_ids.add(entry.identifier)

        assert len(batch_entries) == 1
        assert batch_entries[0] is entry1

    def test_no_duplicates_all_kept(self) -> None:
        """重複がない場合、全エントリが保持される。"""
        entries = [
            _make_sra_entry("SRP000001", "study"),
            _make_sra_entry("SRP000002", "study"),
            _make_sra_entry("SRP000003", "study"),
        ]

        batch_entries: list[SRA] = []
        seen_ids: set[str] = set()
        for entry in entries:
            if entry.identifier not in seen_ids:
                batch_entries.append(entry)
                seen_ids.add(entry.identifier)

        assert len(batch_entries) == 3
