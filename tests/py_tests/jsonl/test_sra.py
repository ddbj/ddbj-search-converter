"""Tests for ddbj_search_converter.jsonl.sra module.

SRA モジュールは tar ファイル読み込みに強く依存するため、
ここではパース関数と正規化関数のユニットテストを中心に行う。
"""
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, cast

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.sra import (
    _normalize_accessibility,
    _normalize_status,
    parse_study,
    parse_submission,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger
from ddbj_search_converter.schema import Accessibility, Status


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


class TestNormalizeStatus:
    """Tests for _normalize_status function."""

    @pytest.mark.parametrize("input_val,expected", [
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
    ])
    def test_normalize_status(self, input_val: Optional[str], expected: str) -> None:
        result = _normalize_status(input_val)
        assert result == expected


class TestNormalizeAccessibility:
    """Tests for _normalize_accessibility function."""

    @pytest.mark.parametrize("input_val,expected", [
        (None, "public-access"),
        ("public", "public-access"),
        ("controlled", "controlled-access"),
        ("controlled-access", "controlled-access"),
        ("controlled_access", "controlled-access"),
        ("Public", "public-access"),
        ("CONTROLLED", "controlled-access"),
        ("unknown", "public-access"),
        ("", "public-access"),
    ])
    def test_normalize_accessibility(self, input_val: Optional[str], expected: str) -> None:
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
