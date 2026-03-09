"""Tests for ddbj_search_converter.dblink.assembly_and_master module."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.dblink.assembly_and_master import normalize_master_id, strip_version_suffix


class TestStripVersionSuffix:
    """Tests for strip_version_suffix function."""

    @pytest.mark.parametrize(
        ("raw_id", "expected"),
        [
            ("GCA_000001215.4", "GCA_000001215"),
            ("GCF_000001405.40", "GCF_000001405"),
            ("ABC.1.2", "ABC"),
            ("GCA_000001215", "GCA_000001215"),
            ("na", "na"),
        ],
    )
    def test_known_inputs(self, raw_id: str, expected: str) -> None:
        assert strip_version_suffix(raw_id) == expected


class TestStripVersionSuffixPBT:
    """Property-based tests for strip_version_suffix."""

    @given(text=st.text(min_size=0, max_size=50))
    def test_result_has_no_dot(self, text: str) -> None:
        """結果にドットが含まれない。"""
        result = strip_version_suffix(text)
        assert "." not in result

    @given(text=st.text(min_size=0, max_size=50))
    def test_idempotent(self, text: str) -> None:
        """strip_version_suffix は冪等。"""
        result1 = strip_version_suffix(text)
        result2 = strip_version_suffix(result1)
        assert result1 == result2


class TestNormalizeMasterId:
    """Tests for normalize_master_id function."""

    @pytest.mark.parametrize(
        ("raw_id", "expected"),
        [
            ("AABU00000000.1", "AABU00000000"),
            ("CP035466.1", "CP000000"),
            ("BAAA01000001-1", "BAAA00000000"),
            ("ABCD12345-2", "ABCD00000"),
            ("ABC12345", "ABC00000"),
            ("ABC00000-1", "ABC00000"),
            ("ABC00000.1", "ABC00000"),
            ("ABC00000", "ABC00000"),
            ("A12345-1", "A00000"),
            ("A12345.1", "A00000"),
            ("ABCDEF12345678-1", "ABCDEF00000000"),
            ("na", "na"),
            ("ABC123.1-2", "ABC000"),
            ("ABC123-1.2", "ABC000"),
        ],
    )
    def test_known_inputs(self, raw_id: str, expected: str) -> None:
        assert normalize_master_id(raw_id) == expected


class TestNormalizeMasterIdPBT:
    """Property-based tests for normalize_master_id."""

    @given(text=st.from_regex(r"[A-Z]{1,6}\d{5,10}", fullmatch=True))
    def test_result_digits_are_zero(self, text: str) -> None:
        """normalize_master_id の結果の数字は全て 0。"""
        result = normalize_master_id(text)
        for ch in result:
            if ch.isdigit():
                assert ch == "0"

    @given(text=st.from_regex(r"[A-Z]{1,6}\d{5,10}", fullmatch=True))
    def test_idempotent(self, text: str) -> None:
        """normalize_master_id は冪等。"""
        result1 = normalize_master_id(text)
        result2 = normalize_master_id(result1)
        assert result1 == result2

    @given(text=st.text(min_size=0, max_size=50))
    def test_no_dot_in_result(self, text: str) -> None:
        """結果にドットが含まれない。"""
        result = normalize_master_id(text)
        assert "." not in result

    @given(text=st.text(min_size=0, max_size=50))
    def test_no_hyphen_in_result(self, text: str) -> None:
        """結果にハイフンが含まれない。"""
        result = normalize_master_id(text)
        assert "-" not in result


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_string(self) -> None:
        assert strip_version_suffix("") == ""
        assert normalize_master_id("") == ""

    def test_dot_only(self) -> None:
        assert strip_version_suffix(".") == ""
        assert normalize_master_id(".") == ""

    def test_hyphen_only(self) -> None:
        assert strip_version_suffix("-") == "-"
        assert normalize_master_id("-") == ""

    def test_leading_dot(self) -> None:
        assert strip_version_suffix(".abc") == ""

    def test_digits_only(self) -> None:
        assert normalize_master_id("12345") == "00000"
