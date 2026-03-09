"""Tests for ddbj_search_converter.dblink.utils module."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.utils import (
    convert_id_if_needed,
    filter_by_blacklist,
    filter_pairs_by_blacklist,
    filter_sra_pairs_by_blacklist,
    load_blacklist,
    load_sra_blacklist,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


class TestFilterByBlacklist:
    """Tests for filter_by_blacklist function."""

    def test_filters_both_sides(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            bs_to_bp = {("SAMD00001", "PRJDB1111"), ("SAMD00002", "PRJDB2222")}
            bp_blacklist = {"PRJDB1111"}
            bs_blacklist = {"SAMD00003"}
            result = filter_by_blacklist(bs_to_bp, bp_blacklist, bs_blacklist)
            assert result == {("SAMD00002", "PRJDB2222")}

    def test_filters_biosample_side(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            bs_to_bp = {("SAMD00001", "PRJDB1111"), ("SAMD00002", "PRJDB2222")}
            result = filter_by_blacklist(bs_to_bp, set(), {"SAMD00001"})
            assert result == {("SAMD00002", "PRJDB2222")}

    def test_empty_input(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = filter_by_blacklist(set(), {"X"}, {"Y"})
            assert result == set()


class TestFilterPairsByBlacklist:
    """Tests for filter_pairs_by_blacklist function."""

    @pytest.mark.parametrize(
        ("position", "blacklist", "expected"),
        [
            ("left", {"A1"}, {("A2", "B2"), ("A3", "B3")}),
            ("right", {"B1"}, {("A2", "B2"), ("A3", "B3")}),
            ("both", {"A1", "B2"}, {("A3", "B3")}),
        ],
    )
    def test_filter_positions(
        self,
        tmp_path: Path,
        clean_ctx: None,
        position: str,
        blacklist: set[str],
        expected: set[tuple[str, str]],
    ) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            pairs = {("A1", "B1"), ("A2", "B2"), ("A3", "B3")}
            result = filter_pairs_by_blacklist(pairs, blacklist, position)  # type: ignore[arg-type]
            assert result == expected

    def test_returns_all_when_no_match(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            pairs = {("A1", "B1"), ("A2", "B2")}
            result = filter_pairs_by_blacklist(pairs, {"C1"}, "left")
            assert result == pairs


class TestFilterSraPairsByBlacklist:
    """Tests for filter_sra_pairs_by_blacklist function."""

    def test_filters_sra_pairs(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            pairs = {("SRR001", "SRX001"), ("SRR002", "SRX002")}
            result = filter_sra_pairs_by_blacklist(pairs, {"SRR001"})
            assert result == {("SRR002", "SRX002")}

    def test_returns_all_when_empty_blacklist(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            pairs = {("SRR001", "SRX001"), ("SRR002", "SRX002")}
            result = filter_sra_pairs_by_blacklist(pairs, set())
            assert result == pairs


class TestLoadBlacklist:
    """Tests for load_blacklist function."""

    def test_loads_both_blacklists(self, tmp_path: Path, clean_ctx: None) -> None:
        const_dir = tmp_path / "const"
        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "bp" / "blacklist.txt").write_text("PRJDB1111\nPRJDB2222\n", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("SAMD00001\n", encoding="utf-8")

        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            bp_blacklist, bs_blacklist = load_blacklist(config)
            assert bp_blacklist == {"PRJDB1111", "PRJDB2222"}
            assert bs_blacklist == {"SAMD00001"}

    def test_returns_empty_when_files_not_exist(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            bp_blacklist, bs_blacklist = load_blacklist(config)
            assert bp_blacklist == set()
            assert bs_blacklist == set()


class TestBug4TrailingWhitespace:
    """Bug #4 (fixed): load_blacklist が各行の空白を strip する。"""

    def test_trailing_whitespace_should_be_stripped(self, tmp_path: Path, clean_ctx: None) -> None:
        const_dir = tmp_path / "const"
        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "bp" / "blacklist.txt").write_text("PRJDB1111  \nPRJDB2222\t\n", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("", encoding="utf-8")

        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            bp_blacklist, _ = load_blacklist(config)
            assert "PRJDB1111" in bp_blacklist
            assert "PRJDB2222" in bp_blacklist


class TestBug5CommentLines:
    """Bug #5 (fixed): load_blacklist がコメント行 (#) をスキップする。"""

    def test_comment_lines_should_be_skipped(self, tmp_path: Path, clean_ctx: None) -> None:
        const_dir = tmp_path / "const"
        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "bp" / "blacklist.txt").write_text("# This is a comment\nPRJDB1111\n", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("", encoding="utf-8")

        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            bp_blacklist, _ = load_blacklist(config)
            assert "# This is a comment" not in bp_blacklist
            assert bp_blacklist == {"PRJDB1111"}


class TestLoadSraBlacklist:
    """Tests for load_sra_blacklist function."""

    def test_loads_sra_blacklist_with_comments(self, tmp_path: Path, clean_ctx: None) -> None:
        const_dir = tmp_path / "const"
        (const_dir / "sra").mkdir(parents=True)
        content = "# Comment line\nSRR001\nSRX002\n# Another comment\nSRS003\n"
        (const_dir / "sra" / "blacklist.txt").write_text(content, encoding="utf-8")

        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            result = load_sra_blacklist(config)
            assert result == {"SRR001", "SRX002", "SRS003"}

    def test_returns_empty_when_file_not_exists(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = load_sra_blacklist(config)
            assert result == set()


class TestPBT:
    """Property-based tests for filter functions."""

    @settings(deadline=2000)
    @given(
        pairs=st.frozensets(st.tuples(st.text(min_size=1, max_size=10), st.text(min_size=1, max_size=10)), max_size=20),
        blacklist=st.frozensets(st.text(min_size=1, max_size=10), max_size=5),
    )
    def test_filter_by_blacklist_is_subset(self, pairs: frozenset, blacklist: frozenset) -> None:  # type: ignore[type-arg]
        """filter_by_blacklist の結果は常に入力の部分集合。"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                result = filter_by_blacklist(set(pairs), set(blacklist), set(blacklist))
                assert result.issubset(pairs)

    @given(
        pairs=st.frozensets(st.tuples(st.text(min_size=1, max_size=10), st.text(min_size=1, max_size=10)), max_size=20),
        blacklist=st.frozensets(st.text(min_size=1, max_size=10), max_size=5),
    )
    def test_blacklisted_ids_not_in_result(self, pairs: frozenset, blacklist: frozenset) -> None:  # type: ignore[type-arg]
        """blacklist に含まれる ID を持つペアは結果に含まれない。"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                bl = set(blacklist)
                result = filter_by_blacklist(set(pairs), bl, bl)
                for bs, bp in result:
                    assert bs not in bl
                    assert bp not in bl

    @given(
        pairs=st.frozensets(st.tuples(st.text(min_size=1, max_size=10), st.text(min_size=1, max_size=10)), max_size=20),
        blacklist=st.frozensets(st.text(min_size=1, max_size=10), max_size=5),
    )
    def test_sra_filter_is_subset(self, pairs: frozenset, blacklist: frozenset) -> None:  # type: ignore[type-arg]
        """filter_sra_pairs_by_blacklist の結果は常に入力の部分集合。"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                result = filter_sra_pairs_by_blacklist(set(pairs), set(blacklist))
                assert result.issubset(pairs)


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_blacklist_file(self, tmp_path: Path, clean_ctx: None) -> None:
        """空の blacklist ファイル。"""
        const_dir = tmp_path / "const"
        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "bp" / "blacklist.txt").write_text("", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("", encoding="utf-8")

        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            bp_bl, bs_bl = load_blacklist(config)
            assert bp_bl == set()
            assert bs_bl == set()

    def test_blacklist_file_with_only_empty_lines(self, tmp_path: Path, clean_ctx: None) -> None:
        """空行のみの blacklist ファイル。"""
        const_dir = tmp_path / "const"
        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "bp" / "blacklist.txt").write_text("\n\n\n", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("", encoding="utf-8")

        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            bp_bl, _ = load_blacklist(config)
            assert bp_bl == set()


class TestConvertIdIfNeeded:
    """Tests for convert_id_if_needed function."""

    def test_valid_accession_returned_as_is(self, tmp_path: Path, clean_ctx: None) -> None:
        """有効な accession はそのまま返される。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = convert_id_if_needed("PRJNA123", "bioproject", {}, "test", "sra")
            assert result == "PRJNA123"

    def test_numeric_id_converted_via_mapping(self, tmp_path: Path, clean_ctx: None) -> None:
        """数値 ID は mapping で accession に変換される。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            mapping = {"123": "PRJNA123"}
            result = convert_id_if_needed("123", "bioproject", mapping, "test", "sra")
            assert result == "PRJNA123"

    def test_unconvertible_id_returns_none(self, tmp_path: Path, clean_ctx: None) -> None:
        """変換不能な ID は None を返す。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = convert_id_if_needed("999", "bioproject", {}, "test", "sra")
            assert result is None

    def test_biosample_accession_returned(self, tmp_path: Path, clean_ctx: None) -> None:
        """BioSample accession はそのまま返される。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = convert_id_if_needed("SAMD00000001", "biosample", {}, "test", "sra")
            assert result == "SAMD00000001"

    def test_biosample_numeric_id_converted(self, tmp_path: Path, clean_ctx: None) -> None:
        """BioSample 数値 ID は mapping で変換される。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            mapping = {"456": "SAMN00000456"}
            result = convert_id_if_needed("456", "biosample", mapping, "test", "sra")
            assert result == "SAMN00000456"
