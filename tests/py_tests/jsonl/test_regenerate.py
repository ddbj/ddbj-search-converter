"""Tests for ddbj_search_converter.jsonl.regenerate module."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.regenerate import (
    load_accessions_from_file,
    validate_accessions,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


class TestLoadAccessionsFromFile:
    """Tests for load_accessions_from_file function."""

    def test_loads_accessions(self, tmp_path: Path) -> None:
        f = tmp_path / "accessions.txt"
        f.write_text("PRJDB1\nPRJDB2\nPRJDB3\n", encoding="utf-8")
        result = load_accessions_from_file(f)
        assert result == {"PRJDB1", "PRJDB2", "PRJDB3"}

    def test_skips_comments_and_empty_lines(self, tmp_path: Path) -> None:
        f = tmp_path / "accessions.txt"
        f.write_text("# comment\n\nPRJDB1\n# another comment\nPRJDB2\n\n", encoding="utf-8")
        result = load_accessions_from_file(f)
        assert result == {"PRJDB1", "PRJDB2"}

    def test_strips_whitespace(self, tmp_path: Path) -> None:
        f = tmp_path / "accessions.txt"
        f.write_text("  PRJDB1  \n\tPRJDB2\t\n", encoding="utf-8")
        result = load_accessions_from_file(f)
        assert result == {"PRJDB1", "PRJDB2"}

    def test_empty_file(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.txt"
        f.write_text("", encoding="utf-8")
        result = load_accessions_from_file(f)
        assert result == set()


class TestValidateAccessions:
    """Tests for validate_accessions function."""

    def test_bioproject_valid(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"PRJDB12345", "PRJNA123456"}
            result = validate_accessions("bioproject", accessions)
            assert result == accessions

    def test_bioproject_invalid_filtered(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"PRJDB12345", "INVALID001", "SAMN00000001"}
            result = validate_accessions("bioproject", accessions)
            assert result == {"PRJDB12345"}

    def test_biosample_valid(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"SAMD00000001", "SAMN12345678"}
            result = validate_accessions("biosample", accessions)
            assert result == accessions

    def test_sra_valid(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"SRA123456", "SRP123456", "SRR123456"}
            result = validate_accessions("sra", accessions)
            assert result == accessions

    def test_jga_valid(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"JGAS000001", "JGAD000001"}
            result = validate_accessions("jga", accessions)
            assert result == accessions

    def test_unknown_type(self, tmp_path: Path, clean_ctx: None) -> None:
        """未知の data_type では全て invalid になる。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"PRJDB12345"}
            result = validate_accessions("unknown", accessions)
            assert result == set()

    def test_empty_input(self) -> None:
        result = validate_accessions("bioproject", set())
        assert result == set()


class TestValidateAccessionsPBT:
    """Property-based tests for validate_accessions."""

    @settings(deadline=2000)
    @given(accessions=st.frozensets(st.text(min_size=1, max_size=20), max_size=20))
    def test_result_is_subset(self, accessions: frozenset) -> None:  # type: ignore[type-arg]
        """validate_accessions の結果は常に入力の部分集合。"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                for data_type in ("bioproject", "biosample", "sra", "jga"):
                    result = validate_accessions(data_type, set(accessions))
                    assert result.issubset(accessions)
