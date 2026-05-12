"""Tests for ddbj_search_converter.jsonl.regenerate module."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.regenerate import (
    load_accessions_from_file,
    validate_accessions,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger
from py_tests.strategies import (
    st_bioproject_id,
    st_biosample_id,
    st_invalid_accession_text,
    st_jga_dac,
    st_jga_dataset,
    st_jga_policy,
    st_jga_study,
    st_sra_analysis,
    st_sra_experiment,
    st_sra_run,
    st_sra_sample,
    st_sra_study,
    st_sra_submission,
)


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
    """Property-based tests for validate_accessions.

    ``st.text()`` のような無秩序な文字列だけだと「全部 invalid → 結果は空」が常に
    成立してしまい、validate_accessions の actual matching 経路が走らない。
    valid / invalid を混合した strategy で「valid は通る・invalid は落ちる」を
    強く pin する。
    """

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

    @given(
        valid=st.frozensets(st_bioproject_id(), min_size=0, max_size=10),
        invalid_text=st.frozensets(st_invalid_accession_text("bioproject"), min_size=0, max_size=10),
    )
    def test_bioproject_partitions_correctly(self, valid: frozenset, invalid_text: frozenset) -> None:  # type: ignore[type-arg]
        """valid な BioProject ID は通り、invalid は落ちる。
        union を投入したとき、結果 = valid (なお invalid_text が偶然 valid に
        match することは strategies の filter で排除済み)。
        """
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                result = validate_accessions("bioproject", set(valid) | set(invalid_text))
                # invalid_text は必ず regex に match しないことを strategy が保証
                assert result == set(valid), f"valid={valid} invalid={invalid_text} result={result}"

    @given(
        valid=st.frozensets(st_biosample_id(), min_size=0, max_size=10),
        invalid_text=st.frozensets(st_invalid_accession_text("biosample"), min_size=0, max_size=10),
    )
    def test_biosample_partitions_correctly(self, valid: frozenset, invalid_text: frozenset) -> None:  # type: ignore[type-arg]
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                result = validate_accessions("biosample", set(valid) | set(invalid_text))
                assert result == set(valid)

    @given(
        sra_ids=st.frozensets(
            st.one_of(
                st_sra_submission(),
                st_sra_study(),
                st_sra_experiment(),
                st_sra_run(),
                st_sra_sample(),
                st_sra_analysis(),
            ),
            min_size=0,
            max_size=10,
        ),
    )
    def test_sra_accepts_all_six_subtypes(self, sra_ids: frozenset) -> None:  # type: ignore[type-arg]
        """SRA 6 subtype のいずれも通る (data_type='sra')。"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                result = validate_accessions("sra", set(sra_ids))
                assert result == set(sra_ids)

    @given(
        jga_ids=st.frozensets(
            st.one_of(st_jga_study(), st_jga_dataset(), st_jga_dac(), st_jga_policy()),
            min_size=0,
            max_size=10,
        ),
    )
    def test_jga_accepts_all_four_subtypes(self, jga_ids: frozenset) -> None:  # type: ignore[type-arg]
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            config = Config(result_dir=tmp, const_dir=tmp)
            with run_logger(config=config):
                result = validate_accessions("jga", set(jga_ids))
                assert result == set(jga_ids)
