"""Tests for ddbj_search_converter.jsonl.regenerate module."""
from pathlib import Path

from ddbj_search_converter.jsonl.regenerate import (
    load_accessions_from_file,
    validate_accessions,
)
from ddbj_search_converter.logging.logger import run_logger
from ddbj_search_converter.config import Config


class TestLoadAccessionsFromFile:
    """Tests for load_accessions_from_file function."""

    def test_loads_accessions(self, tmp_path: Path) -> None:
        """正常にアクセッションを読み込む。"""
        file_path = tmp_path / "accessions.txt"
        content = """PRJDB1111
PRJDB2222
PRJDB3333
"""
        file_path.write_text(content, encoding="utf-8")

        result = load_accessions_from_file(file_path)

        assert result == {"PRJDB1111", "PRJDB2222", "PRJDB3333"}

    def test_skips_empty_lines(self, tmp_path: Path) -> None:
        """空行をスキップする。"""
        file_path = tmp_path / "accessions.txt"
        content = """PRJDB1111

PRJDB2222

"""
        file_path.write_text(content, encoding="utf-8")

        result = load_accessions_from_file(file_path)

        assert result == {"PRJDB1111", "PRJDB2222"}

    def test_skips_comment_lines(self, tmp_path: Path) -> None:
        """コメント行をスキップする。"""
        file_path = tmp_path / "accessions.txt"
        content = """# This is a comment
PRJDB1111
# Another comment
PRJDB2222
"""
        file_path.write_text(content, encoding="utf-8")

        result = load_accessions_from_file(file_path)

        assert result == {"PRJDB1111", "PRJDB2222"}


class TestValidateAccessions:
    """Tests for validate_accessions function."""

    def test_validates_bioproject_accessions(self, tmp_path: Path) -> None:
        """BioProject アクセッションを検証する。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"PRJDB1111", "PRJNA2222", "INVALID"}

            result = validate_accessions("bioproject", accessions)

            assert "PRJDB1111" in result
            assert "PRJNA2222" in result
            assert "INVALID" not in result

    def test_validates_biosample_accessions(self, tmp_path: Path) -> None:
        """BioSample アクセッションを検証する。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"SAMD00001", "SAMN00002", "INVALID"}

            result = validate_accessions("biosample", accessions)

            assert "SAMD00001" in result
            assert "SAMN00002" in result
            assert "INVALID" not in result

    def test_validates_sra_accessions(self, tmp_path: Path) -> None:
        """SRA アクセッションを検証する。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"SRR001", "DRX002", "INVALID"}

            result = validate_accessions("sra", accessions)

            assert "SRR001" in result
            assert "DRX002" in result
            assert "INVALID" not in result

    def test_validates_jga_accessions(self, tmp_path: Path) -> None:
        """JGA アクセッションを検証する。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            accessions = {"JGAS001", "JGAD002", "INVALID"}

            result = validate_accessions("jga", accessions)

            assert "JGAS001" in result
            assert "JGAD002" in result
            assert "INVALID" not in result
