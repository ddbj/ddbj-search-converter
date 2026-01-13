"""Tests for ddbj_search_converter.dblink.metabobank module."""
import pytest
from pathlib import Path

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.metabobank import (
    iterate_metabobank_dirs,
    load_preserve_file,
    process_metabobank_dir,
)
from ddbj_search_converter.logging.logger import run_logger


class TestIterateMetabobankDirs:
    """Tests for iterate_metabobank_dirs function."""

    def test_iterates_metabobank_directories(self, tmp_path: Path) -> None:
        """MetaboBank ディレクトリを正しく iterate する。"""
        # Create directory structure (1 層構造)
        (tmp_path / "MTBKS1").mkdir()
        (tmp_path / "MTBKS10").mkdir()
        (tmp_path / "MTBKS232").mkdir()
        (tmp_path / "other_dir").mkdir()  # Should be skipped
        (tmp_path / "livelist.txt").touch()  # Should be skipped

        result = list(iterate_metabobank_dirs(tmp_path))

        assert len(result) == 3
        names = [d.name for d in result]
        assert "MTBKS1" in names
        assert "MTBKS10" in names
        assert "MTBKS232" in names
        assert "other_dir" not in names

    def test_returns_empty_when_path_not_exists(self, tmp_path: Path) -> None:
        """存在しないパスの場合は空を返す。"""
        result = list(iterate_metabobank_dirs(tmp_path / "nonexistent"))
        assert result == []

    def test_returns_sorted_order(self, tmp_path: Path) -> None:
        """ディレクトリはソート順で返される。"""
        (tmp_path / "MTBKS3").mkdir()
        (tmp_path / "MTBKS1").mkdir()
        (tmp_path / "MTBKS2").mkdir()

        result = list(iterate_metabobank_dirs(tmp_path))
        names = [d.name for d in result]
        assert names == ["MTBKS1", "MTBKS2", "MTBKS3"]


class TestProcessMetabobankDir:
    """Tests for process_metabobank_dir function."""

    def test_processes_complete_dir(self, tmp_path: Path) -> None:
        """IDF と SDRF の両方がある場合を処理する。"""
        mtb_dir = tmp_path / "MTBKS232"
        mtb_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB17011
"""
        (mtb_dir / "MTBKS232.idf.txt").write_text(idf_content, encoding="utf-8")

        sdrf_content = """Source Name\tComment[BioSample]
JMP035\tSAMD00657132
JMP036\tSAMD00657133
"""
        (mtb_dir / "MTBKS232.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        mtb_id, bp_id, bs_ids = process_metabobank_dir(mtb_dir)

        assert mtb_id == "MTBKS232"
        assert bp_id == "PRJDB17011"
        assert bs_ids == {"SAMD00657132", "SAMD00657133"}

    def test_handles_missing_idf(self, tmp_path: Path) -> None:
        """IDF がない場合も処理できる。"""
        mtb_dir = tmp_path / "MTBKS100"
        mtb_dir.mkdir()

        sdrf_content = """Source Name\tComment[BioSample]
S1\tSAMD00001
"""
        (mtb_dir / "MTBKS100.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        mtb_id, bp_id, bs_ids = process_metabobank_dir(mtb_dir)

        assert mtb_id == "MTBKS100"
        assert bp_id is None
        assert bs_ids == {"SAMD00001"}

    def test_handles_missing_sdrf(self, tmp_path: Path) -> None:
        """SDRF がない場合も処理できる。"""
        mtb_dir = tmp_path / "MTBKS100"
        mtb_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB1234
"""
        (mtb_dir / "MTBKS100.idf.txt").write_text(idf_content, encoding="utf-8")

        mtb_id, bp_id, bs_ids = process_metabobank_dir(mtb_dir)

        assert mtb_id == "MTBKS100"
        assert bp_id == "PRJDB1234"
        assert bs_ids == set()

    def test_handles_empty_dir(self, tmp_path: Path) -> None:
        """IDF も SDRF もない場合も処理できる。"""
        mtb_dir = tmp_path / "MTBKS100"
        mtb_dir.mkdir()

        mtb_id, bp_id, bs_ids = process_metabobank_dir(mtb_dir)

        assert mtb_id == "MTBKS100"
        assert bp_id is None
        assert bs_ids == set()


class TestLoadPreserveFile:
    """Tests for load_preserve_file function."""

    @pytest.fixture
    def config(self, tmp_path: Path) -> Config:
        """テスト用 Config を作成。"""
        return Config(result_dir=tmp_path, const_dir=tmp_path)

    def test_loads_both_preserve_files(self, config: Config) -> None:
        """両方の preserve ファイルを読み込む。"""
        mtb_dir = config.const_dir / "metabobank"
        mtb_dir.mkdir(parents=True)

        bp_content = "MTBKS1\tPRJDB1111\nMTBKS2\tPRJDB2222\n"
        (mtb_dir / "mtb_id_bioproject_preserve.tsv").write_text(bp_content, encoding="utf-8")

        bs_content = "MTBKS1\tSAMD00001\nMTBKS1\tSAMD00002\nMTBKS3\tSAMD00003\n"
        (mtb_dir / "mtb_id_biosample_preserve.tsv").write_text(bs_content, encoding="utf-8")

        with run_logger(config=config):
            mtb_to_bp, mtb_to_bs = load_preserve_file(config)

        assert mtb_to_bp == {("MTBKS1", "PRJDB1111"), ("MTBKS2", "PRJDB2222")}
        assert mtb_to_bs == {("MTBKS1", "SAMD00001"), ("MTBKS1", "SAMD00002"), ("MTBKS3", "SAMD00003")}

    def test_handles_missing_files(self, config: Config) -> None:
        """ファイルが存在しない場合は空を返す。"""
        with run_logger(config=config):
            mtb_to_bp, mtb_to_bs = load_preserve_file(config)

        assert mtb_to_bp == set()
        assert mtb_to_bs == set()

    def test_handles_empty_lines(self, config: Config) -> None:
        """空行をスキップする。"""
        mtb_dir = config.const_dir / "metabobank"
        mtb_dir.mkdir(parents=True)

        bp_content = "MTBKS1\tPRJDB1111\n\nMTBKS2\tPRJDB2222\n"
        (mtb_dir / "mtb_id_bioproject_preserve.tsv").write_text(bp_content, encoding="utf-8")

        with run_logger(config=config):
            mtb_to_bp, _ = load_preserve_file(config)

        assert mtb_to_bp == {("MTBKS1", "PRJDB1111"), ("MTBKS2", "PRJDB2222")}
