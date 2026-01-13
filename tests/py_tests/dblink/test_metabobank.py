"""Tests for ddbj_search_converter.dblink.metabobank module."""
from pathlib import Path

from ddbj_search_converter.dblink.metabobank import (
    iterate_metabobank_dirs,
    process_metabobank_dir,
)


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
