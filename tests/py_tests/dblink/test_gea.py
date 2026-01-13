"""Tests for ddbj_search_converter.dblink.gea module."""
from pathlib import Path

from ddbj_search_converter.dblink.gea import iterate_gea_dirs, process_gea_dir


class TestIterateGeaDirs:
    """Tests for iterate_gea_dirs function."""

    def test_iterates_gea_directories(self, tmp_path: Path) -> None:
        """GEA ディレクトリを正しく iterate する。"""
        # Create directory structure
        (tmp_path / "E-GEAD-000" / "E-GEAD-100").mkdir(parents=True)
        (tmp_path / "E-GEAD-000" / "E-GEAD-200").mkdir(parents=True)
        (tmp_path / "E-GEAD-1000" / "E-GEAD-1001").mkdir(parents=True)
        (tmp_path / "livelist.txt").touch()  # Should be skipped

        result = list(iterate_gea_dirs(tmp_path))

        assert len(result) == 3
        names = [d.name for d in result]
        assert "E-GEAD-100" in names
        assert "E-GEAD-200" in names
        assert "E-GEAD-1001" in names

    def test_returns_empty_when_path_not_exists(self, tmp_path: Path) -> None:
        """存在しないパスの場合は空を返す。"""
        result = list(iterate_gea_dirs(tmp_path / "nonexistent"))
        assert result == []


class TestProcessGeaDir:
    """Tests for process_gea_dir function."""

    def test_processes_complete_gea_dir(self, tmp_path: Path) -> None:
        """IDF と SDRF の両方がある場合を処理する。"""
        gea_dir = tmp_path / "E-GEAD-291"
        gea_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB7770
"""
        (gea_dir / "E-GEAD-291.idf.txt").write_text(idf_content, encoding="utf-8")

        sdrf_content = """Source Name\tComment[BioSample]
AF\tSAMD00001
"""
        (gea_dir / "E-GEAD-291.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        gea_id, bp_id, bs_ids = process_gea_dir(gea_dir)

        assert gea_id == "E-GEAD-291"
        assert bp_id == "PRJDB7770"
        assert bs_ids == {"SAMD00001"}

    def test_handles_missing_idf(self, tmp_path: Path) -> None:
        """IDF がない場合も処理できる。"""
        gea_dir = tmp_path / "E-GEAD-100"
        gea_dir.mkdir()

        sdrf_content = """Source Name\tComment[BioSample]
AF\tSAMD00001
"""
        (gea_dir / "E-GEAD-100.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        gea_id, bp_id, bs_ids = process_gea_dir(gea_dir)

        assert gea_id == "E-GEAD-100"
        assert bp_id is None
        assert bs_ids == {"SAMD00001"}

    def test_handles_missing_sdrf(self, tmp_path: Path) -> None:
        """SDRF がない場合も処理できる。"""
        gea_dir = tmp_path / "E-GEAD-100"
        gea_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB1234
"""
        (gea_dir / "E-GEAD-100.idf.txt").write_text(idf_content, encoding="utf-8")

        gea_id, bp_id, bs_ids = process_gea_dir(gea_dir)

        assert gea_id == "E-GEAD-100"
        assert bp_id == "PRJDB1234"
        assert bs_ids == set()
