"""Tests for ddbj_search_converter.dblink.gea module (pure logic only)."""

from pathlib import Path

from ddbj_search_converter.dblink.gea import iterate_gea_dirs
from ddbj_search_converter.dblink.idf_sdrf import process_idf_sdrf_dir


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


class TestProcessIdfSdrfDir:
    """Tests for process_idf_sdrf_dir function (GEA context)."""

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

        result = process_idf_sdrf_dir(gea_dir)

        assert result.entry_id == "E-GEAD-291"
        assert result.bioproject == "PRJDB7770"
        assert result.biosamples == {"SAMD00001"}

    def test_handles_missing_idf(self, tmp_path: Path) -> None:
        """IDF がない場合も処理できる。"""
        gea_dir = tmp_path / "E-GEAD-100"
        gea_dir.mkdir()

        sdrf_content = """Source Name\tComment[BioSample]
AF\tSAMD00001
"""
        (gea_dir / "E-GEAD-100.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        result = process_idf_sdrf_dir(gea_dir)

        assert result.entry_id == "E-GEAD-100"
        assert result.bioproject is None
        assert result.biosamples == {"SAMD00001"}

    def test_handles_missing_sdrf(self, tmp_path: Path) -> None:
        """SDRF がない場合も処理できる。"""
        gea_dir = tmp_path / "E-GEAD-100"
        gea_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB1234
"""
        (gea_dir / "E-GEAD-100.idf.txt").write_text(idf_content, encoding="utf-8")

        result = process_idf_sdrf_dir(gea_dir)

        assert result.entry_id == "E-GEAD-100"
        assert result.bioproject == "PRJDB1234"
        assert result.biosamples == set()


class TestGeaSdrfSraHarvest:
    """Tests for GEA SDRF SRA_RUN / SRA_EXPERIMENT harvesting (G/M10)."""

    def test_real_fixture_e_gead_1096_has_sra_values(self) -> None:
        """実 fixture E-GEAD-1096 で SRA_RUN / SRA_EXPERIMENT を抽出できる (E2E)。"""
        fixture_dir = (
            Path(__file__).parent.parent.parent / "fixtures/usr/local/resources/gea/experiment/E-GEAD-1000/E-GEAD-1096"
        )
        assert fixture_dir.exists(), f"fixture missing: {fixture_dir}"

        result = process_idf_sdrf_dir(fixture_dir)

        assert result.entry_id == "E-GEAD-1096"
        assert len(result.biosamples) > 0, "biosamples should be populated"
        assert len(result.sra_runs) > 0, "sra_runs should be populated for E-GEAD-1096"
        assert len(result.sra_experiments) > 0, "sra_experiments should be populated"
        # format check
        for run_id in result.sra_runs:
            assert run_id[0] in "SDE", f"bad sra-run prefix: {run_id}"
            assert run_id[1:3] == "RR", f"bad sra-run body: {run_id}"
        for exp_id in result.sra_experiments:
            assert exp_id[0] in "SDE", f"bad sra-experiment prefix: {exp_id}"
            assert exp_id[1:3] == "RX", f"bad sra-experiment body: {exp_id}"
