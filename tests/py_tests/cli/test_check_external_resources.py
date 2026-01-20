"""Tests for ddbj_search_converter.cli.check_external_resources module."""
import os
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pytest

from ddbj_search_converter.logging.logger import _ctx


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestGetRequiredFiles:
    """Tests for get_required_files function."""

    def test_returns_file_list(self, tmp_path: Path) -> None:
        """必要なファイルリストを返す。"""
        from ddbj_search_converter.cli.check_external_resources import \
            get_required_files

        with patch(
            "ddbj_search_converter.cli.check_external_resources.find_latest_sra_accessions_tab_file",
            return_value=tmp_path / "sra.tab",
        ), patch(
            "ddbj_search_converter.cli.check_external_resources.find_latest_dra_accessions_tab_file",
            return_value=tmp_path / "dra.tab",
        ):
            files = get_required_files(tmp_path)

        assert len(files) > 0
        names = [name for name, _ in files]
        assert "BP Blacklist" in names
        assert "BS Blacklist" in names
        assert "SRA_Accessions.tab" in names
        assert "DRA_Accessions.tab" in names


class TestCheckExternalResourcesMain:
    """Tests for check_external_resources main function."""

    def test_main_raises_when_files_missing(
        self, tmp_path: Path, clean_ctx: None
    ) -> None:
        """ファイルがない場合は例外を投げる。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"
        const_dir.mkdir(parents=True)

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            with patch(
                "ddbj_search_converter.cli.check_external_resources.find_latest_sra_accessions_tab_file",
                return_value=None,
            ), patch(
                "ddbj_search_converter.cli.check_external_resources.find_latest_dra_accessions_tab_file",
                return_value=None,
            ):
                from ddbj_search_converter.cli.check_external_resources import \
                    main

                with pytest.raises(Exception, match="required file\\(s\\) are missing"):
                    main()

        finally:
            os.environ.clear()
            os.environ.update(original_env)

    def test_main_succeeds_when_all_files_exist(
        self, tmp_path: Path, clean_ctx: None
    ) -> None:
        """全ファイルが存在する場合は成功する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"

        (const_dir / "dblink").mkdir(parents=True)
        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "metabobank").mkdir(parents=True)

        (const_dir / "dblink" / "bp_bs_preserved.tsv").write_text("", encoding="utf-8")
        (const_dir / "bp" / "blacklist.txt").write_text("", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("", encoding="utf-8")
        (const_dir / "metabobank" / "mtb_id_bioproject_preserve.tsv").write_text("", encoding="utf-8")
        (const_dir / "metabobank" / "mtb_id_biosample_preserve.tsv").write_text("", encoding="utf-8")

        mock_files = {
            "NCBI_BIOSAMPLE_XML": tmp_path / "biosample.xml.gz",
            "DDBJ_BIOSAMPLE_XML": tmp_path / "ddbj_biosample.xml.gz",
            "NCBI_BIOPROJECT_XML": tmp_path / "bioproject.xml",
            "DDBJ_BIOPROJECT_XML": tmp_path / "ddbj_bioproject.xml",
            "JGA_STUDY_XML": tmp_path / "jga_study.xml",
            "JGA_DATASET_ANALYSIS_CSV": tmp_path / "jga1.csv",
            "JGA_ANALYSIS_STUDY_CSV": tmp_path / "jga2.csv",
            "JGA_DATASET_DATA_CSV": tmp_path / "jga3.csv",
            "JGA_DATA_EXPERIMENT_CSV": tmp_path / "jga4.csv",
            "JGA_EXPERIMENT_STUDY_CSV": tmp_path / "jga5.csv",
            "JGA_DATASET_POLICY_CSV": tmp_path / "jga6.csv",
            "JGA_POLICY_DAC_CSV": tmp_path / "jga7.csv",
            "TRAD_WGS_ORGANISM_LIST": tmp_path / "wgs.txt",
            "TRAD_TLS_ORGANISM_LIST": tmp_path / "tls.txt",
            "TRAD_TSA_ORGANISM_LIST": tmp_path / "tsa.txt",
            "TRAD_TPA_WGS_ORGANISM_LIST": tmp_path / "tpa_wgs.txt",
            "TRAD_TPA_TSA_ORGANISM_LIST": tmp_path / "tpa_tsa.txt",
            "TRAD_TPA_TLS_ORGANISM_LIST": tmp_path / "tpa_tls.txt",
        }

        for path in mock_files.values():
            path.write_text("", encoding="utf-8")

        sra_tab = tmp_path / "sra.tab"
        dra_tab = tmp_path / "dra.tab"
        sra_tab.write_text("", encoding="utf-8")
        dra_tab.write_text("", encoding="utf-8")

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            patches = [
                patch(f"ddbj_search_converter.cli.check_external_resources.{k}", v)
                for k, v in mock_files.items()
            ]
            patches.append(
                patch(
                    "ddbj_search_converter.cli.check_external_resources.find_latest_sra_accessions_tab_file",
                    return_value=sra_tab,
                )
            )
            patches.append(
                patch(
                    "ddbj_search_converter.cli.check_external_resources.find_latest_dra_accessions_tab_file",
                    return_value=dra_tab,
                )
            )

            with patches[0]:
                for p in patches[1:]:
                    with p:
                        pass

            # Apply all patches at once
            from ddbj_search_converter.cli import check_external_resources

            for k, v in mock_files.items():
                setattr(check_external_resources, k, v)

            with patch.object(
                check_external_resources,
                "find_latest_sra_accessions_tab_file",
                return_value=sra_tab,
            ), patch.object(
                check_external_resources,
                "find_latest_dra_accessions_tab_file",
                return_value=dra_tab,
            ):
                check_external_resources.main()

        finally:
            os.environ.clear()
            os.environ.update(original_env)
