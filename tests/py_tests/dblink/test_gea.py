"""Tests for ddbj_search_converter.dblink.gea module."""
import os
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import init_dblink_db
from ddbj_search_converter.dblink.gea import (iterate_gea_dirs, main,
                                              process_gea_dir)
from ddbj_search_converter.logging.logger import _ctx


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


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


class TestGeaMain:
    """Integration tests for gea main function."""

    def test_main_processes_gea_and_saves_to_db(
        self, tmp_path: Path, clean_ctx: None
    ) -> None:
        """main() が GEA データを処理して DB に保存する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"
        gea_dir = tmp_path / "gea"

        (const_dir / "bp").mkdir(parents=True)
        (const_dir / "bs").mkdir(parents=True)
        (const_dir / "bp" / "blacklist.txt").write_text("", encoding="utf-8")
        (const_dir / "bs" / "blacklist.txt").write_text("", encoding="utf-8")

        (gea_dir / "E-GEAD-000" / "E-GEAD-100").mkdir(parents=True)
        idf = """Comment[BioProject]\tPRJDB1234
"""
        sdrf = """Source Name\tComment[BioSample]
Sample1\tSAMD00001
"""
        (gea_dir / "E-GEAD-000" / "E-GEAD-100" / "E-GEAD-100.idf.txt").write_text(
            idf, encoding="utf-8"
        )
        (gea_dir / "E-GEAD-000" / "E-GEAD-100" / "E-GEAD-100.sdrf.txt").write_text(
            sdrf, encoding="utf-8"
        )

        config = Config(result_dir=result_dir, const_dir=const_dir)
        init_dblink_db(config)

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            with patch("ddbj_search_converter.dblink.gea.GEA_BASE_PATH", gea_dir):
                main()

            db_path = const_dir / "dblink" / "dblink.tmp.duckdb"
            with duckdb.connect(str(db_path)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
                assert count is not None
                assert count[0] == 2

        finally:
            os.environ.clear()
            os.environ.update(original_env)
