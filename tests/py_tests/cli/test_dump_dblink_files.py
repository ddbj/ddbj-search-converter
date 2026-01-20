"""Tests for ddbj_search_converter.cli.dump_dblink_files module."""
import os
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (finalize_relation_db,
                                             init_dblink_db)
from ddbj_search_converter.logging.logger import _ctx


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestDumpDblinkFilesMain:
    """Tests for dump_dblink_files main function."""

    def test_main_exports_tsv_files(self, tmp_path: Path, clean_ctx: None) -> None:
        """main() が TSV ファイルを出力する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"
        output_dir = tmp_path / "dblink_output"

        config = Config(result_dir=result_dir, const_dir=const_dir)
        init_dblink_db(config)

        tmp_db = const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db)) as conn:
            conn.execute("""
                INSERT INTO relation VALUES
                ('bioproject', 'PRJDB1', 'biosample', 'SAMD1'),
                ('gea', 'E-GEAD-1', 'bioproject', 'PRJDB1')
            """)

        finalize_relation_db(config)

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            with patch(
                "ddbj_search_converter.cli.dump_dblink_files.DBLINK_OUTPUT_PATH",
                output_dir,
            ):
                from ddbj_search_converter.cli.dump_dblink_files import main
                main()

            bs_bp_file = output_dir / "biosample-bioproject" / "biosample2bioproject.tsv"
            assert bs_bp_file.exists()
            content = bs_bp_file.read_text(encoding="utf-8")
            assert "SAMD1\tPRJDB1" in content

        finally:
            os.environ.clear()
            os.environ.update(original_env)
