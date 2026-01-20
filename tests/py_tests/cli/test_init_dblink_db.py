"""Tests for ddbj_search_converter.cli.init_dblink_db module."""
import os
from pathlib import Path
from typing import Generator

import duckdb
import pytest

from ddbj_search_converter.logging.logger import _ctx


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestInitDblinkDbMain:
    """Tests for init_dblink_db main function."""

    def test_main_creates_database(self, tmp_path: Path, clean_ctx: None) -> None:
        """main() が正常に DB を作成する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            from ddbj_search_converter.cli.init_dblink_db import main
            main()

            db_path = const_dir / "dblink" / "dblink.tmp.duckdb"
            assert db_path.exists()

            with duckdb.connect(str(db_path)) as conn:
                tables = conn.execute("SHOW TABLES").fetchall()
                assert ("relation",) in tables

        finally:
            os.environ.clear()
            os.environ.update(original_env)

    def test_main_logs_activity(self, tmp_path: Path, clean_ctx: None) -> None:
        """main() がログファイルを作成する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            from ddbj_search_converter.cli.init_dblink_db import main
            main()

            log_dir = result_dir / "logs"
            assert log_dir.exists()
            log_files = list(log_dir.glob("*.log.jsonl"))
            assert len(log_files) == 1

        finally:
            os.environ.clear()
            os.environ.update(original_env)
