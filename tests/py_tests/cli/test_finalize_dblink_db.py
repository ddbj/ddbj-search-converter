"""Tests for ddbj_search_converter.cli.finalize_dblink_db module."""
import os
from pathlib import Path
from typing import Generator

import duckdb
import pytest

from ddbj_search_converter.dblink.db import init_dblink_db
from ddbj_search_converter.logging.logger import _ctx


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestFinalizeDblinkDbMain:
    """Tests for finalize_dblink_db main function."""

    def test_main_finalizes_database(self, tmp_path: Path, clean_ctx: None) -> None:
        """main() が DB を finalize する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"

        from ddbj_search_converter.config import Config
        config = Config(result_dir=result_dir, const_dir=const_dir)
        init_dblink_db(config)

        tmp_db = const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            from ddbj_search_converter.cli.finalize_dblink_db import main
            main()

            assert not tmp_db.exists()
            final_db = const_dir / "dblink" / "dblink.duckdb"
            assert final_db.exists()

            with duckdb.connect(str(final_db)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
                assert count is not None
                assert count[0] == 1

        finally:
            os.environ.clear()
            os.environ.update(original_env)
