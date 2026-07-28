"""status_cache.db のテスト。

status cache は毎回全件で作り直すので、窓ビルドのような差分の観点はない。
TSV を経由する投入が値を歪めないこと、finalize が accession の一意性を保証する
ことを見る。
"""

from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.status_cache.db import (
    StatusTable,
    fetch_bp_statuses_from_cache,
    fetch_bs_statuses_from_cache,
    finalize_status_cache_db,
    init_status_cache_db,
    insert_statuses,
    status_cache_exists,
)
from py_tests.strategies import st_tsv_hostile_text

STATUS_STRATEGY = st.sampled_from(["public", "private", "suppressed", "withdrawn"])


def build_cache(config: Config, rows_by_table: dict[StatusTable, list[tuple[str, str]]]) -> None:
    tables: tuple[StatusTable, ...] = ("bp_status", "bs_status")
    init_status_cache_db(config)
    for table in tables:
        insert_statuses(config, table, rows_by_table.get(table, []))
    finalize_status_cache_db(config)


class TestBuild:
    def test_insert_returns_row_count(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        init_status_cache_db(config)
        assert insert_statuses(config, "bp_status", [("PRJDB1", "public"), ("PRJDB2", "withdrawn")]) == 2

    def test_insert_empty_returns_zero(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        init_status_cache_db(config)
        assert insert_statuses(config, "bp_status", []) == 0

    def test_roundtrip_through_cache(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_cache(
            config,
            {
                "bp_status": [("PRJDB1", "public"), ("PRJDB2", "suppressed")],
                "bs_status": [("SAMD1", "withdrawn")],
            },
        )

        assert fetch_bp_statuses_from_cache(config, ["PRJDB1", "PRJDB2"]) == {
            "PRJDB1": "public",
            "PRJDB2": "suppressed",
        }
        assert fetch_bs_statuses_from_cache(config, ["SAMD1"]) == {"SAMD1": "withdrawn"}

    def test_cache_exists_only_after_finalize(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        assert status_cache_exists(config) is False
        init_status_cache_db(config)
        assert status_cache_exists(config) is False
        finalize_status_cache_db(config)
        assert status_cache_exists(config) is True

    def test_rebuild_over_existing_cache_keeps_file_present(self, tmp_path: Path) -> None:
        """置き換えは atomic rename なので、cache が存在しない瞬間を作らない。"""
        config = Config(result_dir=tmp_path)
        build_cache(config, {"bp_status": [("PRJDB1", "public")]})
        final_path = tmp_path / "bp_bs_status.duckdb"
        assert final_path.exists()

        build_cache(config, {"bp_status": [("PRJDB2", "public")]})
        assert final_path.exists()
        assert fetch_bp_statuses_from_cache(config, ["PRJDB1", "PRJDB2"]) == {"PRJDB2": "public"}

    def test_duplicate_accession_fails_at_finalize(self, tmp_path: Path) -> None:
        """同一 accession が 2 度出たら、どちらが勝つか曖昧なまま通さない。"""
        config = Config(result_dir=tmp_path)
        init_status_cache_db(config)
        insert_statuses(config, "bp_status", [("PRJDB1", "public"), ("PRJDB1", "withdrawn")])

        with pytest.raises(duckdb.ConstraintException):
            finalize_status_cache_db(config)


class TestFetch:
    def test_fetch_empty_accessions_returns_empty(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_cache(config, {"bp_status": [("PRJDB1", "public")]})
        assert fetch_bp_statuses_from_cache(config, []) == {}

    def test_fetch_missing_accession_is_omitted(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_cache(config, {"bp_status": [("PRJDB1", "public")]})
        assert fetch_bp_statuses_from_cache(config, ["PRJDB_ABSENT"]) == {}

    def test_tables_are_queried_independently(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_cache(config, {"bp_status": [("ACC", "public")], "bs_status": [("ACC", "withdrawn")]})

        assert fetch_bp_statuses_from_cache(config, ["ACC"]) == {"ACC": "public"}
        assert fetch_bs_statuses_from_cache(config, ["ACC"]) == {"ACC": "withdrawn"}


class TestPbtRoundTrip:
    @given(
        rows=st.lists(
            st.tuples(st_tsv_hostile_text(min_size=1), STATUS_STRATEGY),
            min_size=0,
            max_size=30,
            unique_by=lambda row: row[0],
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_roundtrip_preserves_rows(
        self, rows: list[tuple[str, str]], tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        config = Config(result_dir=tmp_path_factory.mktemp("pbt"))
        build_cache(config, {"bp_status": rows})

        assert fetch_bp_statuses_from_cache(config, [row[0] for row in rows]) == dict(rows)
