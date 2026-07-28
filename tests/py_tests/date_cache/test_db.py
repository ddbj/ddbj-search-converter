"""date_cache.db のテスト。

窓ビルドは「既存 DB を引き継いで更新分だけ上書きする」ので、上書きされる側と
残る側の両方を見る必要がある。cache_meta は「窓ビルドに入ってよいか」の判断材料
なので、壊れている / 古い状態を「使える」と誤判定しないことを重点的に確認する。
"""

from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.date_cache.db import (
    SCHEMA_VERSION,
    DateRow,
    DateTable,
    cache_meta_is_usable,
    date_cache_ready,
    fetch_bp_accessions_modified_since_from_cache,
    fetch_bp_dates_from_cache,
    fetch_bs_accessions_modified_since_from_cache,
    fetch_bs_dates_from_cache,
    finalize_date_cache_db,
    init_date_cache_db,
    load_dates,
    read_cache_meta,
    set_cache_meta,
    write_dates_tsv,
)
from py_tests.strategies import st_timestamp_str, st_tsv_hostile_text

DATE_ROW_STRATEGY = st.tuples(
    st_tsv_hostile_text(min_size=1),
    st.one_of(st.none(), st_timestamp_str()),
    st.one_of(st.none(), st_timestamp_str()),
    st.one_of(st.none(), st_timestamp_str()),
)


def build_full(
    config: Config,
    rows_by_table: dict[DateTable, list[DateRow]],
    *,
    watermark: str = "2026-01-31",
) -> None:
    """全件ビルド相当の DB を作る。"""
    tables: tuple[DateTable, ...] = ("bp_date", "bs_date")
    init_date_cache_db(config, seed_from_final=False)
    for table in tables:
        rows = rows_by_table.get(table, [])
        written = write_dates_tsv(config, table, rows)
        load_dates(config, table, written, replace_existing=False)
        set_cache_meta(config, table, full_built_at=watermark, watermark=watermark)
    finalize_date_cache_db(config)


def build_window(config: Config, table: DateTable, rows: list[DateRow], *, watermark: str) -> None:
    """窓ビルド相当の更新をかける。"""
    init_date_cache_db(config, seed_from_final=True)
    written = write_dates_tsv(config, table, rows)
    load_dates(config, table, written, replace_existing=True)
    set_cache_meta(config, table, full_built_at=None, watermark=watermark)
    finalize_date_cache_db(config)


class TestFullBuild:
    def test_full_build_creates_tables_and_rows(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(
            config,
            {
                "bp_date": [("PRJDB1", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", None)],
                "bs_date": [("SAMD1", None, None, None)],
            },
        )

        assert fetch_bp_dates_from_cache(config, ["PRJDB1"]) == {
            "PRJDB1": ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", None)
        }
        assert fetch_bs_dates_from_cache(config, ["SAMD1"]) == {"SAMD1": (None, None, None)}

    def test_full_build_discards_rows_from_previous_generation(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB_OLD", None, None, None)]})
        build_full(config, {"bp_date": [("PRJDB_NEW", None, None, None)]})

        assert fetch_bp_dates_from_cache(config, ["PRJDB_OLD", "PRJDB_NEW"]) == {"PRJDB_NEW": (None, None, None)}

    def test_full_build_over_existing_cache_keeps_file_present(self, tmp_path: Path) -> None:
        """置き換えは atomic rename なので、cache が存在しない瞬間を作らない。"""
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB1", None, None, None)]})
        final_path = tmp_path / "bp_bs_date.duckdb"
        assert final_path.exists()

        build_full(config, {"bp_date": [("PRJDB2", None, None, None)]})
        assert final_path.exists()


class TestWindowBuild:
    def test_window_build_overwrites_matching_accession(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", None)]})

        build_window(
            config,
            "bp_date",
            [("PRJDB1", "2026-01-01T00:00:00Z", "2026-02-10T00:00:00Z", "2026-02-10T00:00:00Z")],
            watermark="2026-02-15",
        )

        assert fetch_bp_dates_from_cache(config, ["PRJDB1"]) == {
            "PRJDB1": ("2026-01-01T00:00:00Z", "2026-02-10T00:00:00Z", "2026-02-10T00:00:00Z")
        }

    def test_window_build_keeps_rows_outside_the_window(self, tmp_path: Path) -> None:
        """窓に入らなかった accession が消えないこと。全件を保持し続ける前提の要。"""
        config = Config(result_dir=tmp_path)
        build_full(
            config,
            {
                "bp_date": [
                    ("PRJDB_OLD", None, "2020-01-01T00:00:00Z", None),
                    ("PRJDB_HIT", None, "2020-01-01T00:00:00Z", None),
                ],
                "bs_date": [("SAMD_OLD", None, "2020-01-01T00:00:00Z", None)],
            },
        )

        build_window(config, "bp_date", [("PRJDB_HIT", None, "2026-02-10T00:00:00Z", None)], watermark="2026-02-15")

        dates = fetch_bp_dates_from_cache(config, ["PRJDB_OLD", "PRJDB_HIT"])
        assert dates["PRJDB_OLD"] == (None, "2020-01-01T00:00:00Z", None)
        assert dates["PRJDB_HIT"] == (None, "2026-02-10T00:00:00Z", None)
        # 触っていない側のテーブルも保持される
        assert fetch_bs_dates_from_cache(config, ["SAMD_OLD"]) == {"SAMD_OLD": (None, "2020-01-01T00:00:00Z", None)}

    def test_window_build_inserts_newly_appeared_accession(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB1", None, "2020-01-01T00:00:00Z", None)]})

        build_window(config, "bp_date", [("PRJDB_NEW", None, "2026-02-10T00:00:00Z", None)], watermark="2026-02-15")

        assert set(fetch_bp_dates_from_cache(config, ["PRJDB1", "PRJDB_NEW"])) == {"PRJDB1", "PRJDB_NEW"}

    def test_window_build_with_no_rows_keeps_cache_intact(self, tmp_path: Path) -> None:
        """更新 0 件の日でも DB が壊れない / 空にならない。"""
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB1", None, "2020-01-01T00:00:00Z", None)]})

        build_window(config, "bp_date", [], watermark="2026-02-15")

        assert fetch_bp_dates_from_cache(config, ["PRJDB1"]) == {"PRJDB1": (None, "2020-01-01T00:00:00Z", None)}
        meta = read_cache_meta(config)
        assert meta is not None
        assert meta["bp_date"].watermark == "2026-02-15"

    def test_window_build_advances_only_the_target_table_watermark(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {}, watermark="2026-01-31")

        build_window(config, "bp_date", [], watermark="2026-02-15")

        meta = read_cache_meta(config)
        assert meta is not None
        assert meta["bp_date"].watermark == "2026-02-15"
        assert meta["bs_date"].watermark == "2026-01-31"

    def test_window_build_preserves_full_built_at(self, tmp_path: Path) -> None:
        """窓ビルドは full_built_at を進めない (全件を通った事実は変わらないため)。"""
        config = Config(result_dir=tmp_path)
        build_full(config, {}, watermark="2026-01-31")

        build_window(config, "bp_date", [], watermark="2026-02-15")

        meta = read_cache_meta(config)
        assert meta is not None
        assert meta["bp_date"].full_built_at == "2026-01-31"


class TestCacheMetaUsability:
    def test_ready_after_full_build(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {})
        assert date_cache_ready(config) is True

    def test_not_ready_when_db_missing(self, tmp_path: Path) -> None:
        assert date_cache_ready(Config(result_dir=tmp_path)) is False

    def test_not_ready_when_cache_meta_table_missing(self, tmp_path: Path) -> None:
        """cache_meta を持たない旧世代の DB を「使える」と誤判定しない。"""
        config = Config(result_dir=tmp_path)
        final_path = tmp_path / "bp_bs_date.duckdb"
        with duckdb.connect(str(final_path)) as conn:
            conn.execute("CREATE TABLE bp_date (accession TEXT NOT NULL)")
            conn.execute("CREATE TABLE bs_date (accession TEXT NOT NULL)")

        assert read_cache_meta(config) is None
        assert date_cache_ready(config) is False

    def test_not_ready_when_schema_version_is_older(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {})
        with duckdb.connect(str(tmp_path / "bp_bs_date.duckdb")) as conn:
            conn.execute("UPDATE cache_meta SET schema_version = ?", (SCHEMA_VERSION - 1,))

        assert date_cache_ready(config) is False

    def test_not_ready_when_schema_version_is_newer(self, tmp_path: Path) -> None:
        """ロールバック時に、新しい形式の DB をそのまま使い続けない。"""
        config = Config(result_dir=tmp_path)
        build_full(config, {})
        with duckdb.connect(str(tmp_path / "bp_bs_date.duckdb")) as conn:
            conn.execute("UPDATE cache_meta SET schema_version = ?", (SCHEMA_VERSION + 1,))

        assert date_cache_ready(config) is False

    def test_not_ready_when_full_built_at_is_null(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {})
        with duckdb.connect(str(tmp_path / "bp_bs_date.duckdb")) as conn:
            conn.execute("UPDATE cache_meta SET full_built_at = NULL")

        assert date_cache_ready(config) is False

    def test_not_ready_when_one_table_meta_is_missing(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {})
        with duckdb.connect(str(tmp_path / "bp_bs_date.duckdb")) as conn:
            conn.execute("DELETE FROM cache_meta WHERE table_name = 'bs_date'")

        assert date_cache_ready(config) is False

    def test_none_meta_is_not_usable(self) -> None:
        assert cache_meta_is_usable(None) is False


class TestFetchDates:
    def test_fetch_empty_accessions_returns_empty(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB1", None, None, None)]})
        assert fetch_bp_dates_from_cache(config, []) == {}

    def test_fetch_missing_accession_is_omitted(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        build_full(config, {"bp_date": [("PRJDB1", None, None, None)]})
        assert fetch_bp_dates_from_cache(config, ["PRJDB_ABSENT"]) == {}


class TestFetchModifiedSince:
    @pytest.fixture
    def config(self, tmp_path: Path) -> Config:
        config = Config(result_dir=tmp_path)
        rows: list[DateRow] = [
            ("ACC_BEFORE", None, "2026-01-14T23:59:59Z", None),
            ("ACC_BOUNDARY", None, "2026-01-15T00:00:00Z", None),
            ("ACC_AFTER", None, "2026-01-16T00:00:00Z", None),
            ("ACC_NULL", None, None, None),
        ]
        build_full(config, {"bp_date": rows, "bs_date": rows})
        return config

    def test_boundary_is_inclusive(self, config: Config) -> None:
        got = fetch_bp_accessions_modified_since_from_cache(config, "2026-01-15T00:00:00Z")
        assert got == {"ACC_BOUNDARY", "ACC_AFTER"}

    def test_null_modified_is_never_returned(self, config: Config) -> None:
        got = fetch_bp_accessions_modified_since_from_cache(config, "1900-01-01T00:00:00Z")
        assert "ACC_NULL" not in got

    def test_no_match_returns_empty_set(self, config: Config) -> None:
        assert fetch_bp_accessions_modified_since_from_cache(config, "2099-01-01T00:00:00Z") == set()

    def test_bs_table_is_queried_independently(self, config: Config) -> None:
        assert fetch_bs_accessions_modified_since_from_cache(config, "2026-01-15T00:00:00Z") == {
            "ACC_BOUNDARY",
            "ACC_AFTER",
        }


class TestPbtRoundTrip:
    @given(rows=st.lists(DATE_ROW_STRATEGY, min_size=0, max_size=30, unique_by=lambda row: row[0]))
    @settings(max_examples=20, deadline=None)
    def test_full_build_roundtrip(self, rows: list[DateRow], tmp_path_factory: pytest.TempPathFactory) -> None:
        config = Config(result_dir=tmp_path_factory.mktemp("pbt"))
        build_full(config, {"bp_date": rows})

        got = fetch_bp_dates_from_cache(config, [row[0] for row in rows])
        assert got == {row[0]: (row[1], row[2], row[3]) for row in rows}

    @given(
        base=st.lists(DATE_ROW_STRATEGY, min_size=1, max_size=20, unique_by=lambda row: row[0]),
        data=st.data(),
    )
    @settings(max_examples=20, deadline=None)
    def test_window_build_equals_full_build_of_merged_rows(
        self, base: list[DateRow], data: st.DataObject, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """窓で上書きした結果が、最初からその値で全件ビルドした結果と一致する。"""
        updates = data.draw(
            st.lists(
                st.sampled_from(base).flatmap(
                    lambda row: st.tuples(
                        st.just(row[0]),
                        st.one_of(st.none(), st_timestamp_str()),
                        st.one_of(st.none(), st_timestamp_str()),
                        st.one_of(st.none(), st_timestamp_str()),
                    )
                ),
                max_size=10,
                unique_by=lambda row: row[0],
            )
        )

        windowed_config = Config(result_dir=tmp_path_factory.mktemp("window"))
        build_full(windowed_config, {"bp_date": base})
        build_window(windowed_config, "bp_date", updates, watermark="2026-02-15")

        merged = {row[0]: row for row in base}
        merged.update({row[0]: row for row in updates})
        direct_config = Config(result_dir=tmp_path_factory.mktemp("direct"))
        build_full(direct_config, {"bp_date": list(merged.values())})

        accessions = [row[0] for row in base]
        assert fetch_bp_dates_from_cache(windowed_config, accessions) == fetch_bp_dates_from_cache(
            direct_config, accessions
        )
