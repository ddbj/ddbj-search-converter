"""date_cache.build のテスト。

この build が過去に本番を止めてきた失敗は「PostgreSQL から読みながら DuckDB へ
書き、接続を掴んだまま切られる」という構造に由来する。したがって最重要の不変
条件は「DuckDB への投入が始まる時点で PostgreSQL からの取得が終わっていること」
で、それを実際の DuckDB に対して確認する。
"""

import datetime
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from pytest_mock import MockerFixture

from ddbj_search_converter.config import DEFAULT_MARGIN_DAYS, Config
from ddbj_search_converter.date_cache.build import (
    BP_QUERY,
    BS_QUERY,
    DATE_SOURCES,
    DateSource,
    build_date_cache,
    build_query,
    window_start,
)
from ddbj_search_converter.date_cache.db import (
    DateRow,
    date_cache_ready,
    fetch_bp_dates_from_cache,
    read_cache_meta,
)

TODAY = datetime.date(2026, 2, 20)


def rows_for(table: str) -> list[DateRow]:
    prefix = "PRJDB" if table == "bp_date" else "SAMD"
    return [(f"{prefix}1", None, "2026-02-10T00:00:00Z", None)]


def patch_fetch(mocker: MockerFixture, captured: dict[str, str | None] | None = None) -> None:
    """PostgreSQL アクセスだけを差し替える。DuckDB 側は本物を使う。"""

    def fake_fetch(postgres_url: str, source: DateSource, since: str | None) -> Iterator[DateRow]:
        if captured is not None:
            captured[source.table] = since
        yield from rows_for(source.table)

    mocker.patch("ddbj_search_converter.date_cache.build._fetch_dates", side_effect=fake_fetch)


class TestBuildQuery:
    def test_without_since_returns_base_query_unchanged(self) -> None:
        assert build_query(BP_QUERY, None) == BP_QUERY

    @pytest.mark.parametrize("base", [BP_QUERY, BS_QUERY], ids=["bp", "bs"])
    def test_with_since_adds_modified_date_lower_bound(self, base: str) -> None:
        query = build_query(base, "2026-01-01")
        assert "p.modified_date >= %s" in query
        assert query.startswith(base)

    @pytest.mark.parametrize("base", [BP_QUERY, BS_QUERY], ids=["bp", "bs"])
    def test_with_since_has_exactly_one_placeholder(self, base: str) -> None:
        assert build_query(base, "2026-01-01").count("%s") == 1


class TestQueryShape:
    def test_bs_query_joins_on_smp_id(self) -> None:
        """accession と sample 行は smp_id で 1:1。submission_id で畳み込むと、
        1 submission 内の 1 行の日付が全 accession に配られてしまう。
        """
        assert "s.smp_id = p.smp_id" in BS_QUERY
        assert "DISTINCT ON" not in BS_QUERY

    def test_bp_query_joins_on_submission_id(self) -> None:
        # BP は mass.project 側が submission_id で一意なので畳み込みが起きない。
        assert "s.submission_id = p.submission_id" in BP_QUERY

    def test_queries_target_their_own_tables(self) -> None:
        assert "mass.bioproject_summary" in BP_QUERY
        assert "mass.project" in BP_QUERY
        assert "mass.biosample_summary" in BS_QUERY
        assert "mass.sample" in BS_QUERY
        assert "biosample" not in BP_QUERY
        assert "bioproject" not in BS_QUERY

    def test_sources_cover_both_tables_with_distinct_databases(self) -> None:
        assert {source.table for source in DATE_SOURCES} == {"bp_date", "bs_date"}
        assert len({source.postgres_db_name for source in DATE_SOURCES}) == len(DATE_SOURCES)
        assert len({source.cursor_name for source in DATE_SOURCES}) == len(DATE_SOURCES)


class TestWindowStart:
    def test_subtracts_default_margin(self) -> None:
        assert window_start("2026-02-20") == "2026-01-21"

    def test_margin_is_the_shared_default(self) -> None:
        watermark = datetime.date(2026, 2, 20)
        expected = (watermark - datetime.timedelta(days=DEFAULT_MARGIN_DAYS)).strftime("%Y-%m-%d")
        assert window_start(watermark.strftime("%Y-%m-%d")) == expected

    def test_crosses_month_and_year_boundaries(self) -> None:
        assert window_start("2026-01-05") == "2025-12-06"


class TestConnectionIsClosedBeforeLoad:
    def test_no_rows_are_in_duckdb_while_postgres_is_still_being_read(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """取得の generator が閉じる時点で、DuckDB にはまだ 1 行も入っていない。

        generator の finally は PostgreSQL 接続を閉じる位置そのもの。ここで一時 DB
        を writer として開けて、かつ空であることは「取得を終えてから投入を始める」
        構造になっている証拠になる。取得と投入が再び 1 つのループに融合したら、
        writer が競合するか行が見えるかのどちらかでここが落ちる。
        """
        config = Config(result_dir=tmp_path)
        tmp_db_path = tmp_path / "bp_bs_date.tmp.duckdb"
        observed: dict[str, int] = {}

        def fake_fetch(postgres_url: str, source: DateSource, since: str | None) -> Iterator[DateRow]:
            try:
                yield from rows_for(source.table)
            finally:
                with duckdb.connect(str(tmp_db_path)) as conn:
                    observed[source.table] = conn.execute(f"SELECT count(*) FROM {source.table}").fetchone()[0]

        mocker.patch("ddbj_search_converter.date_cache.build._fetch_dates", side_effect=fake_fetch)

        build_date_cache(config, full=True, today=TODAY)

        assert observed == {"bp_date": 0, "bs_date": 0}
        # 投入自体はその後きちんと行われている
        assert fetch_bp_dates_from_cache(config, ["PRJDB1"]) == {"PRJDB1": (None, "2026-02-10T00:00:00Z", None)}


class TestFullBuild:
    def test_full_build_records_full_built_at_and_watermark(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = Config(result_dir=tmp_path)
        patch_fetch(mocker)

        build_date_cache(config, full=True, today=TODAY)

        meta = read_cache_meta(config)
        assert meta is not None
        for table in ("bp_date", "bs_date"):
            assert meta[table].full_built_at == "2026-02-20"
            assert meta[table].watermark == "2026-02-20"
        assert date_cache_ready(config) is True

    def test_full_build_fetches_without_since(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = Config(result_dir=tmp_path)
        captured: dict[str, str | None] = {}
        patch_fetch(mocker, captured)

        build_date_cache(config, full=True, today=TODAY)

        assert captured == {"bp_date": None, "bs_date": None}

    def test_missing_cache_falls_back_to_full_build(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = Config(result_dir=tmp_path)
        captured: dict[str, str | None] = {}
        patch_fetch(mocker, captured)

        build_date_cache(config, full=False, today=TODAY)

        assert captured == {"bp_date": None, "bs_date": None}
        assert date_cache_ready(config) is True

    @pytest.mark.parametrize(
        "corruption",
        [
            "UPDATE cache_meta SET schema_version = schema_version - 1",
            "UPDATE cache_meta SET full_built_at = NULL",
            "DELETE FROM cache_meta",
            "DROP TABLE cache_meta",
        ],
        ids=["older-schema", "never-full-built", "no-rows", "no-table"],
    )
    def test_unusable_cache_falls_back_to_full_build(
        self, corruption: str, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """窓ビルドの土台にできない DB を、黙って差分更新の基準に使わない。"""
        config = Config(result_dir=tmp_path)
        patch_fetch(mocker)
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 1))
        with duckdb.connect(str(tmp_path / "bp_bs_date.duckdb")) as conn:
            conn.execute(corruption)

        captured: dict[str, str | None] = {}
        patch_fetch(mocker, captured)
        build_date_cache(config, full=False, today=TODAY)

        assert captured == {"bp_date": None, "bs_date": None}


class TestWindowBuild:
    def test_window_build_uses_watermark_minus_margin(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = Config(result_dir=tmp_path)
        patch_fetch(mocker)
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 20))

        captured: dict[str, str | None] = {}
        patch_fetch(mocker, captured)
        build_date_cache(config, full=False, today=datetime.date(2026, 2, 25))

        assert captured == {"bp_date": "2026-01-21", "bs_date": "2026-01-21"}

    def test_window_build_advances_watermark_but_not_full_built_at(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        config = Config(result_dir=tmp_path)
        patch_fetch(mocker)
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 20))
        build_date_cache(config, full=False, today=datetime.date(2026, 2, 25))

        meta = read_cache_meta(config)
        assert meta is not None
        assert meta["bp_date"].watermark == "2026-02-25"
        assert meta["bp_date"].full_built_at == "2026-02-20"

    def test_missed_days_widen_the_window(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """日次が止まっていた間は watermark が進まないので、窓がその分広がる。"""
        config = Config(result_dir=tmp_path)
        patch_fetch(mocker)
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 1))

        captured: dict[str, str | None] = {}
        patch_fetch(mocker, captured)
        build_date_cache(config, full=False, today=datetime.date(2026, 3, 15))

        # 直近 30 日ではなく、最後に取り込んだ 2026-02-01 から 30 日戻る
        assert captured["bp_date"] == "2026-01-02"

    def test_window_build_keeps_untouched_rows(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = Config(result_dir=tmp_path)
        mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_dates",
            side_effect=lambda url, source, since: iter(
                [("PRJDB_OLD", None, "2020-01-01T00:00:00Z", None)] if source.table == "bp_date" else []
            ),
        )
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 20))

        patch_fetch(mocker)
        build_date_cache(config, full=False, today=datetime.date(2026, 2, 25))

        dates = fetch_bp_dates_from_cache(config, ["PRJDB_OLD", "PRJDB1"])
        assert dates["PRJDB_OLD"] == (None, "2020-01-01T00:00:00Z", None)
        assert dates["PRJDB1"] == (None, "2026-02-10T00:00:00Z", None)

    def test_explicit_full_ignores_existing_watermark(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = Config(result_dir=tmp_path)
        patch_fetch(mocker)
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 20))

        captured: dict[str, str | None] = {}
        patch_fetch(mocker, captured)
        build_date_cache(config, full=True, today=datetime.date(2026, 2, 25))

        assert captured == {"bp_date": None, "bs_date": None}
