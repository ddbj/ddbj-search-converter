"""Tests for ddbj_search_converter.date_cache.build module."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from pytest_mock import MockerFixture

from ddbj_search_converter.config import Config
from ddbj_search_converter.date_cache.build import (
    BP_POSTGRES_DB_NAME,
    BP_QUERY,
    BS_POSTGRES_DB_NAME,
    BS_QUERY,
    CURSOR_ITERSIZE,
    _fetch_all_bp_dates,
    _fetch_all_bs_dates,
    build_date_cache,
)


def _make_named_cursor_mock(rows: list[tuple[Any, ...]]) -> MagicMock:
    """名前付き cursor を模した MagicMock を返す。

    `with conn.cursor(name=...) as cur:` パターンを成立させる必要がある。
    """
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    cur.__iter__.return_value = iter(rows)
    return cur


def _make_connection_mock(cur: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestFetchAllBpDates:
    """_fetch_all_bp_dates の接続パラメータ・cursor 設定・format_date 適用。"""

    def test_connects_with_bioproject_dbname_and_parsed_url(self, mocker: MockerFixture) -> None:
        """parse_postgres_url の結果と dbname=bioproject で connect される。"""
        cur = _make_named_cursor_mock([])
        conn = _make_connection_mock(cur)
        mock_connect = mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        list(_fetch_all_bp_dates("postgresql://alice:secret@db.example:6543"))

        kwargs = mock_connect.call_args.kwargs
        assert kwargs["host"] == "db.example"
        assert kwargs["port"] == 6543
        assert kwargs["user"] == "alice"
        assert kwargs["password"] == "secret"
        assert kwargs["dbname"] == BP_POSTGRES_DB_NAME == "bioproject"

    def test_uses_named_cursor_with_itersize(self, mocker: MockerFixture) -> None:
        """server-side cursor (named) を CURSOR_ITERSIZE で構成する。"""
        cur = _make_named_cursor_mock([])
        conn = _make_connection_mock(cur)
        mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        list(_fetch_all_bp_dates("postgresql://u:p@h:5432"))

        # cursor 名は固定 (server-side cursor 化のため必須)
        conn.cursor.assert_called_once_with(name="bp_date_cursor")
        # itersize が CURSOR_ITERSIZE で設定される
        assert cur.itersize == CURSOR_ITERSIZE
        # SQL は BP_QUERY が実行される
        cur.execute.assert_called_once_with(BP_QUERY)

    def test_passes_keepalive_options_to_connect(self, mocker: MockerFixture) -> None:
        """psycopg2.connect に keepalives 群が固定値で渡される (idle 切断対策)。"""
        cur = _make_named_cursor_mock([])
        conn = _make_connection_mock(cur)
        mock_connect = mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        list(_fetch_all_bp_dates("postgresql://u:p@h:5432"))

        kwargs = mock_connect.call_args.kwargs
        assert kwargs["keepalives"] == 1
        assert kwargs["keepalives_idle"] == 60
        assert kwargs["keepalives_interval"] == 10
        assert kwargs["keepalives_count"] == 5

    def test_itersize_set_before_execute(self, mocker: MockerFixture) -> None:
        """itersize は execute の前に設定される (named cursor 仕様: fetch 開始後の変更は無効)。

        execute の side_effect で「呼ばれた瞬間の itersize」をキャプチャする。
        """
        cur = _make_named_cursor_mock([])
        conn = _make_connection_mock(cur)
        mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        captured_itersize: list[int] = []

        def capture(_query: str) -> None:
            captured_itersize.append(cur.itersize)

        cur.execute.side_effect = capture

        list(_fetch_all_bp_dates("postgresql://u:p@h:5432"))

        assert captured_itersize == [CURSOR_ITERSIZE]

    def test_format_date_applied_to_yielded_rows(self, mocker: MockerFixture) -> None:
        """yield 値の 2-4 番目は format_date 経由 (datetime → ISO Z 文字列、None は None)。"""
        rows: list[tuple[Any, ...]] = [
            (
                "PRJDB1",
                datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 1, 2, 12, 30, 45, tzinfo=timezone.utc),
                None,
            ),
            ("PRJDB2", None, None, None),
        ]
        cur = _make_named_cursor_mock(rows)
        conn = _make_connection_mock(cur)
        mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        result = list(_fetch_all_bp_dates("postgresql://u:p@h:5432"))

        assert result == [
            ("PRJDB1", "2026-01-01T00:00:00Z", "2026-01-02T12:30:45Z", None),
            ("PRJDB2", None, None, None),
        ]

    def test_connection_closed_after_iteration(self, mocker: MockerFixture) -> None:
        """yield 完了後、conn.close() が finally で呼ばれる。"""
        cur = _make_named_cursor_mock([("PRJDB1", None, None, None)])
        conn = _make_connection_mock(cur)
        mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        list(_fetch_all_bp_dates("postgresql://u:p@h:5432"))

        conn.close.assert_called_once()

    def test_connection_closed_on_exception(self, mocker: MockerFixture) -> None:
        """cursor 反復中に例外が出ても conn.close() が呼ばれる (finally)。"""
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None

        def raising_iter() -> Any:
            raise RuntimeError("network glitch")
            yield  # pragma: no cover

        cur.__iter__.side_effect = raising_iter
        conn = _make_connection_mock(cur)
        mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        with pytest.raises(RuntimeError):
            list(_fetch_all_bp_dates("postgresql://u:p@h:5432"))

        conn.close.assert_called_once()


class TestFetchAllBsDates:
    """_fetch_all_bs_dates は BP と同型 (dbname=biosample, named cursor=bs_date_cursor, BS_QUERY)。"""

    def test_connects_with_biosample_dbname(self, mocker: MockerFixture) -> None:
        cur = _make_named_cursor_mock([])
        conn = _make_connection_mock(cur)
        mock_connect = mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        list(_fetch_all_bs_dates("postgresql://u:p@h:5432"))

        assert mock_connect.call_args.kwargs["dbname"] == BS_POSTGRES_DB_NAME == "biosample"

    def test_uses_bs_named_cursor_with_itersize_and_query(self, mocker: MockerFixture) -> None:
        cur = _make_named_cursor_mock([])
        conn = _make_connection_mock(cur)
        mocker.patch(
            "ddbj_search_converter.date_cache.build.psycopg2.connect",
            return_value=conn,
        )

        list(_fetch_all_bs_dates("postgresql://u:p@h:5432"))

        conn.cursor.assert_called_once_with(name="bs_date_cursor")
        assert cur.itersize == CURSOR_ITERSIZE
        cur.execute.assert_called_once_with(BS_QUERY)


class TestQueryStringsContainExpectedTables:
    """SQL クエリ文字列が SSOT のテーブル名を参照している。"""

    def test_bp_query_targets_bioproject_tables(self) -> None:
        assert "mass.bioproject_summary" in BP_QUERY
        assert "mass.project" in BP_QUERY

    def test_bs_query_targets_biosample_tables(self) -> None:
        assert "mass.biosample_summary" in BS_QUERY
        assert "mass.sample" in BS_QUERY

    def test_bp_and_bs_queries_are_distinct(self) -> None:
        """BP_QUERY と BS_QUERY は別 SQL (コピペ間違いで同一になる回帰を検出)。"""
        assert BP_QUERY != BS_QUERY
        # BP は biosample_summary を含まず、BS は bioproject_summary を含まない
        assert "mass.biosample_summary" not in BP_QUERY
        assert "mass.bioproject_summary" not in BS_QUERY


class TestBuildDateCacheOrder:
    """build_date_cache 全体の呼び出し順序。"""

    def test_calls_in_expected_order(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """init → fetch_bp → insert_bp → fetch_bs → insert_bs → finalize の順。"""
        config = Config(
            result_dir=tmp_path,
            xsm_postgres_url="postgresql://u:p@h:5432",
        )

        mocker.patch("ddbj_search_converter.date_cache.build.log_info")
        mock_init = mocker.patch("ddbj_search_converter.date_cache.build.init_date_cache_db")
        mock_fetch_bp = mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_all_bp_dates",
            return_value=iter([("PRJDB1", None, None, None)]),
        )
        mock_insert_bp = mocker.patch(
            "ddbj_search_converter.date_cache.build.insert_bp_dates",
            return_value=1,
        )
        mock_fetch_bs = mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_all_bs_dates",
            return_value=iter([("SAMD1", None, None, None)]),
        )
        mock_insert_bs = mocker.patch(
            "ddbj_search_converter.date_cache.build.insert_bs_dates",
            return_value=1,
        )
        mock_finalize = mocker.patch("ddbj_search_converter.date_cache.build.finalize_date_cache_db")

        # parent mock を attach して呼び出し順を観察する
        parent = Mock()
        parent.attach_mock(mock_init, "init")
        parent.attach_mock(mock_fetch_bp, "fetch_bp")
        parent.attach_mock(mock_insert_bp, "insert_bp")
        parent.attach_mock(mock_fetch_bs, "fetch_bs")
        parent.attach_mock(mock_insert_bs, "insert_bs")
        parent.attach_mock(mock_finalize, "finalize")

        build_date_cache(config)

        names = [call[0] for call in parent.mock_calls]
        assert names == ["init", "fetch_bp", "insert_bp", "fetch_bs", "insert_bs", "finalize"]

    def test_passes_xsm_postgres_url_to_fetchers(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """fetch_bp / fetch_bs に config.xsm_postgres_url が渡される。"""
        config = Config(
            result_dir=tmp_path,
            xsm_postgres_url="postgresql://u:p@h:5432",
        )

        mocker.patch("ddbj_search_converter.date_cache.build.log_info")
        mocker.patch("ddbj_search_converter.date_cache.build.init_date_cache_db")
        mocker.patch("ddbj_search_converter.date_cache.build.finalize_date_cache_db")
        mocker.patch("ddbj_search_converter.date_cache.build.insert_bp_dates", return_value=0)
        mocker.patch("ddbj_search_converter.date_cache.build.insert_bs_dates", return_value=0)
        mock_fetch_bp = mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_all_bp_dates",
            return_value=iter([]),
        )
        mock_fetch_bs = mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_all_bs_dates",
            return_value=iter([]),
        )

        build_date_cache(config)

        mock_fetch_bp.assert_called_once_with("postgresql://u:p@h:5432")
        mock_fetch_bs.assert_called_once_with("postgresql://u:p@h:5432")

    def test_insert_receives_collected_rows(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """fetch の結果が list 化されて insert に渡される。"""
        config = Config(
            result_dir=tmp_path,
            xsm_postgres_url="postgresql://u:p@h:5432",
        )

        mocker.patch("ddbj_search_converter.date_cache.build.log_info")
        mocker.patch("ddbj_search_converter.date_cache.build.init_date_cache_db")
        mocker.patch("ddbj_search_converter.date_cache.build.finalize_date_cache_db")

        bp_rows = [
            ("PRJDB1", "2026-01-01T00:00:00Z", None, None),
            ("PRJDB2", None, None, None),
        ]
        bs_rows = [("SAMD1", None, None, None)]
        mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_all_bp_dates",
            return_value=iter(bp_rows),
        )
        mocker.patch(
            "ddbj_search_converter.date_cache.build._fetch_all_bs_dates",
            return_value=iter(bs_rows),
        )
        mock_insert_bp = mocker.patch(
            "ddbj_search_converter.date_cache.build.insert_bp_dates",
            return_value=len(bp_rows),
        )
        mock_insert_bs = mocker.patch(
            "ddbj_search_converter.date_cache.build.insert_bs_dates",
            return_value=len(bs_rows),
        )

        build_date_cache(config)

        # 第二引数に bp_rows がそのまま渡される (list 化済)
        bp_call = mock_insert_bp.call_args
        assert bp_call.args[0] is config
        assert bp_call.args[1] == bp_rows
        bs_call = mock_insert_bs.call_args
        assert bs_call.args[0] is config
        assert bs_call.args[1] == bs_rows
