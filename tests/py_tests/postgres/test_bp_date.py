"""postgres.bp_date のテスト。"""

from pytest_mock import MockerFixture

from ddbj_search_converter.config import Config
from ddbj_search_converter.postgres.bp_date import (
    BP_DATES_BULK_QUERY,
    fetch_bp_dates_bulk,
)


class TestFetchBpDatesBulk:
    def test_empty_accessions_returns_empty_without_db_call(self, mocker: MockerFixture) -> None:
        """accession リストが空のとき DB 接続せず空 dict を返す。

        ``IN ()`` を生成するパスを構造的に塞ぐためのガードのリグレッション検出。
        旧実装は早期 return がなかったときに ``IN ()`` syntax error を起こしていた。
        """
        spy = mocker.patch("ddbj_search_converter.postgres.bp_date.postgres_connection")
        result = fetch_bp_dates_bulk(Config(), [])
        assert result == {}
        spy.assert_not_called()


class TestBpDatesBulkQueryShape:
    def test_query_uses_any_array_parameter(self) -> None:
        """SQL は ``= ANY(%s)`` で配列 1 個 bind に書き換わっている (旧 ``IN ({placeholders})`` 撤廃)."""
        assert "= ANY(%s)" in BP_DATES_BULK_QUERY
        assert "{placeholders}" not in BP_DATES_BULK_QUERY
