"""Tests for ddbj_search_converter.es.bulk_delete module."""

from __future__ import annotations

from typing import Any

from pytest_mock import MockerFixture

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.bulk_delete import (
    BulkDeleteResult,
    bulk_delete_by_ids,
    generate_delete_actions,
)


class TestGenerateDeleteActions:
    """generate_delete_actions: bulk API 用 action dict を yield する。"""

    def test_action_structure(self) -> None:
        """各 action は _op_type=delete / _index / _id の 3 キーを持つ。"""
        actions = list(generate_delete_actions({"PRJDB1"}, "bioproject"))

        assert len(actions) == 1
        assert actions[0] == {"_op_type": "delete", "_index": "bioproject", "_id": "PRJDB1"}

    def test_yield_count_matches_input(self) -> None:
        """入力 set の要素数と yield 数が一致する (set 順は問わず ID 集合で比較)。"""
        ids = {"PRJDB1", "PRJDB2", "PRJDB3"}
        actions = list(generate_delete_actions(ids, "bioproject"))

        assert len(actions) == 3
        assert {a["_id"] for a in actions} == ids
        assert all(a["_op_type"] == "delete" for a in actions)
        assert all(a["_index"] == "bioproject" for a in actions)

    def test_empty_set_yields_nothing(self) -> None:
        actions = list(generate_delete_actions(set(), "bioproject"))
        assert actions == []


def _config(tmp_path: Any) -> Config:
    return Config(result_dir=tmp_path, es_url="http://es:9200")


def _stub_es_client(mocker: MockerFixture, *, index_exists: bool = True) -> Any:
    """get_es_client を stub し、indices.exists の戻り値を制御する。"""
    es = mocker.MagicMock()
    es.indices.exists.return_value = index_exists
    mocker.patch("ddbj_search_converter.es.bulk_delete.get_es_client", return_value=es)
    return es


class TestBulkDeleteByIdsEmptyOrMissing:
    """空 set / index 不在の short-circuit パス。"""

    def test_empty_accessions_short_circuits(self, mocker: MockerFixture, tmp_path: Any) -> None:
        """空 set は ES 呼び出しなしで totals=0 の結果を返す。"""
        # get_es_client / helpers.bulk が呼ばれてはいけない
        mock_get_client = mocker.patch("ddbj_search_converter.es.bulk_delete.get_es_client")
        mock_helpers_bulk = mocker.patch("ddbj_search_converter.es.bulk_delete.helpers.bulk")

        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", set())

        assert result == BulkDeleteResult(
            index="bioproject",
            total_requested=0,
            success_count=0,
            not_found_count=0,
            error_count=0,
            errors=[],
        )
        mock_get_client.assert_not_called()
        mock_helpers_bulk.assert_not_called()

    def test_index_missing_counts_all_as_not_found(self, mocker: MockerFixture, tmp_path: Any) -> None:
        """index 不在時、全件 not_found としてカウントし helpers.bulk を呼ばない。"""
        _stub_es_client(mocker, index_exists=False)
        mock_helpers_bulk = mocker.patch("ddbj_search_converter.es.bulk_delete.helpers.bulk")
        # bulk_delete_by_ids は index 不在パスで log_info を呼ぶ。
        # logger の挙動は test_logger/test_db でカバー済なのでここでは no-op にする。
        mocker.patch("ddbj_search_converter.es.bulk_delete.log_info")

        accessions = {"PRJDB1", "PRJDB2"}
        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", accessions)

        assert result.total_requested == 2
        assert result.success_count == 0
        assert result.not_found_count == 2
        assert result.error_count == 0
        assert result.errors == []
        mock_helpers_bulk.assert_not_called()


class TestBulkDeleteByIdsHelpersBulkArgs:
    """helpers.bulk への引数固定 (chunk_size / raise_on_error / max_retries / request_timeout)。"""

    def test_default_batch_size_and_options(self, mocker: MockerFixture, tmp_path: Any) -> None:
        es = _stub_es_client(mocker, index_exists=True)
        mock_bulk = mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(1, []),
        )

        bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1"})

        kwargs = mock_bulk.call_args.kwargs
        assert kwargs["chunk_size"] == 1000  # default batch_size
        assert kwargs["stats_only"] is False
        assert kwargs["raise_on_error"] is False
        assert kwargs["max_retries"] == 3
        assert kwargs["request_timeout"] == 600
        # 第一引数は ES client
        assert mock_bulk.call_args.args[0] is es

    def test_custom_batch_size_propagates_to_chunk_size(self, mocker: MockerFixture, tmp_path: Any) -> None:
        _stub_es_client(mocker, index_exists=True)
        mock_bulk = mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(0, []),
        )

        bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1"}, batch_size=42)

        assert mock_bulk.call_args.kwargs["chunk_size"] == 42


class TestBulkDeleteByIdsAggregation:
    """成功 / 404 / その他エラーの集計ルール。"""

    def test_success_only(self, mocker: MockerFixture, tmp_path: Any) -> None:
        """全件成功時、success_count = ES からの success 値、エラーなし。"""
        _stub_es_client(mocker, index_exists=True)
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(3, []),
        )

        result = bulk_delete_by_ids(
            _config(tmp_path),
            "bioproject",
            {"PRJDB1", "PRJDB2", "PRJDB3"},
        )

        assert result.success_count == 3
        assert result.not_found_count == 0
        assert result.error_count == 0
        assert result.errors == []

    def test_404_counted_as_not_found_not_error(self, mocker: MockerFixture, tmp_path: Any) -> None:
        """failed の status=404 は not_found に振り分け、errors には入れない。"""
        _stub_es_client(mocker, index_exists=True)
        failed = [
            {"delete": {"_id": "PRJDB1", "status": 404, "result": "not_found"}},
            {"delete": {"_id": "PRJDB2", "status": 404, "result": "not_found"}},
        ]
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(0, failed),
        )

        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1", "PRJDB2"})

        assert result.success_count == 0
        assert result.not_found_count == 2
        assert result.error_count == 0
        assert result.errors == []

    def test_non_404_errors_are_collected(self, mocker: MockerFixture, tmp_path: Any) -> None:
        """404 以外の failed は errors に追加され、error_count に反映される。"""
        _stub_es_client(mocker, index_exists=True)
        failed = [
            {"delete": {"_id": "PRJDB1", "status": 404}},
            {"delete": {"_id": "PRJDB2", "status": 500, "error": "internal"}},
            {"delete": {"_id": "PRJDB3", "status": 503, "error": "unavailable"}},
        ]
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(1, failed),
        )

        result = bulk_delete_by_ids(
            _config(tmp_path),
            "bioproject",
            {"PRJDB1", "PRJDB2", "PRJDB3", "PRJDB4"},
        )

        assert result.success_count == 1
        assert result.not_found_count == 1
        assert result.error_count == 2
        # errors の中身は 500/503 のみ (404 は入らない)
        statuses = [e["delete"]["status"] for e in result.errors]
        assert sorted(statuses) == [500, 503]

    def test_errors_capped_at_100(self, mocker: MockerFixture, tmp_path: Any) -> None:
        """errors リストは最大 100 件で切られる (101 件投入 → 100 件保持)。"""
        _stub_es_client(mocker, index_exists=True)
        failed = [{"delete": {"_id": f"id_{i}", "status": 500, "error": f"e_{i}"}} for i in range(101)]
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(0, failed),
        )

        result = bulk_delete_by_ids(
            _config(tmp_path),
            "bioproject",
            {f"id_{i}" for i in range(101)},
        )

        # error_count は実数 (101)、errors のリストは 100 件で打ち切り
        assert result.error_count == 101
        assert len(result.errors) == 100

    def test_unexpected_failed_type_treated_as_no_errors(
        self, mocker: MockerFixture, tmp_path: Any
    ) -> None:
        """failed が list でないとき (stats_only モード等の予期しない戻り値) は errors 空。

        防御的分岐の挙動を固定する。
        """
        _stub_es_client(mocker, index_exists=True)
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(2, 5),  # failed が int (異常)
        )

        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1", "PRJDB2"})

        assert result.success_count == 2
        assert result.not_found_count == 0
        assert result.error_count == 0
        assert result.errors == []

    def test_failed_without_delete_key_collected_as_error(
        self, mocker: MockerFixture, tmp_path: Any
    ) -> None:
        """failed の各要素が `delete` キーを持たない場合も errors に入る (404 でないため)。

        防御的分岐 `err.get("delete", {})` の挙動を固定する。
        """
        _stub_es_client(mocker, index_exists=True)
        failed = [
            {"index": {"_id": "PRJDB1", "status": 500}},  # delete キーなし
        ]
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(0, failed),
        )

        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1"})

        assert result.error_count == 1
        assert result.not_found_count == 0


class TestBugBulkDeleteSerializability:
    """`bulk_delete` の `errors` 内に Exception 等の non-JSON-serializable 値が紛れても、
    `BulkDeleteResult.model_dump_json()` が成功することを保証する。

    bulk_insert では `_sanitize_error_info` で同種のバグを塞いでいるが、bulk_delete には
    対応する sanitize がなく、`elasticsearch.helpers.bulk` が `failed` の dict 値に
    `ApiError` を含めて返したケースで Pydantic シリアライズが死ぬ。
    """

    def test_errors_with_exception_value_serializable(
        self, mocker: MockerFixture, tmp_path: Any
    ) -> None:
        """failed の dict 値に Exception が混じっても model_dump_json() で落ちない。"""
        _stub_es_client(mocker, index_exists=True)

        class FakeApiError(Exception):
            def __init__(self) -> None:
                super().__init__("ApiError(500, 'internal')")

        failed = [
            {"delete": {"_id": "PRJDB1", "status": 500, "error": FakeApiError()}},
        ]
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(0, failed),
        )

        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1"})

        # 集計は通常通り (404 でないので errors に入る)
        assert result.error_count == 1
        # ここが fail していたバグの本丸: Pydantic で再シリアライズ可能でなくてはならない
        result.model_dump_json()

    def test_errors_with_top_level_exception_serializable(
        self, mocker: MockerFixture, tmp_path: Any
    ) -> None:
        """failed 自体が Exception を含む (dict 全体が non-dict) ケースも serializable。"""
        _stub_es_client(mocker, index_exists=True)

        class FakeApiError(Exception):
            def __init__(self) -> None:
                super().__init__("transport-level error")

        failed = [FakeApiError()]
        mocker.patch(
            "ddbj_search_converter.es.bulk_delete.helpers.bulk",
            return_value=(0, failed),
        )

        result = bulk_delete_by_ids(_config(tmp_path), "bioproject", {"PRJDB1"})

        assert result.error_count == 1
        result.model_dump_json()
