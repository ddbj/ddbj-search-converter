"""Unit tests for ddbj_search_converter.es.snapshot.

実 ES client を mock し、各 snapshot 操作が:
- 期待した ES API を期待した引数で呼ぶ
- ES からの response を正しく整形して返す
- ES がエラーを返した場合に防御的に動作する

を verify する。``cast("dict[str, Any]", response.body)`` で外部境界の body を
そのまま受け取る形なので、response.body 構造の互換性を pin することが目的。
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ddbj_search_converter.es.snapshot import (
    create_snapshot,
    delete_repository,
    delete_snapshot,
    export_index_settings,
    get_snapshot,
    get_snapshot_status,
    list_repositories,
    list_snapshots,
    register_repository,
    restore_snapshot,
)


def _make_mock_response(body: Any) -> MagicMock:
    """elasticsearch.Elasticsearch の戻り値を模す。"""
    resp = MagicMock()
    resp.body = body
    return resp


@pytest.fixture
def mock_es_client() -> MagicMock:
    """get_es_client をパッチで差し替えて返す client mock。"""
    return MagicMock()


@pytest.fixture
def patched_get_es(mock_es_client: MagicMock):  # type: ignore[no-untyped-def]
    """``get_es_client`` を mock_es_client に固定する patcher を yield する。"""
    with patch(
        "ddbj_search_converter.es.snapshot.get_es_client", return_value=mock_es_client
    ):
        yield mock_es_client


class TestRegisterRepository:
    def test_calls_create_repository_with_fs_body(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.create_repository.return_value = _make_mock_response(
            {"acknowledged": True}
        )

        result = register_repository(test_config, "backup", "/var/backups", compress=True)

        assert result == {"acknowledged": True}
        patched_get_es.snapshot.create_repository.assert_called_once_with(
            name="backup",
            body={
                "type": "fs",
                "settings": {
                    "location": "/var/backups",
                    "compress": True,
                },
            },
        )

    def test_compress_false_propagates(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.create_repository.return_value = _make_mock_response({})
        register_repository(test_config, "r", "/x", compress=False)
        body = patched_get_es.snapshot.create_repository.call_args.kwargs["body"]
        assert body["settings"]["compress"] is False


class TestListRepositories:
    def test_flattens_response_into_list_of_dicts(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.get_repository.return_value = _make_mock_response(
            {
                "backup": {"type": "fs", "settings": {"location": "/x"}},
                "archive": {"type": "fs", "settings": {"location": "/y"}},
            }
        )

        result = list_repositories(test_config)
        names = {r["name"] for r in result}
        assert names == {"backup", "archive"}
        for r in result:
            assert r["type"] == "fs"
            assert "settings" in r

    def test_empty_response(self, patched_get_es: MagicMock, test_config: MagicMock) -> None:
        patched_get_es.snapshot.get_repository.return_value = _make_mock_response({})
        assert list_repositories(test_config) == []


class TestDeleteRepository:
    def test_calls_delete_repository(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.delete_repository.return_value = _make_mock_response(
            {"acknowledged": True}
        )
        result = delete_repository(test_config, "backup")
        assert result == {"acknowledged": True}
        patched_get_es.snapshot.delete_repository.assert_called_once_with(name="backup")


class TestCreateSnapshot:
    def test_default_indexes_are_all_indexes(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        from ddbj_search_converter.es.index import ALL_INDEXES

        patched_get_es.snapshot.create.return_value = _make_mock_response({"accepted": True})
        create_snapshot(test_config, "repo", snapshot_name="snap1")
        body = patched_get_es.snapshot.create.call_args.kwargs["body"]
        # ALL_INDEXES 全て含む
        for idx in ALL_INDEXES:
            assert idx in body["indices"]

    def test_explicit_indexes_only(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.create.return_value = _make_mock_response({})
        create_snapshot(
            test_config, "repo", snapshot_name="s", indexes=["bioproject", "biosample"]
        )
        body = patched_get_es.snapshot.create.call_args.kwargs["body"]
        assert body["indices"] == "bioproject,biosample"

    def test_metadata_attached_when_provided(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.create.return_value = _make_mock_response({})
        create_snapshot(
            test_config, "repo", snapshot_name="s",
            indexes=["bioproject"], metadata={"k": "v"},
        )
        body = patched_get_es.snapshot.create.call_args.kwargs["body"]
        assert body["metadata"] == {"k": "v"}

    def test_metadata_omitted_by_default(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.create.return_value = _make_mock_response({})
        create_snapshot(test_config, "repo", snapshot_name="s", indexes=["bioproject"])
        body = patched_get_es.snapshot.create.call_args.kwargs["body"]
        assert "metadata" not in body

    def test_wait_for_completion_default_true(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.create.return_value = _make_mock_response({})
        create_snapshot(test_config, "repo", snapshot_name="s", indexes=["bioproject"])
        assert patched_get_es.snapshot.create.call_args.kwargs["wait_for_completion"] is True

    def test_auto_generated_snapshot_name_prefix(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        from ddbj_search_converter.es.settings import SNAPSHOT_SETTINGS

        patched_get_es.snapshot.create.return_value = _make_mock_response({})
        create_snapshot(test_config, "repo", snapshot_name=None, indexes=["bioproject"])
        snapshot_name = patched_get_es.snapshot.create.call_args.kwargs["snapshot"]
        assert snapshot_name.startswith(SNAPSHOT_SETTINGS["snapshot_name_prefix"])


class TestListSnapshots:
    def test_extracts_relevant_fields(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.get.return_value = _make_mock_response(
            {
                "snapshots": [
                    {
                        "snapshot": "s1",
                        "state": "SUCCESS",
                        "start_time": "2026-05-12T10:00:00Z",
                        "end_time": "2026-05-12T10:05:00Z",
                        "duration_in_millis": 300_000,
                        "indices": ["bioproject"],
                        "shards": {"successful": 1, "failed": 0},
                        "metadata": {},
                    }
                ]
            }
        )
        result = list_snapshots(test_config, "repo")
        assert len(result) == 1
        assert result[0]["snapshot"] == "s1"
        assert result[0]["state"] == "SUCCESS"

    def test_empty_repository(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.get.return_value = _make_mock_response({"snapshots": []})
        assert list_snapshots(test_config, "repo") == []


class TestGetSnapshot:
    def test_returns_first_snapshot(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.get.return_value = _make_mock_response(
            {"snapshots": [{"snapshot": "s1", "state": "SUCCESS"}]}
        )
        result = get_snapshot(test_config, "repo", "s1")
        assert result["snapshot"] == "s1"

    def test_raises_when_not_found(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.get.return_value = _make_mock_response({"snapshots": []})
        with pytest.raises(ValueError, match="not found"):
            get_snapshot(test_config, "repo", "s1")


class TestDeleteSnapshot:
    def test_calls_delete(self, patched_get_es: MagicMock, test_config: MagicMock) -> None:
        patched_get_es.snapshot.delete.return_value = _make_mock_response({"acknowledged": True})
        delete_snapshot(test_config, "repo", "s1")
        patched_get_es.snapshot.delete.assert_called_once_with(repository="repo", snapshot="s1")


class TestRestoreSnapshot:
    def test_body_includes_indices_when_given(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.restore.return_value = _make_mock_response({})
        restore_snapshot(test_config, "repo", "s1", indexes=["bioproject"])
        body = patched_get_es.snapshot.restore.call_args.kwargs["body"]
        assert body["indices"] == "bioproject"
        assert body["include_global_state"] is False

    def test_rename_pattern_only_when_both_provided(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.restore.return_value = _make_mock_response({})
        restore_snapshot(
            test_config, "repo", "s1",
            rename_pattern="bioproject", rename_replacement="bioproject-restored",
        )
        body = patched_get_es.snapshot.restore.call_args.kwargs["body"]
        assert body["rename_pattern"] == "bioproject"
        assert body["rename_replacement"] == "bioproject-restored"

    def test_rename_pattern_alone_is_ignored(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        """rename_pattern だけで rename_replacement が無い場合は body に乗らない (ES が
        rejection を返す前に防御する)。"""
        patched_get_es.snapshot.restore.return_value = _make_mock_response({})
        restore_snapshot(test_config, "repo", "s1", rename_pattern="x")
        body = patched_get_es.snapshot.restore.call_args.kwargs["body"]
        assert "rename_pattern" not in body
        assert "rename_replacement" not in body


class TestExportIndexSettings:
    def test_collects_settings_and_mappings_per_index(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.indices.get_settings.return_value = _make_mock_response(
            {"bioproject": {"settings": {"index": {"number_of_shards": "1"}}}}
        )
        patched_get_es.indices.get_mapping.return_value = _make_mock_response(
            {"bioproject": {"mappings": {"properties": {"identifier": {"type": "keyword"}}}}}
        )

        result = export_index_settings(test_config, indexes=["bioproject"])
        assert "settings" in result["bioproject"]
        assert "mappings" in result["bioproject"]
        assert result["bioproject"]["settings"]["index"]["number_of_shards"] == "1"

    def test_records_error_for_missing_index(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.indices.get_settings.side_effect = RuntimeError("boom")

        result = export_index_settings(test_config, indexes=["nonexistent"])
        assert "error" in result["nonexistent"]


class TestGetSnapshotStatus:
    def test_with_specific_snapshot(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.status.return_value = _make_mock_response({"snapshots": []})
        get_snapshot_status(test_config, "repo", "s1")
        patched_get_es.snapshot.status.assert_called_once_with(repository="repo", snapshot="s1")

    def test_without_snapshot_uses_repo_level(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.snapshot.status.return_value = _make_mock_response({"snapshots": []})
        get_snapshot_status(test_config, "repo", None)
        patched_get_es.snapshot.status.assert_called_once_with(repository="repo")
