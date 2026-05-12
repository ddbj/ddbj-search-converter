"""Unit tests for ddbj_search_converter.es.monitoring.

実 ES client を mock し、各 monitoring 関数が:
- 期待した ES API を呼ぶ
- response から正しいフィールドを抽出する
- ゼロ除算など防御的計算を行う
- Pydantic model に packaging される

を verify する。``Get_*`` 系は型契約 (BaseModel) を守って返すこと自体が大事。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ddbj_search_converter.es.monitoring import (
    ClusterHealth,
    HealthStatus,
    IndexStats,
    NodeStats,
    _parse_size,
    check_health,
    get_cluster_health,
    get_index_stats,
    get_node_stats,
)


@pytest.fixture
def patched_get_es():  # type: ignore[no-untyped-def]
    mock_es = MagicMock()
    with patch(
        "ddbj_search_converter.es.monitoring.get_es_client", return_value=mock_es
    ):
        yield mock_es


class TestGetClusterHealth:
    def test_returns_cluster_health_model(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cluster.health.return_value = {
            "status": "green",
            "cluster_name": "ddbj-search",
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "active_primary_shards": 14,
            "active_shards": 14,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
        }

        result = get_cluster_health(test_config)
        assert isinstance(result, ClusterHealth)
        assert result.status == "green"
        assert result.active_primary_shards == 14

    def test_yellow_status_passed_through(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cluster.health.return_value = {
            "status": "yellow", "cluster_name": "c", "number_of_nodes": 1,
            "number_of_data_nodes": 1, "active_primary_shards": 0,
            "active_shards": 0, "relocating_shards": 0,
            "initializing_shards": 0, "unassigned_shards": 1,
        }
        assert get_cluster_health(test_config).status == "yellow"


class TestGetNodeStats:
    def test_extracts_fs_and_jvm_metrics(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.nodes.stats.return_value = {
            "nodes": {
                "node1": {
                    "name": "n1",
                    "host": "10.0.0.1",
                    "fs": {"total": {"total_in_bytes": 1000, "free_in_bytes": 400}},
                    "jvm": {"mem": {"heap_used_in_bytes": 500, "heap_max_in_bytes": 1000, "heap_used_percent": 50}},
                },
            }
        }

        result = get_node_stats(test_config)
        assert len(result) == 1
        n = result[0]
        assert isinstance(n, NodeStats)
        assert n.name == "n1"
        # disk_used = (1000 - 400) / 1000 * 100 == 60.0
        assert n.disk_used_percent == 60.0
        assert n.heap_used_percent == 50

    def test_zero_total_disk_avoids_division_by_zero(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        """0 byte の fs を返す ES からは disk_used_percent=0 を返す (ZeroDivisionError 回避)。"""
        patched_get_es.nodes.stats.return_value = {
            "nodes": {
                "node1": {
                    "name": "n1", "host": "h",
                    "fs": {"total": {"total_in_bytes": 0, "free_in_bytes": 0}},
                    "jvm": {"mem": {"heap_used_in_bytes": 0, "heap_max_in_bytes": 0, "heap_used_percent": 0}},
                },
            }
        }
        result = get_node_stats(test_config)
        assert result[0].disk_used_percent == 0.0

    def test_missing_node_metadata_falls_back_to_id(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.nodes.stats.return_value = {
            "nodes": {
                "abc-node-id": {
                    # name missing
                    "fs": {"total": {"total_in_bytes": 100, "free_in_bytes": 50}},
                    "jvm": {"mem": {}},
                }
            }
        }
        result = get_node_stats(test_config)
        assert result[0].name == "abc-node-id"
        assert result[0].host == "unknown"


class TestParseSize:
    """``_parse_size`` の human-readable size parser。"""

    @pytest.mark.parametrize(
        ("input_str", "expected"),
        [
            ("100b", 100),
            ("1kb", 1024),
            ("1mb", 1024**2),
            ("1gb", 1024**3),
            ("1tb", 1024**4),
            ("1.5mb", int(1.5 * 1024**2)),
            ("0", 0),
            ("", 0),
            ("invalid", 0),
            ("not a size mb", 0),
        ],
    )
    def test_parse_variants(self, input_str: str, expected: int) -> None:
        assert _parse_size(input_str) == expected


class TestBugParseSizeSuffixOrder:
    """旧 ``_parse_size`` は dict iteration 順で ``b`` を ``kb``/``mb``/``gb``/``tb`` より
    先に match させ、 ``"1kb"`` を 0 と返していた。longest-suffix 優先に修正。

    回帰防止のため最小ケースを ``TestBug`` クラスとして残す。
    """

    @pytest.mark.parametrize(
        ("input_str", "expected"),
        [
            ("1kb", 1024),
            ("1mb", 1024 * 1024),
            ("1gb", 1024**3),
            ("1tb", 1024**4),
            # 大文字小文字混在も lowercase 化されて通る
            ("1KB", 1024),
            ("1Gb", 1024**3),
        ],
    )
    def test_multi_letter_suffix_takes_precedence_over_b(
        self, input_str: str, expected: int
    ) -> None:
        assert _parse_size(input_str) == expected


class TestGetIndexStats:
    def test_returns_list_of_index_stats(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cat.indices.return_value = [
            {
                "index": "bioproject",
                "docs.count": "100",
                "store.size": "5mb",
                "pri": "1",
                "rep": "0",
            },
            {
                "index": "biosample",
                "docs.count": "200",
                "store.size": "10mb",
                "pri": "1",
                "rep": "0",
            },
        ]
        result = get_index_stats(test_config)
        assert len(result) == 2
        names = {s.name for s in result}
        assert names == {"bioproject", "biosample"}
        for s in result:
            assert isinstance(s, IndexStats)

    def test_returns_empty_on_error(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cat.indices.side_effect = RuntimeError("ES down")
        assert get_index_stats(test_config) == []

    def test_empty_response(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cat.indices.return_value = []
        assert get_index_stats(test_config) == []


class TestCheckHealth:
    """check_health は cluster_health / node_stats / 警告閾値を集約して
    HealthStatus list を返す。"""

    def test_returns_list_of_health_status(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cluster.health.return_value = {
            "status": "green", "cluster_name": "c",
            "number_of_nodes": 1, "number_of_data_nodes": 1,
            "active_primary_shards": 14, "active_shards": 14,
            "relocating_shards": 0, "initializing_shards": 0,
            "unassigned_shards": 0,
        }
        patched_get_es.nodes.stats.return_value = {
            "nodes": {
                "n1": {
                    "name": "n1", "host": "h",
                    "fs": {"total": {"total_in_bytes": 100, "free_in_bytes": 80}},  # 20% used
                    "jvm": {"mem": {"heap_used_in_bytes": 100, "heap_max_in_bytes": 1000, "heap_used_percent": 10}},
                }
            }
        }
        result = check_health(test_config)
        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, HealthStatus)
            assert item.level in {"ok", "warning", "critical"}

    def test_critical_status_when_cluster_red(
        self, patched_get_es: MagicMock, test_config: MagicMock
    ) -> None:
        patched_get_es.cluster.health.return_value = {
            "status": "red", "cluster_name": "c",
            "number_of_nodes": 1, "number_of_data_nodes": 1,
            "active_primary_shards": 0, "active_shards": 0,
            "relocating_shards": 0, "initializing_shards": 0,
            "unassigned_shards": 14,
        }
        patched_get_es.nodes.stats.return_value = {
            "nodes": {
                "n1": {
                    "name": "n1", "host": "h",
                    "fs": {"total": {"total_in_bytes": 100, "free_in_bytes": 80}},
                    "jvm": {"mem": {"heap_used_in_bytes": 0, "heap_max_in_bytes": 1000, "heap_used_percent": 0}},
                }
            }
        }
        result = check_health(test_config)
        critical = [r for r in result if r.level == "critical"]
        assert critical, f"expected at least one critical entry, got: {result}"
