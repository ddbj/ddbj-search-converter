"""Elasticsearch monitoring utilities."""

from typing import List

from pydantic import BaseModel

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import get_es_client
from ddbj_search_converter.es.settings import get_health_thresholds


class HealthStatus(BaseModel):
    """Health status information."""
    level: str  # "ok", "warning", "critical"
    message: str


class ClusterHealth(BaseModel):
    """Cluster health information."""
    status: str  # "green", "yellow", "red"
    cluster_name: str
    number_of_nodes: int
    number_of_data_nodes: int
    active_primary_shards: int
    active_shards: int
    relocating_shards: int
    initializing_shards: int
    unassigned_shards: int


class NodeStats(BaseModel):
    """Node statistics."""
    name: str
    host: str
    disk_total_bytes: int
    disk_free_bytes: int
    disk_used_percent: float
    heap_used_bytes: int
    heap_max_bytes: int
    heap_used_percent: float


class IndexStats(BaseModel):
    """Index statistics."""
    name: str
    docs_count: int
    store_size_bytes: int
    primary_shards: int
    replica_shards: int


def get_cluster_health(config: Config) -> ClusterHealth:
    """Get cluster health information.

    Args:
        config: Configuration object

    Returns:
        ClusterHealth dataclass
    """
    es_client = get_es_client(config)
    health = es_client.cluster.health()

    return ClusterHealth(
        status=health["status"],
        cluster_name=health["cluster_name"],
        number_of_nodes=health["number_of_nodes"],
        number_of_data_nodes=health["number_of_data_nodes"],
        active_primary_shards=health["active_primary_shards"],
        active_shards=health["active_shards"],
        relocating_shards=health["relocating_shards"],
        initializing_shards=health["initializing_shards"],
        unassigned_shards=health["unassigned_shards"],
    )


def get_node_stats(config: Config) -> List[NodeStats]:
    """Get node statistics.

    Args:
        config: Configuration object

    Returns:
        List of NodeStats dataclasses
    """
    es_client = get_es_client(config)
    stats = es_client.nodes.stats(metric=["fs", "jvm"])

    nodes = []
    for node_id, node_data in stats["nodes"].items():
        fs_data = node_data.get("fs", {}).get("total", {})
        jvm_data = node_data.get("jvm", {}).get("mem", {})

        disk_total = fs_data.get("total_in_bytes", 0)
        disk_free = fs_data.get("free_in_bytes", 0)
        disk_used_percent = 0.0
        if disk_total > 0:
            disk_used_percent = ((disk_total - disk_free) / disk_total) * 100

        heap_used = jvm_data.get("heap_used_in_bytes", 0)
        heap_max = jvm_data.get("heap_max_in_bytes", 0)
        heap_used_percent = jvm_data.get("heap_used_percent", 0)

        nodes.append(NodeStats(
            name=node_data.get("name", node_id),
            host=node_data.get("host", "unknown"),
            disk_total_bytes=disk_total,
            disk_free_bytes=disk_free,
            disk_used_percent=disk_used_percent,
            heap_used_bytes=heap_used,
            heap_max_bytes=heap_max,
            heap_used_percent=heap_used_percent,
        ))

    return nodes


def get_index_stats(config: Config) -> List[IndexStats]:
    """Get statistics for all indexes.

    Args:
        config: Configuration object

    Returns:
        List of IndexStats dataclasses
    """
    es_client = get_es_client(config)

    try:
        response = es_client.cat.indices(format="json")
        indices = list(response) if response else []
    except Exception:
        return []

    stats: List[IndexStats] = []
    for idx in indices:
        if not isinstance(idx, dict):
            continue
        stats.append(IndexStats(
            name=str(idx.get("index", "unknown")),
            docs_count=int(idx.get("docs.count", 0) or 0),
            store_size_bytes=_parse_size(str(idx.get("store.size", "0"))),
            primary_shards=int(idx.get("pri", 0) or 0),
            replica_shards=int(idx.get("rep", 0) or 0),
        ))

    return stats


def _parse_size(size_str: str) -> int:
    """Parse a size string like '10mb' or '1gb' to bytes."""
    if not size_str:
        return 0

    size_str = size_str.lower().strip()

    multipliers = {
        "b": 1,
        "kb": 1024,
        "mb": 1024 ** 2,
        "gb": 1024 ** 3,
        "tb": 1024 ** 4,
    }

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                value = float(size_str[:-len(suffix)])
                return int(value * multiplier)
            except ValueError:
                return 0

    try:
        return int(float(size_str))
    except ValueError:
        return 0


def check_health(config: Config) -> List[HealthStatus]:
    """Run health checks and return any warnings or issues.

    Args:
        config: Configuration object

    Returns:
        List of HealthStatus objects (empty if all OK)
    """
    thresholds = get_health_thresholds()
    issues: List[HealthStatus] = []

    # Check cluster health
    try:
        cluster = get_cluster_health(config)
        if cluster.status == "red":
            issues.append(HealthStatus(
                level="critical",
                message="Cluster status is RED - some primary shards are unassigned"
            ))
        elif cluster.status == "yellow":
            issues.append(HealthStatus(
                level="warning",
                message="Cluster status is YELLOW - some replica shards are unassigned"
            ))

        if cluster.unassigned_shards > 0:
            issues.append(HealthStatus(
                level="warning",
                message=f"{cluster.unassigned_shards} unassigned shards"
            ))
    except Exception as e:
        issues.append(HealthStatus(
            level="critical",
            message=f"Failed to get cluster health: {e}"
        ))
        return issues

    # Check node stats
    try:
        nodes = get_node_stats(config)
        for node in nodes:
            # Disk usage
            if node.disk_used_percent >= thresholds["disk_critical_percent"]:
                issues.append(HealthStatus(
                    level="critical",
                    message=f"Node '{node.name}' disk usage is {node.disk_used_percent:.1f}% (critical threshold: {thresholds['disk_critical_percent']}%)"
                ))
            elif node.disk_used_percent >= thresholds["disk_warning_percent"]:
                issues.append(HealthStatus(
                    level="warning",
                    message=f"Node '{node.name}' disk usage is {node.disk_used_percent:.1f}% (warning threshold: {thresholds['disk_warning_percent']}%)"
                ))

            # JVM heap
            if node.heap_used_percent >= thresholds["heap_critical_percent"]:
                issues.append(HealthStatus(
                    level="critical",
                    message=f"Node '{node.name}' JVM heap usage is {node.heap_used_percent:.1f}% (critical threshold: {thresholds['heap_critical_percent']}%)"
                ))
            elif node.heap_used_percent >= thresholds["heap_warning_percent"]:
                issues.append(HealthStatus(
                    level="warning",
                    message=f"Node '{node.name}' JVM heap usage is {node.heap_used_percent:.1f}% (warning threshold: {thresholds['heap_warning_percent']}%)"
                ))
    except Exception as e:
        issues.append(HealthStatus(
            level="warning",
            message=f"Failed to get node stats: {e}"
        ))

    return issues


def format_bytes(bytes_val: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(bytes_val) < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val = int(bytes_val / 1024)
    return f"{bytes_val:.1f} PB"
