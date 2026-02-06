"""Elasticsearch settings configuration.

This module centralizes ES-related settings with documentation on their purpose
and the reasoning behind each value.
"""

from typing import Any, Dict

# === Index Settings ===
# These settings are applied when creating new indexes.
# Used in mappings/common.py for index creation.

INDEX_SETTINGS: Dict[str, Any] = {
    "index": {
        # Refresh interval: how often the index is refreshed to make new docs searchable.
        # Default: "1s" for near-real-time search.
        # During bulk inserts, temporarily set to "-1" to improve performance.
        "refresh_interval": "1s",

        # Nested objects limit: maximum number of nested objects per document.
        # Default ES value is 10000, but we need more for BioSample attributes
        # and other nested fields.
        "mapping.nested_objects.limit": 100000,

        # Number of primary shards: 1 is sufficient for our data volume.
        # For single-node deployment, more shards add unnecessary overhead.
        "number_of_shards": 1,

        # Number of replicas: 0 for development/single-node.
        # For production with multiple nodes, consider increasing to 1.
        "number_of_replicas": 0,
    }
}


# === Bulk Insert Settings ===
# Settings for elasticsearch.helpers.bulk() operations.

BULK_INSERT_SETTINGS: Dict[str, Any] = {
    # Number of documents per bulk request.
    # 5000 provides good throughput for large-scale data ingestion.
    # Reduce if memory pressure occurs with very large documents.
    "batch_size": 5000,

    # Maximum retries for failed bulk operations.
    "max_retries": 3,

    # Request timeout in seconds.
    # 600 seconds for large-scale bulk inserts with batch_size=5000.
    "request_timeout": 600,

    # Refresh interval during bulk insert.
    # "-1" disables automatic refresh for better performance.
    "bulk_refresh_interval": "-1",

    # Refresh interval after bulk insert.
    # "1s" restores normal near-real-time behavior.
    "normal_refresh_interval": "1s",
}


# === Snapshot Settings ===
# Default settings for snapshot operations.

SNAPSHOT_SETTINGS: Dict[str, Any] = {
    # Default repository name
    "default_repo_name": "backup",

    # Default snapshot name prefix (timestamp will be appended)
    "snapshot_name_prefix": "ddbj_search",

    # Whether to compress snapshots by default
    "compress": True,

    # Whether to include global cluster state in snapshots
    "include_global_state": False,
}


# === Health Check Thresholds ===
# Thresholds for monitoring and alerting.

HEALTH_CHECK_THRESHOLDS: Dict[str, Any] = {
    # Disk usage warning threshold (percentage)
    "disk_warning_percent": 80,

    # Disk usage critical threshold (percentage)
    "disk_critical_percent": 90,

    # JVM heap usage warning threshold (percentage)
    "heap_warning_percent": 75,

    # JVM heap usage critical threshold (percentage)
    "heap_critical_percent": 90,
}


def get_health_thresholds() -> Dict[str, Any]:
    """Get health check thresholds.

    Returns a copy to prevent accidental modification.
    """
    return HEALTH_CHECK_THRESHOLDS.copy()
