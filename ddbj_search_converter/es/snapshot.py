"""Elasticsearch snapshot management."""

from datetime import datetime
from typing import Any, cast

from ddbj_search_converter.config import LOCAL_TZ, Config
from ddbj_search_converter.es.client import get_es_client
from ddbj_search_converter.es.index import ALL_INDEXES
from ddbj_search_converter.es.settings import SNAPSHOT_SETTINGS


def register_repository(
    config: Config,
    repo_name: str,
    repo_path: str,
    compress: bool = True,
) -> dict[str, Any]:
    """Register a snapshot repository.

    Args:
        config: Configuration object
        repo_name: Name of the repository
        repo_path: Path to the repository (must match ES path.repo setting)
        compress: Whether to compress snapshots

    Returns:
        Response from ES
    """
    es_client = get_es_client(config)
    response = es_client.snapshot.create_repository(
        name=repo_name,
        body={
            "type": "fs",
            "settings": {
                "location": repo_path,
                "compress": compress,
            },
        },
    )
    return cast("dict[str, Any]", response.body)


def list_repositories(config: Config) -> list[dict[str, Any]]:
    """List all snapshot repositories.

    Returns:
        List of repository info dicts
    """
    es_client = get_es_client(config)
    response = es_client.snapshot.get_repository()
    repos = []
    for name, info in response.body.items():
        repos.append(
            {
                "name": name,
                "type": info.get("type"),
                "settings": info.get("settings", {}),
            }
        )
    return repos


def delete_repository(config: Config, repo_name: str) -> dict[str, Any]:
    """Delete a snapshot repository.

    Args:
        config: Configuration object
        repo_name: Name of the repository to delete

    Returns:
        Response from ES
    """
    es_client = get_es_client(config)
    response = es_client.snapshot.delete_repository(name=repo_name)
    return cast("dict[str, Any]", response.body)


def create_snapshot(
    config: Config,
    repo_name: str,
    snapshot_name: str | None = None,
    indexes: list[str] | None = None,
    include_global_state: bool = SNAPSHOT_SETTINGS["include_global_state"],
    wait_for_completion: bool = True,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Create a snapshot.

    Args:
        config: Configuration object
        repo_name: Name of the repository
        snapshot_name: Name of the snapshot (auto-generated if not provided)
        indexes: List of indexes to snapshot (all DDBJ indexes if not provided)
        include_global_state: Whether to include cluster state
        wait_for_completion: Whether to wait for snapshot to complete
        metadata: Optional metadata to attach to snapshot

    Returns:
        Snapshot info
    """
    es_client = get_es_client(config)

    if snapshot_name is None:
        timestamp = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
        snapshot_name = f"{SNAPSHOT_SETTINGS['snapshot_name_prefix']}_{timestamp}"

    if indexes is None:
        indexes = list(ALL_INDEXES)

    body: dict[str, Any] = {
        "indices": ",".join(indexes),
        "include_global_state": include_global_state,
    }

    if metadata:
        body["metadata"] = metadata

    response = es_client.snapshot.create(
        repository=repo_name,
        snapshot=snapshot_name,
        body=body,
        wait_for_completion=wait_for_completion,
    )

    return cast("dict[str, Any]", response.body)


def list_snapshots(
    config: Config,
    repo_name: str,
) -> list[dict[str, Any]]:
    """List snapshots in a repository.

    Args:
        config: Configuration object
        repo_name: Name of the repository

    Returns:
        List of snapshot info dicts
    """
    es_client = get_es_client(config)
    response = es_client.snapshot.get(repository=repo_name, snapshot="*")

    snapshots = [
        {
            "snapshot": snap.get("snapshot"),
            "state": snap.get("state"),
            "start_time": snap.get("start_time"),
            "end_time": snap.get("end_time"),
            "duration_in_millis": snap.get("duration_in_millis"),
            "indices": snap.get("indices", []),
            "shards": snap.get("shards", {}),
            "metadata": snap.get("metadata", {}),
        }
        for snap in response.body.get("snapshots", [])
    ]

    return snapshots


def get_snapshot(
    config: Config,
    repo_name: str,
    snapshot_name: str,
) -> dict[str, Any]:
    """Get details of a specific snapshot.

    Args:
        config: Configuration object
        repo_name: Name of the repository
        snapshot_name: Name of the snapshot

    Returns:
        Snapshot details
    """
    es_client = get_es_client(config)
    response = es_client.snapshot.get(repository=repo_name, snapshot=snapshot_name)
    snapshots = response.body.get("snapshots", [])
    if not snapshots:
        raise ValueError(f"Snapshot '{snapshot_name}' not found in repository '{repo_name}'")
    return cast("dict[str, Any]", snapshots[0])


def delete_snapshot(
    config: Config,
    repo_name: str,
    snapshot_name: str,
) -> dict[str, Any]:
    """Delete a snapshot.

    Args:
        config: Configuration object
        repo_name: Name of the repository
        snapshot_name: Name of the snapshot to delete

    Returns:
        Response from ES
    """
    es_client = get_es_client(config)
    response = es_client.snapshot.delete(repository=repo_name, snapshot=snapshot_name)
    return cast("dict[str, Any]", response.body)


def restore_snapshot(
    config: Config,
    repo_name: str,
    snapshot_name: str,
    indexes: list[str] | None = None,
    rename_pattern: str | None = None,
    rename_replacement: str | None = None,
    include_global_state: bool = False,
    wait_for_completion: bool = True,
) -> dict[str, Any]:
    """Restore a snapshot.

    Args:
        config: Configuration object
        repo_name: Name of the repository
        snapshot_name: Name of the snapshot to restore
        indexes: List of indexes to restore (all from snapshot if not provided)
        rename_pattern: Regex pattern for renaming indexes
        rename_replacement: Replacement string for renaming
        include_global_state: Whether to restore cluster state
        wait_for_completion: Whether to wait for restore to complete

    Returns:
        Restore info
    """
    es_client = get_es_client(config)

    body: dict[str, Any] = {
        "include_global_state": include_global_state,
    }

    if indexes:
        body["indices"] = ",".join(indexes)

    if rename_pattern and rename_replacement:
        body["rename_pattern"] = rename_pattern
        body["rename_replacement"] = rename_replacement

    response = es_client.snapshot.restore(
        repository=repo_name,
        snapshot=snapshot_name,
        body=body,
        wait_for_completion=wait_for_completion,
    )

    return cast("dict[str, Any]", response.body)


def export_index_settings(
    config: Config,
    indexes: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Export index settings and mappings for migration verification.

    Args:
        config: Configuration object
        indexes: List of indexes to export (all DDBJ indexes if not provided)

    Returns:
        Dict of index name -> {settings, mappings}
    """
    es_client = get_es_client(config)

    if indexes is None:
        indexes = list(ALL_INDEXES)

    result = {}
    for index in indexes:
        try:
            settings_resp = es_client.indices.get_settings(index=index)
            mappings_resp = es_client.indices.get_mapping(index=index)

            index_settings = settings_resp.body.get(index, {}).get("settings", {})
            index_mappings = mappings_resp.body.get(index, {}).get("mappings", {})

            result[index] = {
                "settings": index_settings,
                "mappings": index_mappings,
            }
        except Exception:
            result[index] = {"error": "Index not found or not accessible"}

    return result


def get_snapshot_status(
    config: Config,
    repo_name: str,
    snapshot_name: str | None = None,
) -> dict[str, Any]:
    """Get the status of a snapshot operation.

    Args:
        config: Configuration object
        repo_name: Name of the repository
        snapshot_name: Name of the snapshot (all snapshots if not provided)

    Returns:
        Snapshot status info
    """
    es_client = get_es_client(config)

    if snapshot_name:
        response = es_client.snapshot.status(repository=repo_name, snapshot=snapshot_name)
    else:
        response = es_client.snapshot.status(repository=repo_name)

    return cast("dict[str, Any]", response.body)
