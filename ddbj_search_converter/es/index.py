"""Elasticsearch index creation and deletion."""

import contextlib
from typing import Any, Literal, cast

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import check_index_exists, get_es_client, resolve_alias_to_indexes
from ddbj_search_converter.es.mappings.bioproject import get_bioproject_mapping
from ddbj_search_converter.es.mappings.biosample import get_biosample_mapping
from ddbj_search_converter.es.mappings.jga import JGA_INDEXES, JgaIndexType, get_jga_mapping
from ddbj_search_converter.es.mappings.sra import SRA_INDEXES, SraIndexType, get_sra_mapping

IndexName = Literal[
    "bioproject",
    "biosample",
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
]

ALL_INDEXES: list[IndexName] = [
    "bioproject",
    "biosample",
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
]

IndexGroup = Literal["bioproject", "biosample", "sra", "jga", "all"]

ALIASES: dict[str, list[IndexName]] = {
    "sra": cast("list[IndexName]", list(SRA_INDEXES)),
    "jga": cast("list[IndexName]", list(JGA_INDEXES)),
    "entries": list(ALL_INDEXES),
}


def get_indexes_for_group(group: IndexGroup) -> list[IndexName]:
    """Get the list of index names for a given group."""
    if group == "all":
        return ALL_INDEXES.copy()
    if group == "sra":
        return cast("list[IndexName]", list(SRA_INDEXES))
    if group == "jga":
        return cast("list[IndexName]", list(JGA_INDEXES))
    return [group]


def get_mapping_for_index(index: IndexName) -> dict[str, Any]:
    """Get the mapping for a specific index."""
    if index == "bioproject":
        return get_bioproject_mapping()
    if index == "biosample":
        return get_biosample_mapping()
    if index.startswith("sra-"):
        return get_sra_mapping(cast("SraIndexType", index))
    if index.startswith("jga-"):
        return get_jga_mapping(cast("JgaIndexType", index))
    raise ValueError(f"Unknown index: {index}")


def create_index(
    config: Config,
    index: IndexName | IndexGroup,
    skip_existing: bool = False,
) -> list[str]:
    """Create an Elasticsearch index.

    Args:
        config: Configuration object
        index: Index name or group ("sra", "jga", "all")
        skip_existing: If True, skip indexes that already exist instead of raising an error

    Returns:
        List of created index names

    Raises:
        Exception: If an index already exists and skip_existing is False
    """
    es_client = get_es_client(config)
    created: list[str] = []

    indexes = get_indexes_for_group(cast("IndexGroup", index))

    for idx in indexes:
        if check_index_exists(es_client, idx):
            if skip_existing:
                continue
            raise Exception(f"Index '{idx}' already exists.")

        mapping = get_mapping_for_index(idx)
        es_client.indices.create(index=idx, body=mapping)
        created.append(idx)

    for alias_name, alias_indexes in ALIASES.items():
        for created_idx in created:
            if created_idx in alias_indexes:
                es_client.indices.put_alias(index=created_idx, name=alias_name)

    return created


def delete_index(
    config: Config,
    index: IndexName | IndexGroup,
    skip_missing: bool = False,
) -> list[str]:
    """Delete an Elasticsearch index.

    Args:
        config: Configuration object
        index: Index name or group ("sra", "jga", "all")
        skip_missing: If True, skip indexes that don't exist instead of raising an error

    Returns:
        List of deleted index names

    Raises:
        Exception: If an index doesn't exist and skip_missing is False
    """
    es_client = get_es_client(config)
    deleted: list[str] = []

    indexes = get_indexes_for_group(cast("IndexGroup", index))

    for idx in indexes:
        if not check_index_exists(es_client, idx):
            if skip_missing:
                continue
            raise Exception(f"Index '{idx}' does not exist.")

        for alias_name, alias_indexes in ALIASES.items():
            if idx in alias_indexes:
                with contextlib.suppress(Exception):
                    es_client.indices.delete_alias(index=idx, name=alias_name)
        es_client.indices.delete(index=idx)
        deleted.append(idx)

    return deleted


def setup_aliases(config: Config) -> list[str]:
    """Set up aliases for all existing indexes.

    Useful for adding aliases to indexes that were created before
    alias support was added.

    Returns:
        List of alias names that were set up
    """
    es_client = get_es_client(config)
    setup: list[str] = []

    for alias_name, alias_indexes in ALIASES.items():
        for idx in alias_indexes:
            if check_index_exists(es_client, idx):
                es_client.indices.put_alias(index=idx, name=alias_name)
        setup.append(alias_name)

    return setup


def list_indexes(config: Config) -> list[dict[str, Any]]:
    """List all DDBJ Search indexes with their doc counts.

    Returns:
        List of dicts containing index name, doc_count, and physical_index (if alias)
    """
    es_client = get_es_client(config)
    result: list[dict[str, Any]] = []

    for idx in ALL_INDEXES:
        if check_index_exists(es_client, idx):
            stats = es_client.indices.stats(index=idx)
            doc_count = stats["_all"]["primaries"]["docs"]["count"]
            physical_indexes = resolve_alias_to_indexes(es_client, idx)
            result.append(
                {
                    "index": idx,
                    "doc_count": doc_count,
                    "exists": True,
                    "physical_indexes": physical_indexes,
                }
            )
        else:
            result.append(
                {
                    "index": idx,
                    "doc_count": 0,
                    "exists": False,
                    "physical_indexes": [],
                }
            )

    return result


# === Blue-Green Alias Swap ===


def make_physical_index_name(index: IndexName, date_suffix: str) -> str:
    """Build a dated physical index name.

    >>> make_physical_index_name("bioproject", "20260413")
    'bioproject-20260413'
    """
    return f"{index}-{date_suffix}"


def create_index_with_suffix(
    config: Config,
    index: IndexName | IndexGroup,
    date_suffix: str,
    skip_existing: bool = False,
) -> list[str]:
    """Create dated physical indexes without attaching any aliases.

    Used for the Blue-Green full update flow: data is inserted into the new
    indexes while the old ones continue to serve queries.

    Args:
        config: Configuration object
        index: Index name or group
        date_suffix: Date suffix in YYYYMMDD format
        skip_existing: If True, skip indexes that already exist

    Returns:
        List of created physical index names
    """
    es_client = get_es_client(config)
    created: list[str] = []

    indexes = get_indexes_for_group(cast("IndexGroup", index))

    for idx in indexes:
        physical_name = make_physical_index_name(idx, date_suffix)
        if check_index_exists(es_client, physical_name):
            if skip_existing:
                continue
            raise Exception(f"Index '{physical_name}' already exists.")

        mapping = get_mapping_for_index(idx)
        es_client.indices.create(index=physical_name, body=mapping)
        created.append(physical_name)

    return created


def swap_aliases(
    config: Config,
    date_suffix: str,
) -> dict[str, str]:
    """Atomically swap all aliases to new dated indexes.

    Builds a single ``_aliases`` API request that removes aliases from old
    indexes and adds them to new dated indexes.

    Args:
        config: Configuration object
        date_suffix: Date suffix of the new indexes (YYYYMMDD)

    Returns:
        Mapping of logical index name to old physical index name (for cleanup).
        Empty dict if no old indexes were found (first-time setup).

    Raises:
        Exception: If a new physical index does not exist
    """
    es_client = get_es_client(config)

    actions: list[dict[str, Any]] = []
    old_indexes: dict[str, str] = {}

    for idx in ALL_INDEXES:
        new_physical = make_physical_index_name(idx, date_suffix)
        if not check_index_exists(es_client, new_physical):
            raise Exception(f"New index '{new_physical}' does not exist. Create it first.")

        # Find old physical index behind the per-index alias
        old_physicals = resolve_alias_to_indexes(es_client, idx)
        for old_physical in old_physicals:
            if old_physical != new_physical:
                old_indexes[idx] = old_physical
                # Remove per-index alias from old
                actions.append({"remove": {"index": old_physical, "alias": idx}})
                # Remove group aliases from old
                for alias_name, alias_indexes in ALIASES.items():
                    if idx in alias_indexes:
                        actions.append({"remove": {"index": old_physical, "alias": alias_name}})

        # Add per-index alias to new
        actions.append({"add": {"index": new_physical, "alias": idx}})
        # Add group aliases to new
        for alias_name, alias_indexes in ALIASES.items():
            if idx in alias_indexes:
                actions.append({"add": {"index": new_physical, "alias": alias_name}})

    if actions:
        es_client.indices.update_aliases(body={"actions": actions})

    return old_indexes


def delete_physical_indexes(
    config: Config,
    index_names: list[str],
) -> list[str]:
    """Delete specified physical indexes.

    Args:
        config: Configuration object
        index_names: List of physical index names to delete

    Returns:
        List of deleted index names
    """
    es_client = get_es_client(config)
    deleted: list[str] = []

    for name in index_names:
        if check_index_exists(es_client, name):
            # Remove any aliases first
            with contextlib.suppress(Exception):
                alias_info = es_client.indices.get_alias(index=name)
                for alias_name in alias_info.body.get(name, {}).get("aliases", {}):
                    es_client.indices.delete_alias(index=name, name=alias_name)
            es_client.indices.delete(index=name)
            deleted.append(name)

    return deleted


def migrate_to_blue_green(
    config: Config,
    date_suffix: str,
) -> None:
    """One-time migration from fixed-name indexes to Blue-Green.

    1. Create new dated indexes
    2. Reindex data from fixed-name indexes
    3. Delete fixed-name indexes (brief downtime)
    4. Create aliases pointing to new indexes

    Args:
        config: Configuration object
        date_suffix: Date suffix for the new indexes (YYYYMMDD)
    """
    es_client = get_es_client(config)

    # Step 1-2: Create new dated indexes and reindex data
    for idx in ALL_INDEXES:
        physical_name = make_physical_index_name(idx, date_suffix)
        if check_index_exists(es_client, physical_name):
            raise Exception(f"Index '{physical_name}' already exists.")

        if not check_index_exists(es_client, idx):
            raise Exception(f"Source index '{idx}' does not exist.")

        mapping = get_mapping_for_index(idx)
        es_client.indices.create(index=physical_name, body=mapping)

        es_client.options(request_timeout=3600).reindex(
            body={
                "source": {"index": idx},
                "dest": {"index": physical_name},
            },
        )

    # Step 3: Delete fixed-name indexes
    for idx in ALL_INDEXES:
        for alias_name, alias_indexes in ALIASES.items():
            if idx in alias_indexes:
                with contextlib.suppress(Exception):
                    es_client.indices.delete_alias(index=idx, name=alias_name)
        es_client.indices.delete(index=idx)

    # Step 4: Create all aliases pointing to new indexes
    actions: list[dict[str, Any]] = []
    for idx in ALL_INDEXES:
        physical_name = make_physical_index_name(idx, date_suffix)
        actions.append({"add": {"index": physical_name, "alias": idx}})
        for alias_name, alias_indexes in ALIASES.items():
            if idx in alias_indexes:
                actions.append({"add": {"index": physical_name, "alias": alias_name}})

    es_client.indices.update_aliases(body={"actions": actions})
