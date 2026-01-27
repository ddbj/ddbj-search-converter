"""Elasticsearch index creation and deletion."""

from typing import Any, Dict, List, Literal, Union, cast

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import check_index_exists, get_es_client
from ddbj_search_converter.es.mappings.bioproject import get_bioproject_mapping
from ddbj_search_converter.es.mappings.biosample import get_biosample_mapping
from ddbj_search_converter.es.mappings.jga import (JGA_INDEXES, JgaIndexType,
                                                   get_jga_mapping)
from ddbj_search_converter.es.mappings.sra import (SRA_INDEXES, SraIndexType,
                                                   get_sra_mapping)

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

ALL_INDEXES: List[IndexName] = [
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

ALIASES: Dict[str, List[IndexName]] = {
    "sra": cast(List[IndexName], list(SRA_INDEXES)),
    "jga": cast(List[IndexName], list(JGA_INDEXES)),
    "entries": list(ALL_INDEXES),
}


def get_indexes_for_group(group: IndexGroup) -> List[IndexName]:
    """Get the list of index names for a given group."""
    if group == "all":
        return ALL_INDEXES.copy()
    if group == "sra":
        return cast(List[IndexName], list(SRA_INDEXES))
    if group == "jga":
        return cast(List[IndexName], list(JGA_INDEXES))
    return [group]


def get_mapping_for_index(index: IndexName) -> Dict[str, Any]:
    """Get the mapping for a specific index."""
    if index == "bioproject":
        return get_bioproject_mapping()
    if index == "biosample":
        return get_biosample_mapping()
    if index.startswith("sra-"):
        return get_sra_mapping(cast(SraIndexType, index))
    if index.startswith("jga-"):
        return get_jga_mapping(cast(JgaIndexType, index))
    raise ValueError(f"Unknown index: {index}")


def create_index(
    config: Config,
    index: Union[IndexName, IndexGroup],
    skip_existing: bool = False,
) -> List[str]:
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
    created: List[str] = []

    indexes = get_indexes_for_group(cast(IndexGroup, index))

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
    index: Union[IndexName, IndexGroup],
    skip_missing: bool = False,
) -> List[str]:
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
    deleted: List[str] = []

    indexes = get_indexes_for_group(cast(IndexGroup, index))

    for idx in indexes:
        if not check_index_exists(es_client, idx):
            if skip_missing:
                continue
            raise Exception(f"Index '{idx}' does not exist.")

        for alias_name, alias_indexes in ALIASES.items():
            if idx in alias_indexes:
                try:
                    es_client.indices.delete_alias(index=idx, name=alias_name)
                except Exception:
                    pass
        es_client.indices.delete(index=idx)
        deleted.append(idx)

    return deleted


def setup_aliases(config: Config) -> List[str]:
    """Set up aliases for all existing indexes.

    Useful for adding aliases to indexes that were created before
    alias support was added.

    Returns:
        List of alias names that were set up
    """
    es_client = get_es_client(config)
    setup: List[str] = []

    for alias_name, alias_indexes in ALIASES.items():
        for idx in alias_indexes:
            if check_index_exists(es_client, idx):
                es_client.indices.put_alias(index=idx, name=alias_name)
        setup.append(alias_name)

    return setup


def list_indexes(config: Config) -> List[Dict[str, Any]]:
    """List all DDBJ-Search indexes with their doc counts.

    Returns:
        List of dicts containing index name and doc_count
    """
    es_client = get_es_client(config)
    result: List[Dict[str, Any]] = []

    for idx in ALL_INDEXES:
        if check_index_exists(es_client, idx):
            stats = es_client.indices.stats(index=idx)
            doc_count = stats["_all"]["primaries"]["docs"]["count"]
            result.append({
                "index": idx,
                "doc_count": doc_count,
                "exists": True,
            })
        else:
            result.append({
                "index": idx,
                "doc_count": 0,
                "exists": False,
            })

    return result
