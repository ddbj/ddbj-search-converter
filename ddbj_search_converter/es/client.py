"""Elasticsearch client management."""

from ddbj_search_converter.config import Config
from elasticsearch import Elasticsearch


def get_es_client(config: Config) -> Elasticsearch:
    """Create and return an Elasticsearch client."""
    return Elasticsearch(config.es_url)


def check_index_exists(es_client: Elasticsearch, index: str) -> bool:
    """Check if an index exists."""
    return es_client.indices.exists(index=index).meta.status == 200


def set_refresh_interval(es_client: Elasticsearch, index: str, interval: str) -> None:
    """Set the refresh interval for an index.

    Args:
        es_client: Elasticsearch client
        index: Index name
        interval: Refresh interval (e.g., "1s", "-1" for disabled)
    """
    es_client.indices.put_settings(
        index=index,
        body={"index": {"refresh_interval": interval}},
    )


def refresh_index(
    es_client: Elasticsearch,
    index: str,
    timeout: str = "10m",
) -> None:
    """Manually refresh an index to make all documents searchable.

    Args:
        es_client: Elasticsearch client
        index: Index name
        timeout: Timeout for the refresh operation (default: 10 minutes)
    """
    es_client.indices.refresh(index=index, timeout=timeout)
