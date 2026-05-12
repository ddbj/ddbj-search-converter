"""Elasticsearch client management."""

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.settings import (
    BULK_MAX_RETRIES,
    BULK_RETRY_ON_STATUS,
)
from elasticsearch import Elasticsearch

_clients: dict[str, Elasticsearch] = {}


def get_es_client(config: Config) -> Elasticsearch:
    """Return a cached Elasticsearch client for the given config.

    Transport-level retry (timeout / 429 / 502 / 503 / 504) は client 生成時に
    指定する。`helpers.parallel_bulk` / `helpers.bulk` 自体は retry kwargs を受け取らず
    (内部の `streaming_bulk` には `max_retries` 等の引数はあるが、`parallel_bulk` 経由では
    transport level retry の方が動作が確実)、HTTP layer で吸収する設計。
    """
    if config.es_url not in _clients:
        _clients[config.es_url] = Elasticsearch(
            config.es_url,
            request_timeout=120,
            retry_on_timeout=True,
            retry_on_status=BULK_RETRY_ON_STATUS,
            max_retries=BULK_MAX_RETRIES,
        )
    return _clients[config.es_url]


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
    timeout: float = 600.0,
) -> None:
    """Manually refresh an index to make all documents searchable.

    Args:
        es_client: Elasticsearch client
        index: Index name
        timeout: Timeout in seconds for the refresh operation (default: 600s = 10 minutes)
    """
    es_client.options(request_timeout=timeout).indices.refresh(index=index)


def resolve_alias_to_indexes(es_client: Elasticsearch, alias_name: str) -> list[str]:
    """Return the physical index names backing a given alias.

    Returns an empty list if the alias does not exist.
    """
    try:
        response = es_client.indices.get_alias(name=alias_name)
        return list(response.body.keys())
    except Exception:
        return []
