"""Common Elasticsearch mapping definitions shared across all indexes."""

from typing import Any, Dict

from ddbj_search_converter.es.settings import INDEX_SETTINGS

__all__ = ["INDEX_SETTINGS", "get_common_mapping", "merge_mappings"]


def get_common_mapping() -> Dict[str, Any]:
    """Return the common mapping properties shared by all indexes."""
    return {
        "identifier": {"type": "keyword"},
        "properties": {"type": "object", "enabled": False},
        "distribution": {
            "type": "nested",
            "properties": {
                "type": {"type": "keyword"},
                "encodingFormat": {"type": "keyword"},
                "contentUrl": {"type": "keyword", "index": False},
            },
        },
        "isPartOf": {"type": "keyword"},
        "type": {"type": "keyword"},
        "name": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "url": {"type": "keyword", "index": False},
        "organism": {
            "type": "object",
            "properties": {
                "identifier": {"type": "keyword"},
                "name": {"type": "keyword"},
            },
        },
        "title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
        },
        "description": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
        },
        "dbXrefs": {"type": "object", "enabled": False},
        "sameAs": {"type": "object", "enabled": False},
        "status": {"type": "keyword"},
        "accessibility": {"type": "keyword"},
        "dateCreated": {"type": "date"},
        "dateModified": {"type": "date"},
        "datePublished": {"type": "date"},
    }


def merge_mappings(*mappings: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple mapping property dictionaries."""
    result: Dict[str, Any] = {}
    for mapping in mappings:
        result.update(mapping)
    return result
