"""Common Elasticsearch mapping definitions shared across all indexes."""

from typing import Any, Dict

# Index settings
INDEX_SETTINGS: Dict[str, Any] = {
    "index": {
        "refresh_interval": "1s",
        "mapping.nested_objects.limit": 100000,
        "number_of_shards": 1,
        "number_of_replicas": 0,
    }
}


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
        "name": {"type": "text"},
        "url": {"type": "keyword", "index": False},
        "organism": {
            "type": "nested",
            "properties": {
                "identifier": {"type": "keyword"},
                "name": {"type": "keyword"},
            },
        },
        "title": {"type": "text"},
        "description": {"type": "text"},
        "dbXref": {
            "type": "nested",
            "properties": {
                "identifier": {"type": "keyword"},
                "type": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
            },
        },
        "sameAs": {
            "type": "nested",
            "properties": {
                "identifier": {"type": "keyword"},
                "type": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
            },
        },
        "status": {"type": "keyword"},
        "visibility": {"type": "keyword"},
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
