"""Common Elasticsearch mapping definitions shared across all indexes."""

from typing import Any

from ddbj_search_converter.es.settings import INDEX_SETTINGS

__all__ = [
    "INDEX_SETTINGS",
    "get_common_mapping",
    "get_external_link_mapping",
    "get_grant_mapping",
    "get_organization_mapping",
    "get_organization_properties",
    "get_publication_mapping",
    "merge_mappings",
]


def get_common_mapping() -> dict[str, Any]:
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
        "sameAs": {
            "type": "nested",
            "properties": {
                "identifier": {"type": "keyword"},
                "type": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
            },
        },
        "status": {"type": "keyword"},
        "accessibility": {"type": "keyword"},
        "dateCreated": {"type": "date"},
        "dateModified": {"type": "date"},
        "datePublished": {"type": "date"},
    }


def get_organization_properties() -> dict[str, Any]:
    """Return the shared Organization property definitions."""
    return {
        "name": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "abbreviation": {"type": "keyword"},
        "role": {"type": "keyword"},
        "organizationType": {"type": "keyword"},
        "department": {"type": "keyword"},
        "url": {"type": "keyword", "index": False},
    }


def get_organization_mapping() -> dict[str, Any]:
    """Return the shared Organization nested mapping."""
    return {
        "organization": {
            "type": "nested",
            "properties": get_organization_properties(),
        },
    }


def get_publication_mapping() -> dict[str, Any]:
    """Return the shared Publication nested mapping."""
    return {
        "publication": {
            "type": "nested",
            "properties": {
                "id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                },
                "date": {"type": "keyword"},
                "reference": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
                "dbType": {"type": "keyword"},
                "status": {"type": "keyword"},
            },
        },
    }


def get_grant_mapping() -> dict[str, Any]:
    """Return the shared Grant nested mapping.

    agency は Organization と同一 properties を持つ。Grant.agency では
    role / organizationType / department / url は常に None だが、mapping 上は
    Organization と統一しておく。
    """
    return {
        "grant": {
            "type": "nested",
            "properties": {
                "id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                },
                "agency": {
                    "type": "nested",
                    "properties": get_organization_properties(),
                },
            },
        },
    }


def get_external_link_mapping() -> dict[str, Any]:
    """Return the shared ExternalLink nested mapping."""
    return {
        "externalLink": {
            "type": "nested",
            "properties": {
                "url": {"type": "keyword", "index": False},
                "label": {"type": "keyword"},
            },
        },
    }


def merge_mappings(*mappings: dict[str, Any]) -> dict[str, Any]:
    """Merge multiple mapping property dictionaries."""
    result: dict[str, Any] = {}
    for mapping in mappings:
        result.update(mapping)
    return result
