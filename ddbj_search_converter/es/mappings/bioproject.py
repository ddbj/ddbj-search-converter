"""BioProject Elasticsearch mapping definition."""

from typing import Any

from ddbj_search_converter.es.mappings.common import INDEX_SETTINGS, get_common_mapping, merge_mappings


def get_bioproject_specific_mapping() -> dict[str, Any]:
    """Return BioProject-specific mapping properties."""
    return {
        "objectType": {"type": "keyword"},
        "organization": {
            "type": "nested",
            "properties": {
                "name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "organizationType": {"type": "keyword"},
                "role": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
                "abbreviation": {"type": "keyword"},
            },
        },
        "publication": {
            "type": "nested",
            "properties": {
                "id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                },
                "date": {"type": "keyword"},
                "Reference": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
                "DbType": {"type": "keyword"},
                "status": {"type": "keyword"},
            },
        },
        "grant": {
            "type": "nested",
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "keyword"},
                "agency": {
                    "type": "nested",
                    "properties": {
                        "abbreviation": {"type": "keyword"},
                        "name": {"type": "keyword"},
                    },
                },
            },
        },
        "externalLink": {
            "type": "nested",
            "properties": {
                "url": {"type": "keyword", "index": False},
                "label": {"type": "keyword"},
            },
        },
    }


def get_bioproject_mapping() -> dict[str, Any]:
    """Return the complete BioProject mapping including settings."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_bioproject_specific_mapping(),
            )
        },
    }
