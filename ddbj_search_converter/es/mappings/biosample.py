"""BioSample Elasticsearch mapping definition."""

from typing import Any

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_organization_mapping,
    merge_mappings,
)


def get_biosample_specific_mapping() -> dict[str, Any]:
    """Return BioSample-specific mapping properties."""
    return {
        "model": {"type": "keyword"},
        "package": {
            "type": "object",
            "properties": {
                "name": {"type": "keyword"},
                "displayName": {"type": "keyword", "index": False},
            },
        },
    }


def get_biosample_mapping() -> dict[str, Any]:
    """Return the complete BioSample mapping including settings."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_organization_mapping(),
                get_biosample_specific_mapping(),
            )
        },
    }
