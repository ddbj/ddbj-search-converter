"""BioSample Elasticsearch mapping definition."""

from typing import Any, Dict

from ddbj_search_converter.es.mappings.common import (INDEX_SETTINGS,
                                                      get_common_mapping,
                                                      merge_mappings)


def get_biosample_specific_mapping() -> Dict[str, Any]:
    """Return BioSample-specific mapping properties."""
    return {
        "attributes": {
            "type": "nested",
            "properties": {
                "attribute_name": {"type": "keyword"},
                "display_name": {"type": "keyword"},
                "harmonized_name": {"type": "keyword"},
                "content": {"type": "text"},
            },
        },
        "model": {
            "type": "nested",
            "properties": {
                "name": {"type": "keyword"},
            },
        },
        "package": {
            "type": "object",
            "properties": {
                "name": {"type": "keyword"},
                "display_name": {"type": "keyword"},
            },
        },
    }


def get_biosample_mapping() -> Dict[str, Any]:
    """Return the complete BioSample mapping including settings."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_biosample_specific_mapping(),
            )
        },
    }
