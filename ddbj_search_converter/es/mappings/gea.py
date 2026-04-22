"""GEA Elasticsearch mapping definitions."""

from typing import Any

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_organization_mapping,
    get_publication_mapping,
    merge_mappings,
)


def get_gea_specific_mapping() -> dict[str, Any]:
    """Return the GEA-specific mapping properties."""
    return {"experimentType": {"type": "keyword"}}


def get_gea_mapping() -> dict[str, Any]:
    """Return the complete GEA mapping."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_organization_mapping(),
                get_publication_mapping(),
                get_gea_specific_mapping(),
            ),
        },
    }
