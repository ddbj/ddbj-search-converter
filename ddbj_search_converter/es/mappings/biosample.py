"""BioSample Elasticsearch mapping definition."""

from typing import Any

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_organization_mapping,
    merge_mappings,
)


def get_biosample_specific_mapping() -> dict[str, Any]:
    """Return BioSample-specific mapping properties.

    host は中程度 cardinality の生物名で facet 化要請があり text + keyword を維持。
    strain / isolate は submitter 自由文 (株名・検体名の表記揺れ多数、placeholder
    "not provided" 等の混入も多い) で `.keyword` を生やしても facet/term 用途に
    使えないため text 単独。
    """
    text_keyword_256: dict[str, Any] = {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }
    return {
        "model": {"type": "keyword"},
        "package": {
            "type": "object",
            "properties": {
                "name": {"type": "keyword"},
                "displayName": {"type": "keyword", "index": False},
            },
        },
        "derivedFrom": {
            "type": "nested",
            "properties": {
                "identifier": {"type": "keyword"},
                "type": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
            },
        },
        "geoLocName": {"type": "text"},
        "collectionDate": {"type": "text"},
        "host": text_keyword_256,
        "strain": {"type": "text"},
        "isolate": {"type": "text"},
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
