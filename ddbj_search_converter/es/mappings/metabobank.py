"""MetaboBank Elasticsearch mapping definitions."""

from typing import Any

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_organization_mapping,
    get_publication_mapping,
    merge_mappings,
)


def get_metabobank_specific_mapping() -> dict[str, Any]:
    """Return the MetaboBank-specific mapping properties."""
    text_keyword_256: dict[str, Any] = {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }
    return {
        "studyType": text_keyword_256,
        "experimentType": text_keyword_256,
        "submissionType": text_keyword_256,
    }


def get_metabobank_mapping() -> dict[str, Any]:
    """Return the complete MetaboBank mapping."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_organization_mapping(),
                get_publication_mapping(),
                get_metabobank_specific_mapping(),
            ),
        },
    }
