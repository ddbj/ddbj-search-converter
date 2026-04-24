"""JGA Elasticsearch mapping definitions for 4 types."""

from typing import Any, Literal

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_external_link_mapping,
    get_grant_mapping,
    get_organization_mapping,
    get_publication_mapping,
    merge_mappings,
)

JgaIndexType = Literal[
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
]

JGA_INDEXES: list[JgaIndexType] = [
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
]


def get_jga_specific_mapping(index_type: JgaIndexType) -> dict[str, Any]:
    """Return the JGA-specific mapping properties for the specified index type."""
    text_keyword_256: dict[str, Any] = {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }
    if index_type == "jga-study":
        return {
            "studyType": text_keyword_256,
            "vendor": text_keyword_256,
        }
    if index_type == "jga-dataset":
        return {"datasetType": text_keyword_256}
    return {}


def get_jga_mapping(index_type: JgaIndexType) -> dict[str, Any]:
    """Return the complete JGA mapping for the specified index type.

    organization / externalLink は全 type 共通、publication / grant は jga-study 限定。
    studyType / vendor は jga-study 限定、datasetType は jga-dataset 限定。
    """
    mappings = [
        get_common_mapping(),
        get_organization_mapping(),
        get_external_link_mapping(),
    ]
    if index_type == "jga-study":
        mappings.append(get_publication_mapping())
        mappings.append(get_grant_mapping())
    mappings.append(get_jga_specific_mapping(index_type))
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {"properties": merge_mappings(*mappings)},
    }
