"""SRA Elasticsearch mapping definitions for 6 types."""

from typing import Any, Literal

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_organization_mapping,
    get_publication_mapping,
    merge_mappings,
)

SraIndexType = Literal[
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
]

SRA_INDEXES: list[SraIndexType] = [
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
]


def get_sra_specific_mapping(index_type: SraIndexType) -> dict[str, Any]:
    """Return SRA type 別の specific mapping.

    - sra-experiment: libraryStrategy / librarySource / librarySelection /
      libraryLayout / platform (keyword) + instrumentModel (text + keyword subfield)
    - sra-analysis: analysisType (keyword)
    - sra-study / sra-run / sra-sample / sra-submission: 追加 specific なし
    """
    if index_type == "sra-experiment":
        return {
            "libraryStrategy": {"type": "keyword"},
            "librarySource": {"type": "keyword"},
            "librarySelection": {"type": "keyword"},
            "libraryLayout": {"type": "keyword"},
            "platform": {"type": "keyword"},
            "instrumentModel": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
        }
    if index_type == "sra-analysis":
        return {"analysisType": {"type": "keyword"}}
    return {}


def get_sra_mapping(index_type: SraIndexType) -> dict[str, Any]:
    """Return the complete SRA mapping for the specified index type."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_organization_mapping(),
                get_publication_mapping(),
                get_sra_specific_mapping(index_type),
            )
        },
    }
