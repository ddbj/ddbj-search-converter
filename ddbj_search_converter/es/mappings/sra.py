"""SRA Elasticsearch mapping definitions for 6 types."""

from typing import Any, Dict, List, Literal

from ddbj_search_converter.es.mappings.common import (INDEX_SETTINGS,
                                                      get_common_mapping,
                                                      merge_mappings)

SraIndexType = Literal[
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
]

SRA_INDEXES: List[SraIndexType] = [
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
]


def get_sra_specific_mapping() -> Dict[str, Any]:
    """Return SRA-specific mapping properties (shared by all 6 types)."""
    return {
        "downloadUrl": {
            "type": "nested",
            "properties": {
                "type": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
            },
        },
    }


def get_sra_mapping(index_type: SraIndexType) -> Dict[str, Any]:  # pylint: disable=unused-argument
    """Return the complete SRA mapping for the specified index type.

    Args:
        index_type: SRA index type (currently unused, kept for API consistency)

    All SRA types use the same mapping: common + downloadUrl.
    """
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_sra_specific_mapping(),
            )
        },
    }
