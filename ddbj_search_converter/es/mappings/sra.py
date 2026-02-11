"""SRA Elasticsearch mapping definitions for 6 types."""

from typing import Any, Literal

from ddbj_search_converter.es.mappings.common import INDEX_SETTINGS, get_common_mapping

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


def get_sra_mapping(_index_type: SraIndexType) -> dict[str, Any]:
    """Return the complete SRA mapping for the specified index type.

    Args:
        index_type: SRA index type (currently unused, kept for API consistency)

    All SRA types use the same mapping (common only).
    """
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {"properties": get_common_mapping()},
    }
