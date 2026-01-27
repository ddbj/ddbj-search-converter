"""JGA Elasticsearch mapping definitions for 4 types."""

from typing import Any, Dict, List, Literal

from ddbj_search_converter.es.mappings.common import (INDEX_SETTINGS,
                                                      get_common_mapping)

JgaIndexType = Literal[
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
]

JGA_INDEXES: List[JgaIndexType] = [
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
]


def get_jga_mapping(index_type: JgaIndexType) -> Dict[str, Any]:  # pylint: disable=unused-argument
    """Return the complete JGA mapping for the specified index type.

    Args:
        index_type: JGA index type (currently unused, kept for API consistency)

    All JGA types use the same common mapping without additional specific fields.
    """
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": get_common_mapping()
        },
    }
