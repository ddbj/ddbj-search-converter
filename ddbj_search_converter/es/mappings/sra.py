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
      libraryLayout / platform / instrumentModel / libraryName (text + keyword)、
      libraryConstructionProtocol (text 単独、長文)
    - sra-analysis: analysisType (text + keyword subfield)
    - sra-sample: collectionDate / geoLocName (text 単独)、derivedFrom (nested)
    - sra-study / sra-run / sra-submission: 追加 specific なし
    """
    text_keyword_256: dict[str, Any] = {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }
    if index_type == "sra-experiment":
        return {
            "libraryStrategy": text_keyword_256,
            "librarySource": text_keyword_256,
            "librarySelection": text_keyword_256,
            "libraryLayout": text_keyword_256,
            "platform": text_keyword_256,
            "instrumentModel": text_keyword_256,
            "libraryName": text_keyword_256,
            "libraryConstructionProtocol": {"type": "text"},
        }
    if index_type == "sra-analysis":
        return {"analysisType": text_keyword_256}
    if index_type == "sra-sample":
        return {
            "collectionDate": {"type": "text"},
            "geoLocName": {"type": "text"},
            "derivedFrom": {
                "type": "nested",
                "properties": {
                    "identifier": {"type": "keyword"},
                    "type": {"type": "keyword"},
                    "url": {"type": "keyword", "index": False},
                },
            },
        }
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
