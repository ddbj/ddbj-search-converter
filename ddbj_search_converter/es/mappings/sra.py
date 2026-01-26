"""SRA Elasticsearch mapping definitions for 6 types."""

from typing import Any, Dict, Literal

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

SRA_INDEXES: list[SraIndexType] = [
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
]


def get_sra_common_mapping() -> Dict[str, Any]:
    """Return SRA-common mapping properties (shared by all 6 types)."""
    return {
        "centerName": {"type": "keyword"},
    }


def get_sra_submission_mapping() -> Dict[str, Any]:
    """Return sra-submission specific mapping properties."""
    return {
        "labName": {"type": "keyword"},
    }


def get_sra_study_mapping() -> Dict[str, Any]:
    """Return sra-study specific mapping properties."""
    return {
        "studyType": {"type": "keyword"},
    }


def get_sra_experiment_mapping() -> Dict[str, Any]:
    """Return sra-experiment specific mapping properties."""
    return {
        "instrumentModel": {"type": "keyword"},
        "libraryStrategy": {"type": "keyword"},
        "librarySource": {"type": "keyword"},
        "librarySelection": {"type": "keyword"},
        "libraryLayout": {"type": "keyword"},
    }


def get_sra_run_mapping() -> Dict[str, Any]:
    """Return sra-run specific mapping properties."""
    return {
        "runDate": {"type": "date"},
        "runCenter": {"type": "keyword"},
    }


def get_sra_sample_mapping() -> Dict[str, Any]:
    """Return sra-sample specific mapping properties."""
    return {
        "attributes": {
            "type": "nested",
            "properties": {
                "tag": {"type": "keyword"},
                "value": {"type": "text"},
                "units": {"type": "keyword"},
            },
        },
    }


def get_sra_analysis_mapping() -> Dict[str, Any]:
    """Return sra-analysis specific mapping properties."""
    return {
        "analysisType": {"type": "keyword"},
    }


def get_sra_mapping(index_type: SraIndexType) -> Dict[str, Any]:
    """Return the complete SRA mapping for the specified index type."""
    type_mapping_funcs = {
        "sra-submission": get_sra_submission_mapping,
        "sra-study": get_sra_study_mapping,
        "sra-experiment": get_sra_experiment_mapping,
        "sra-run": get_sra_run_mapping,
        "sra-sample": get_sra_sample_mapping,
        "sra-analysis": get_sra_analysis_mapping,
    }

    specific_mapping = type_mapping_funcs[index_type]()

    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_sra_common_mapping(),
                specific_mapping,
            )
        },
    }
