"""Elasticsearch mapping definitions."""

from ddbj_search_converter.es.mappings.bioproject import get_bioproject_mapping
from ddbj_search_converter.es.mappings.biosample import get_biosample_mapping
from ddbj_search_converter.es.mappings.common import INDEX_SETTINGS
from ddbj_search_converter.es.mappings.jga import get_jga_mapping
from ddbj_search_converter.es.mappings.sra import get_sra_mapping

__all__ = [
    "INDEX_SETTINGS",
    "get_bioproject_mapping",
    "get_biosample_mapping",
    "get_jga_mapping",
    "get_sra_mapping",
]
