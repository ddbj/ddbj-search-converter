"""BioProject Elasticsearch mapping definition."""

from typing import Any

from ddbj_search_converter.es.mappings.common import (
    INDEX_SETTINGS,
    get_common_mapping,
    get_external_link_mapping,
    get_grant_mapping,
    get_organization_mapping,
    get_publication_mapping,
    merge_mappings,
)


def get_bioproject_specific_mapping() -> dict[str, Any]:
    """Return BioProject-specific mapping properties.

    organization / publication / grant / externalLink は共通 helper に昇格済のためここには含めない。
    """
    return {
        "objectType": {"type": "keyword"},
        "parentBioProjects": {"type": "object", "enabled": False},
        "childBioProjects": {"type": "object", "enabled": False},
    }


def get_bioproject_mapping() -> dict[str, Any]:
    """Return the complete BioProject mapping including settings."""
    return {
        "settings": INDEX_SETTINGS,
        "mappings": {
            "properties": merge_mappings(
                get_common_mapping(),
                get_organization_mapping(),
                get_publication_mapping(),
                get_grant_mapping(),
                get_external_link_mapping(),
                get_bioproject_specific_mapping(),
            )
        },
    }
