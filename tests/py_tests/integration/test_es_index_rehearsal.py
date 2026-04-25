"""Integration: ES mapping rehearsal smoke.

Verifies that the current mappings in ``ddbj_search_converter.es.mappings``
are accepted by a real ES cluster (staging or local compose). If the cluster
rejects a mapping (field type conflict, unknown setting, JSON syntax error),
``create_index_with_suffix`` raises and the test fails *before* any
production deployment touches the same mapping.
"""

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import check_index_exists
from ddbj_search_converter.es.index import (
    ALL_INDEXES,
    create_index_with_suffix,
    make_physical_index_name,
)
from elasticsearch import Elasticsearch


def test_current_mappings_are_accepted_by_es(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
) -> None:
    """ALL_INDEXES の各 logical index に対し、現 mapping で dated 物理 index が作れる。

    PUT が ES に通れば mapping JSON は構文・型・設定すべてが妥当。各物理 index の
    properties が空でないことで、ES が mapping を実際に保存したことを確認する。
    """
    suffix = rehearsal_date_suffix

    created = create_index_with_suffix(integration_config, "all", suffix)

    expected = [make_physical_index_name(idx, suffix) for idx in ALL_INDEXES]
    assert sorted(created) == sorted(expected)

    for physical in expected:
        assert check_index_exists(integration_es_client, physical), (
            f"index {physical} was not created in ES"
        )
        mapping = integration_es_client.indices.get_mapping(index=physical)
        properties = mapping.body[physical]["mappings"].get("properties", {})
        assert properties, f"index {physical} has empty properties (mapping not stored)"
