"""Integration: Blue-Green alias swap rehearsal.

Verifies that ``swap_aliases`` atomically moves all aliases from old dated
indexes to new dated indexes. If the swap is not atomic, search queries
during deployment could land on empty / partial indexes ⇒ user-visible
search outage. This rehearsal is the key gate before a Blue-Green deploy.

Old dated indexes use ``99991230``; new dated indexes use ``99991231``.
Both are staging-isolated future suffixes that never collide with real
deployments.
"""

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import resolve_alias_to_indexes
from ddbj_search_converter.es.index import (
    ALIASES,
    ALL_INDEXES,
    create_index_with_suffix,
    delete_physical_indexes,
    make_physical_index_name,
    swap_aliases,
)
from elasticsearch import Elasticsearch


def _attach_blue_green_aliases(client: Elasticsearch, suffix: str) -> None:
    """Build the same alias graph that production has after a Blue-Green deploy.

    For each logical index, attach the per-index alias (e.g. ``bioproject``)
    plus the relevant group aliases (``entries``, ``sra``, ``jga``) onto the
    dated physical index.
    """
    for idx in ALL_INDEXES:
        physical = make_physical_index_name(idx, suffix)
        client.indices.put_alias(index=physical, name=idx)
        for alias_name, alias_indexes in ALIASES.items():
            if idx in alias_indexes:
                client.indices.put_alias(index=physical, name=alias_name)


def test_swap_aliases_moves_all_aliases_to_new_dated_indexes(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    rehearsal_old_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    allow_destructive_alias_tests: None,
) -> None:
    """IT-SWAP-01: swap_aliases で全 alias が旧 dated → 新 dated に移る。"""
    new_suffix = rehearsal_date_suffix
    old_suffix = rehearsal_old_date_suffix

    create_index_with_suffix(integration_config, "all", old_suffix)
    _attach_blue_green_aliases(integration_es_client, old_suffix)
    create_index_with_suffix(integration_config, "all", new_suffix)

    for idx in ALL_INDEXES:
        old_physical = make_physical_index_name(idx, old_suffix)
        assert resolve_alias_to_indexes(integration_es_client, idx) == [old_physical]

    old_indexes = swap_aliases(integration_config, new_suffix)

    for idx in ALL_INDEXES:
        new_physical = make_physical_index_name(idx, new_suffix)
        old_physical = make_physical_index_name(idx, old_suffix)
        assert resolve_alias_to_indexes(integration_es_client, idx) == [new_physical]
        assert old_indexes[idx] == old_physical

    for group_name, members in ALIASES.items():
        targets = resolve_alias_to_indexes(integration_es_client, group_name)
        expected = {make_physical_index_name(idx, new_suffix) for idx in members}
        assert set(targets) == expected, f"group alias {group_name} drifted"


def test_entries_alias_resolution_is_never_empty_around_swap(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    rehearsal_old_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    allow_destructive_alias_tests: None,
) -> None:
    """IT-SWAP-02: swap 操作前後で entries alias が常に 14 個の物理 index に解決される。"""
    new_suffix = rehearsal_date_suffix
    old_suffix = rehearsal_old_date_suffix

    create_index_with_suffix(integration_config, "all", old_suffix)
    _attach_blue_green_aliases(integration_es_client, old_suffix)
    create_index_with_suffix(integration_config, "all", new_suffix)

    pre = resolve_alias_to_indexes(integration_es_client, "entries")
    assert len(pre) == len(ALL_INDEXES)
    assert set(pre) == {make_physical_index_name(idx, old_suffix) for idx in ALL_INDEXES}

    swap_aliases(integration_config, new_suffix)

    post = resolve_alias_to_indexes(integration_es_client, "entries")
    assert len(post) == len(ALL_INDEXES)
    assert set(post) == {make_physical_index_name(idx, new_suffix) for idx in ALL_INDEXES}


def test_alias_resolves_after_old_index_deletion(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    rehearsal_old_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    allow_destructive_alias_tests: None,
) -> None:
    """IT-SWAP-03: swap 後に旧 dated index を削除しても alias は新側で解決可能。"""
    new_suffix = rehearsal_date_suffix
    old_suffix = rehearsal_old_date_suffix

    create_index_with_suffix(integration_config, "all", old_suffix)
    _attach_blue_green_aliases(integration_es_client, old_suffix)
    create_index_with_suffix(integration_config, "all", new_suffix)
    swap_aliases(integration_config, new_suffix)

    old_names = [make_physical_index_name(idx, old_suffix) for idx in ALL_INDEXES]
    delete_physical_indexes(integration_config, old_names)

    for idx in ALL_INDEXES:
        new_physical = make_physical_index_name(idx, new_suffix)
        assert resolve_alias_to_indexes(integration_es_client, idx) == [new_physical]

    targets = resolve_alias_to_indexes(integration_es_client, "entries")
    assert len(targets) == len(ALL_INDEXES)
    assert set(targets) == {make_physical_index_name(idx, new_suffix) for idx in ALL_INDEXES}
