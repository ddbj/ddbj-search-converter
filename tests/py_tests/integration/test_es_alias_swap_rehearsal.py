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
    get_indexes_for_group,
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


def test_swap_aliases_sra_only_preserves_other_groups(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    rehearsal_old_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    allow_destructive_alias_tests: None,
) -> None:
    """IT-SWAP-04: index_group='sra' で SRA だけ swap し、他 group の alias は old のまま残る。"""
    new_suffix = rehearsal_date_suffix
    old_suffix = rehearsal_old_date_suffix

    create_index_with_suffix(integration_config, "all", old_suffix)
    _attach_blue_green_aliases(integration_es_client, old_suffix)
    # SRA group の new dated index だけ作成
    create_index_with_suffix(integration_config, "sra", new_suffix)

    old_indexes = swap_aliases(integration_config, new_suffix, "sra")

    sra_indexes = get_indexes_for_group("sra")
    for idx in sra_indexes:
        new_physical = make_physical_index_name(idx, new_suffix)
        assert resolve_alias_to_indexes(integration_es_client, idx) == [new_physical]

    untouched = [idx for idx in ALL_INDEXES if idx not in sra_indexes]
    for idx in untouched:
        old_physical = make_physical_index_name(idx, old_suffix)
        assert resolve_alias_to_indexes(integration_es_client, idx) == [old_physical]

    assert set(old_indexes.keys()) == set(sra_indexes)


def test_swap_aliases_sra_only_keeps_entries_alias_spanning_old_and_new(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    rehearsal_old_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    allow_destructive_alias_tests: None,
) -> None:
    """IT-SWAP-05: SRA だけ swap した後、entries alias は SRA-new + 他 5 group の old を指す。

    部分 Blue-Green 中も entries alias の解決対象数 (= 14) は不変で、検索断が起きないことを確認。
    """
    new_suffix = rehearsal_date_suffix
    old_suffix = rehearsal_old_date_suffix

    create_index_with_suffix(integration_config, "all", old_suffix)
    _attach_blue_green_aliases(integration_es_client, old_suffix)
    create_index_with_suffix(integration_config, "sra", new_suffix)

    swap_aliases(integration_config, new_suffix, "sra")

    targets = resolve_alias_to_indexes(integration_es_client, "entries")
    # SPEC: tests/integration-scenarios.md IT-SWAP-04 — partial swap でも entries
    # alias は常に 14 個 (= len(ALL_INDEXES)) に解決される。docstring の手計算と
    # 整合させるため固定値 14 でも explicit に pin する。
    assert len(targets) == 14, f"entries alias resolved to {len(targets)} indexes, expected 14"
    assert len(targets) == len(ALL_INDEXES)

    sra_indexes = set(get_indexes_for_group("sra"))
    expected_new_sra = {make_physical_index_name(idx, new_suffix) for idx in sra_indexes}
    expected_old_others = {make_physical_index_name(idx, old_suffix) for idx in ALL_INDEXES if idx not in sra_indexes}
    assert set(targets) == expected_new_sra | expected_old_others
    # SRA 6 + 他 8 = 14 の内訳も pin する (group 構成変更時の検出ポイント)。
    assert len(expected_new_sra) == 6
    assert len(expected_old_others) == 8


def test_delete_old_indexes_for_sra_group_only_removes_sra_indexes(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    rehearsal_old_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    allow_destructive_alias_tests: None,
) -> None:
    """IT-SWAP-06: SRA group だけ delete_old_indexes しても他 group の old index は残る。"""
    new_suffix = rehearsal_date_suffix
    old_suffix = rehearsal_old_date_suffix

    create_index_with_suffix(integration_config, "all", old_suffix)
    _attach_blue_green_aliases(integration_es_client, old_suffix)
    create_index_with_suffix(integration_config, "sra", new_suffix)
    swap_aliases(integration_config, new_suffix, "sra")

    sra_indexes = get_indexes_for_group("sra")
    sra_old_names = [make_physical_index_name(idx, old_suffix) for idx in sra_indexes]
    delete_physical_indexes(integration_config, sra_old_names)

    # SRA の old は消えている
    for name in sra_old_names:
        assert not integration_es_client.indices.exists(index=name)

    # 他 group の old は残っている
    untouched = [idx for idx in ALL_INDEXES if idx not in sra_indexes]
    for idx in untouched:
        old_physical = make_physical_index_name(idx, old_suffix)
        assert integration_es_client.indices.exists(index=old_physical)
