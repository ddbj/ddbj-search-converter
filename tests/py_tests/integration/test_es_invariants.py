"""Integration: ES seed data structural invariants.

These tests assume the integration ES is populated (staging or production with
data). They verify that the alias graph and per-index counts are internally
consistent — a failure means the ES state is corrupt or alias swap left the
cluster in an inconsistent state.
"""

from ddbj_search_converter.es.index import ALL_INDEXES
from elasticsearch import Elasticsearch


def test_entries_alias_count_equals_sum_of_individual_logical_counts(
    integration_es_client: Elasticsearch,
    staging_es_has_seed_data: None,
) -> None:
    """IT-INVARIANT-02: ``entries`` alias 経由の count == 各 logical alias 個別 count の合計。

    entries は 14 logical alias が指す全物理 index の合計。doc は重複なくカウントされる
    (各 doc は単一 logical index にしか属さない) ため、合計と一致するのが不変条件。
    """
    entries_count = integration_es_client.count(index="entries").body["count"]
    assert entries_count > 0

    sum_logical = 0
    for logical in ALL_INDEXES:
        c = integration_es_client.count(index=logical).body["count"]
        assert c >= 0
        sum_logical += c

    assert entries_count == sum_logical, f"entries={entries_count}, sum(logicals)={sum_logical}"


def test_each_data_type_logical_alias_resolves_to_exactly_one_physical(
    integration_es_client: Elasticsearch,
    staging_es_has_seed_data: None,
) -> None:
    """IT-INVARIANT-02b: 各 logical alias は dated 物理 index 1 個に解決される。

    Blue-Green swap は各 logical → 単一物理を保つ設計。複数解決されると古い index が
    取り残されている (delete_old_indexes が漏れた) サイン。
    """
    multi: dict[str, list[str]] = {}
    for logical in ALL_INDEXES:
        info = integration_es_client.indices.get_alias(name=logical)
        physicals = sorted(info.body.keys())
        if len(physicals) != 1:
            multi[logical] = physicals
    assert not multi, f"logical aliases not resolving to single physical: {multi}"


def test_sra_alias_resolves_to_six_physical_indexes(
    integration_es_client: Elasticsearch,
    staging_es_has_seed_data: None,
) -> None:
    """IT-INVARIANT-02c: ``sra`` group alias は 6 物理 index に解決される。"""
    info = integration_es_client.indices.get_alias(name="sra")
    physicals = list(info.body.keys())
    assert len(physicals) == 6, f"sra alias resolves to {len(physicals)} indexes (expected 6): {physicals}"


def test_jga_alias_resolves_to_four_physical_indexes(
    integration_es_client: Elasticsearch,
    staging_es_has_seed_data: None,
) -> None:
    """IT-INVARIANT-02d: ``jga`` group alias は 4 物理 index に解決される。"""
    info = integration_es_client.indices.get_alias(name="jga")
    physicals = list(info.body.keys())
    assert len(physicals) == 4, f"jga alias resolves to {len(physicals)} indexes (expected 4): {physicals}"
