"""Integration: bulk delete rehearsal for blacklist application.

Verifies that ``bulk_delete_by_ids`` classifies the per-doc result correctly:
existing docs go to ``success_count``, missing docs go to ``not_found_count``,
and neither shows up in ``error_count``. This protects against blacklist
application aborting partway through (which would leave production data
in an inconsistent state).

The 404-as-not-found behavior is the key invariant; it is the path that the
sanitize helpers (``89c0499``) protect.
"""

from pathlib import Path

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.bulk_delete import bulk_delete_by_ids
from ddbj_search_converter.es.bulk_insert import bulk_insert_jsonl
from ddbj_search_converter.es.index import create_index_with_suffix, make_physical_index_name
from elasticsearch import Elasticsearch

from ._factories import make_bp_doc


def _seed_bp_docs(
    config: Config,
    physical: str,
    tmp_path: Path,
    identifiers: list[str],
) -> None:
    """Create the dated bp index and bulk insert ``identifiers`` into it."""
    jsonl_path = tmp_path / "bp_seed.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for ident in identifiers:
            f.write(make_bp_doc(ident).model_dump_json(by_alias=True) + "\n")

    create_index_with_suffix(config, "bioproject", physical.rsplit("-", 1)[1])
    bulk_insert_jsonl(config, [jsonl_path], index="bioproject", target_index=physical)


def test_bulk_delete_classifies_existing_and_missing_correctly(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-DELETE-01: 既存 doc は success、不在 doc は not_found に分類、error_count は 0。"""
    suffix = rehearsal_date_suffix
    physical = make_physical_index_name("bioproject", suffix)

    _seed_bp_docs(integration_config, physical, tmp_path, ["PRJDB1", "PRJDB2", "PRJDB3"])

    accessions = {"PRJDB1", "PRJDB2", "PRJDB999", "PRJDB1000"}
    result = bulk_delete_by_ids(integration_config, physical, accessions)

    assert result.total_requested == len(accessions)
    assert result.success_count == 2
    assert result.not_found_count == 2
    assert result.error_count == 0
    assert result.errors == []


def test_bulk_delete_against_unrelated_index_leaves_seed_index_intact(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-DELETE-02: 別 index に対する delete 呼び出しが seed index の doc を削除しない。

    bp index に doc を投入 → bs index に対して bp ID で bulk_delete を実行 →
    bp 側の doc は無傷、bs 側は all not_found。
    """
    suffix = rehearsal_date_suffix
    bp_physical = make_physical_index_name("bioproject", suffix)
    bs_physical = make_physical_index_name("biosample", suffix)

    _seed_bp_docs(integration_config, bp_physical, tmp_path, ["PRJDB1", "PRJDB2", "PRJDB3"])
    create_index_with_suffix(integration_config, "biosample", suffix)

    accessions = {"PRJDB1", "PRJDB2", "PRJDB3"}
    result = bulk_delete_by_ids(integration_config, bs_physical, accessions)

    assert result.success_count == 0
    assert result.not_found_count == 3
    assert result.error_count == 0

    integration_es_client.indices.refresh(index=bp_physical)
    bp_count = integration_es_client.count(index=bp_physical).body["count"]
    assert bp_count == 3, "別 index への delete が seed index の doc を巻き込んだ"
