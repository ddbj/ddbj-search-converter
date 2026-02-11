"""Pipeline-aligned ordering for run_names.

Defines the preferred display order that mirrors run_pipeline.sh execution
sequence. Run names not listed here are sorted alphabetically at the end.
"""

PIPELINE_ORDER: list[str] = [
    # Phase 0: Pre-check
    "check_external_resources",
    # Phase 1: DBLink Construction — prepare
    "prepare_bioproject_xml",
    "prepare_biosample_xml",
    "build_sra_and_dra_accessions_db",
    # Phase 1: DBLink Construction — init
    "init_dblink_db",
    # Phase 1: DBLink Construction — relations
    "create_dblink_bp_bs_relations",
    "create_dblink_bp_relations",
    "create_dblink_assembly_and_master_relations",
    "create_dblink_gea_relations",
    "create_dblink_metabobank_relations",
    "create_dblink_jga_relations",
    "create_dblink_sra_internal_relations",
    # Phase 1: DBLink Construction — finalize
    "finalize_dblink_db",
    "dump_dblink_files",
    "show_dblink_counts",
    # Phase 2: JSONL Generation — sync
    "sync_ncbi_tar",
    "sync_dra_tar",
    "build_bp_bs_date_cache",
    # Phase 2: JSONL Generation — generate
    "generate_bp_jsonl",
    "generate_bs_jsonl",
    "generate_sra_jsonl",
    "generate_jga_jsonl",
    "regenerate_jsonl",
    # Phase 3: Elasticsearch
    "es_create_index",
    "es_delete_index",
    "es_bulk_insert",
    "es_delete_blacklist",
    "es_list_indexes",
    "es_health_check",
    "es_snapshot",
    # Debug / Utility
    "show_log_summary",
    "show_log",
]

_ORDER_MAP: dict[str, int] = {name: i for i, name in enumerate(PIPELINE_ORDER)}
_FALLBACK: int = len(PIPELINE_ORDER)


def run_name_sort_key(name: str) -> tuple[int, str]:
    """Sort key: pipeline position first, then alphabetical for unknowns."""
    return (_ORDER_MAP.get(name, _FALLBACK), name)


def sort_run_names(names: list[str]) -> list[str]:
    """Sort run_names in pipeline order."""
    return sorted(names, key=run_name_sort_key)
