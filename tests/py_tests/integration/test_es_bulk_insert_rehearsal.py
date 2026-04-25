"""Integration: bulk insert rehearsal against current ES mappings.

Verifies that documents constructed from ``schema.*`` are accepted by the
corresponding ES mapping. If the mapping has drifted from the schema (e.g.,
a field type tightened in mapping but still permissive in schema),
``parallel_bulk`` reports ``mapping_parsing_exception`` and the test fails
*before* a production deploy hits the same drift.

One test per data type: each schema/mapping pair has independent drift risk;
splitting tests keeps failures localized. ``BioSample`` is the strongest
detector since its mapping is the most attribute-heavy.
"""

from collections.abc import Sequence
from pathlib import Path

import pytest
from pydantic import BaseModel

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.bulk_insert import bulk_insert_jsonl
from ddbj_search_converter.es.index import IndexName, create_index_with_suffix, make_physical_index_name
from ddbj_search_converter.es.mappings.jga import JGA_INDEXES, JgaIndexType
from ddbj_search_converter.es.mappings.sra import SRA_INDEXES, SraIndexType
from elasticsearch import Elasticsearch

from ._factories import (
    make_bp_doc,
    make_bs_doc,
    make_gea_doc,
    make_jga_doc,
    make_mtb_doc,
    make_sra_doc,
)

# DDBJ accession prefixes per logical index. Identifiers built from these
# match production-style naming so sameAs prefix matching is realistic.
_SRA_PREFIX: dict[SraIndexType, str] = {
    "sra-submission": "DRA",
    "sra-study": "DRP",
    "sra-experiment": "DRX",
    "sra-run": "DRR",
    "sra-sample": "DRS",
    "sra-analysis": "DRZ",
}
_JGA_PREFIX: dict[JgaIndexType, str] = {
    "jga-study": "JGAS",
    "jga-dataset": "JGAD",
    "jga-dac": "JGAC",
    "jga-policy": "JGAP",
}


def _run_rehearsal(
    *,
    config: Config,
    suffix: str,
    tmp_path: Path,
    logical_index: IndexName,
    docs: Sequence[BaseModel],
    file_stem: str,
) -> None:
    """Write docs to JSONL, create dated index, bulk insert, assert invariants."""
    physical = make_physical_index_name(logical_index, suffix)
    jsonl_path = tmp_path / f"{file_stem}.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(doc.model_dump_json(by_alias=True) + "\n")

    create_index_with_suffix(config, logical_index, suffix)
    result = bulk_insert_jsonl(config, [jsonl_path], index=logical_index, target_index=physical)

    assert result.error_count == 0, f"[{logical_index}] unexpected errors: {result.errors}"
    assert result.success_count == len(docs)
    assert result.total_docs == len(docs)
    assert result.index == physical


def test_current_bp_mapping_accepts_pydantic_schema_docs(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-MAPPING-02-bp: schema.BioProject ↔ bp mapping drift detection."""
    docs = [make_bp_doc(f"PRJDB{i}") for i in range(1, 6)]
    _run_rehearsal(
        config=integration_config,
        suffix=rehearsal_date_suffix,
        tmp_path=tmp_path,
        logical_index="bioproject",
        docs=docs,
        file_stem="bp",
    )


def test_current_bs_mapping_accepts_pydantic_schema_docs(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-MAPPING-02-bs: schema.BioSample ↔ bs mapping drift detection.

    bs mapping is the most attribute-heavy of all data types, so this test
    has the highest drift detection probability.
    """
    docs = [make_bs_doc(f"SAMD{i:08d}") for i in range(1, 6)]
    _run_rehearsal(
        config=integration_config,
        suffix=rehearsal_date_suffix,
        tmp_path=tmp_path,
        logical_index="biosample",
        docs=docs,
        file_stem="bs",
    )


@pytest.mark.parametrize("logical_index", SRA_INDEXES)
def test_current_sra_mapping_accepts_pydantic_schema_docs(
    logical_index: SraIndexType,
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-MAPPING-02-sra: schema.SRA ↔ sra-* mapping drift, per logical index."""
    prefix = _SRA_PREFIX[logical_index]
    docs = [make_sra_doc(f"{prefix}{i:06d}", logical_index) for i in range(1, 6)]
    _run_rehearsal(
        config=integration_config,
        suffix=rehearsal_date_suffix,
        tmp_path=tmp_path,
        logical_index=logical_index,
        docs=docs,
        file_stem=f"sra_{logical_index}",
    )


@pytest.mark.parametrize("logical_index", JGA_INDEXES)
def test_current_jga_mapping_accepts_pydantic_schema_docs(
    logical_index: JgaIndexType,
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-MAPPING-02-jga: schema.JGA ↔ jga-* mapping drift, per logical index."""
    prefix = _JGA_PREFIX[logical_index]
    docs = [make_jga_doc(f"{prefix}{i:06d}", logical_index) for i in range(1, 6)]
    _run_rehearsal(
        config=integration_config,
        suffix=rehearsal_date_suffix,
        tmp_path=tmp_path,
        logical_index=logical_index,
        docs=docs,
        file_stem=f"jga_{logical_index}",
    )


def test_current_gea_mapping_accepts_pydantic_schema_docs(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-MAPPING-02-gea: schema.GEA ↔ gea mapping drift detection."""
    docs = [make_gea_doc(f"AGDD{i:06d}") for i in range(1, 6)]
    _run_rehearsal(
        config=integration_config,
        suffix=rehearsal_date_suffix,
        tmp_path=tmp_path,
        logical_index="gea",
        docs=docs,
        file_stem="gea",
    )


def test_current_mtb_mapping_accepts_pydantic_schema_docs(
    integration_config: Config,
    integration_es_client: Elasticsearch,
    rehearsal_date_suffix: str,
    cleanup_rehearsal_indexes: None,
    tmp_path: Path,
) -> None:
    """IT-MAPPING-02-mtb: schema.MetaboBank ↔ metabobank mapping drift detection."""
    docs = [make_mtb_doc(f"MTBKS{i:03d}") for i in range(1, 6)]
    _run_rehearsal(
        config=integration_config,
        suffix=rehearsal_date_suffix,
        tmp_path=tmp_path,
        logical_index="metabobank",
        docs=docs,
        file_stem="mtb",
    )
