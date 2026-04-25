"""Integration: JSONL ↔ ES count consistency.

Asserts that the ES docs_count for each logical index is at least as large as
the JSONL row count for that logical (in the latest dated generation). The
ES side is allowed to be larger because of sameAs alias documents; it must
never be smaller, since that would mean some converted JSONL row failed to
land in ES.
"""

import json
from collections import defaultdict
from pathlib import Path

import pytest

from elasticsearch import Elasticsearch


def _latest_dated_subdir(parent: Path) -> Path | None:
    if not parent.exists():
        return None
    dated = sorted(p for p in parent.iterdir() if p.is_dir() and p.name.isdigit())
    return dated[-1] if dated else None


def _count_jsonl_rows_per_logical(jsonl_dir: Path) -> dict[str, int]:
    """Count JSONL rows in ``jsonl_dir`` grouped by ``type`` field (logical index name)."""
    counts: dict[str, int] = defaultdict(int)
    for path in jsonl_dir.rglob("*.jsonl"):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                doc = json.loads(line)
                counts[doc["type"]] += 1
    return dict(counts)


def test_es_docs_count_is_at_least_jsonl_row_count_per_logical(
    integration_jsonl_dir: Path,
    integration_es_client: Elasticsearch,
    staging_es_has_seed_data: None,
) -> None:
    """IT-INVARIANT-03: 各 logical index の ES docs_count >= 最新 dated JSONL の行数。

    blacklist 適用 + sameAs alias で ES 側は (JSONL 行数 − blacklist 件数) + sameAs 件数。
    blacklist が空 (staging では通常 0) なら ES count >= JSONL 行数 が常に成立。
    ES が JSONL より少なければ bulk insert で取りこぼしている事故。
    """
    sub_to_latest: dict[str, Path] = {}
    for sub in ("bioproject", "biosample", "sra", "jga", "gea", "metabobank"):
        latest = _latest_dated_subdir(integration_jsonl_dir / sub / "jsonl")
        if latest is not None:
            sub_to_latest[sub] = latest

    if not sub_to_latest:
        pytest.skip("no JSONL output found under any data type")

    jsonl_counts: dict[str, int] = defaultdict(int)
    for latest_dir in sub_to_latest.values():
        for logical, n in _count_jsonl_rows_per_logical(latest_dir).items():
            jsonl_counts[logical] += n

    assert jsonl_counts, "no logical type observed in JSONL files"

    discrepancies: list[tuple[str, int, int]] = []
    for logical, jcount in jsonl_counts.items():
        es_count = integration_es_client.count(index=logical).body["count"]
        if es_count < jcount:
            discrepancies.append((logical, jcount, es_count))

    assert not discrepancies, f"ES count below JSONL row count: {discrepancies}"
