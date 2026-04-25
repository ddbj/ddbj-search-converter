"""Integration: dblink DuckDB structural invariants.

Asserts properties that the half-edge ``dbxref`` schema must satisfy regardless
of pipeline run timing. A failure here would mean ``finalize_dblink_db`` lost
the symmetry that production search depends on.
"""

from pathlib import Path

import duckdb


def test_dblink_dbxref_is_symmetric_per_type_pair(integration_dblink_db_path: Path) -> None:
    """IT-INVARIANT-01: 半辺化 dbxref で各 (A, B) ペアに対して count(A→B) == count(B→A)。

    回帰元: ``dblink/db.py::finalize_dblink_db`` が ``UNION ALL`` で半辺を 2 倍化する設計
    (`f279f2f`)。対称性が崩れると undirected lookup が片側だけ取りこぼす。
    """
    with duckdb.connect(str(integration_dblink_db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession_type, linked_type, COUNT(*) AS cnt
            FROM dbxref
            GROUP BY accession_type, linked_type
            """,
        ).fetchall()

    counts: dict[tuple[str, str], int] = {(s, d): c for s, d, c in rows}
    assert counts, "dbxref is empty"

    asymmetries: list[tuple[str, str, int, int]] = []
    seen: set[tuple[str, str]] = set()
    for src, dst in counts:
        if src == dst:
            continue  # self-loop は対称性検査外
        pair: tuple[str, str] = (src, dst) if src < dst else (dst, src)
        if pair in seen:
            continue
        seen.add(pair)
        a, b = pair
        ab = counts.get((a, b), 0)
        ba = counts.get((b, a), 0)
        if ab != ba:
            asymmetries.append((a, b, ab, ba))

    assert not asymmetries, f"dbxref half-edge asymmetries: {asymmetries}"


def test_dblink_dbxref_has_no_null_ids(integration_dblink_db_path: Path) -> None:
    """IT-INVARIANT-01b: dbxref の accession / linked_accession に NULL がない。

    NULL があると undirected lookup で取り違えが起きる。回帰元: input 正規化
    (`normalize_edge`、`221f8c3` 周辺) が canonical 形式を保証する設計。
    """
    with duckdb.connect(str(integration_dblink_db_path), read_only=True) as conn:
        null_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM dbxref
            WHERE accession IS NULL
               OR linked_accession IS NULL
               OR accession_type IS NULL
               OR linked_type IS NULL
            """,
        ).fetchone()
    assert null_count is not None
    assert null_count[0] == 0, f"dbxref has {null_count[0]} rows with NULL fields"
