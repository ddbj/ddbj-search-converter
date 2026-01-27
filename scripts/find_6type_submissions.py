#!/usr/bin/env python3
"""fixture 用 submission を探す書き捨てスクリプト。

Usage (本番サーバで実行):
    python scripts/find_6type_submissions.py

戦略:
  DRA/SRA: DuckDB 6 type の先頭 10 件
  ERA:     DuckDB 5 type を先頭から取り、type の OR (和集合) が 6 type になるまで集める
"""

from pathlib import Path

import duckdb

SRA_DB_PATH = Path("/home/w3ddbjld/const/sra/sra_accessions.duckdb")
ALL_TYPES = {"SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"}
TARGET_COUNT = 10


def query_submissions_with_types(
    db_path: Path, prefix: str, min_types: int,
) -> list[tuple[str, set[str]]]:
    """DuckDB から (submission, types set) を type 数降順で取得。"""
    conn = duckdb.connect(str(db_path), read_only=True)
    rows = conn.execute(
        """
        SELECT Submission, LIST(DISTINCT Type) as types
        FROM accessions
        WHERE Submission IS NOT NULL AND Submission LIKE ?
        GROUP BY Submission
        HAVING COUNT(DISTINCT Type) >= ?
        ORDER BY COUNT(DISTINCT Type) DESC, Submission
        """,
        [f"{prefix}%", min_types],
    ).fetchall()
    conn.close()
    return [(r[0], set(r[1])) for r in rows]


def main() -> None:
    if not SRA_DB_PATH.exists():
        print(f"ERROR: {SRA_DB_PATH} が見つかりません")
        return

    final: dict[str, list[str]] = {}

    # --- DRA / SRA: 6 type の先頭 10 件 ---
    for pfx in ["DRA", "SRA"]:
        subs = query_submissions_with_types(SRA_DB_PATH, pfx, 6)
        picked = [s for s, _ in subs[:TARGET_COUNT]]
        print(f"{pfx}: {len(subs)} 件 (6 type) → {len(picked)} 件選択")
        final[pfx] = picked

    # --- ERA: 5 type 以上を先頭から集め、OR で 6 type カバーするまで ---
    era_subs = query_submissions_with_types(SRA_DB_PATH, "ERA", 5)
    print(f"ERA: {len(era_subs)} 件 (5+ type)")

    covered: set[str] = set()
    picked_era: list[str] = []
    for sub, types in era_subs:
        picked_era.append(sub)
        covered |= types
        print(f"  {sub}: {sorted(types)} → covered={sorted(covered)}")
        if len(covered) >= 6 and len(picked_era) >= TARGET_COUNT:
            break

    # カバーできた後も 10 件まで埋める
    if len(picked_era) < TARGET_COUNT:
        for sub, _ in era_subs[len(picked_era):]:
            picked_era.append(sub)
            if len(picked_era) >= TARGET_COUNT:
                break

    missing = ALL_TYPES - covered
    if missing:
        print(f"  WARNING: カバーできなかった type: {sorted(missing)}")
    else:
        print(f"  OK: 全 6 type カバー ({len(picked_era)} 件)")

    final["ERA"] = picked_era

    # --- 結果 ---
    print()
    print("=== bash ===")
    for pfx in ["DRA", "SRA", "ERA"]:
        items = " ".join(f'"{s}"' for s in final[pfx])
        print(f'{pfx}_SUBS=({items})')
    all_subs = final["DRA"] + final["SRA"] + final["ERA"]
    print(f'ALL_SUBMISSIONS=({" ".join(f"{chr(34)}{s}{chr(34)}" for s in all_subs)})')


if __name__ == "__main__":
    main()
