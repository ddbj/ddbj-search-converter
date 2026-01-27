#!/usr/bin/env python3
"""6 type の XML を全て持つ submission を DuckDB + ファイルシステムから探す書き捨てスクリプト。

Usage (本番サーバで実行):
    python scripts/find_6type_submissions.py

出力: fetch_test_fixtures.sh にハードコードできる submission リスト
"""

from pathlib import Path

import duckdb

SRA_DB_PATH = Path("/home/w3ddbjld/const/sra/sra_accessions.duckdb")
XML_BASE = Path("/usr/local/resources/dra/fastq")
XML_TYPES = ["submission", "study", "experiment", "run", "sample", "analysis"]
TARGET_COUNT = 10


def find_6type_submissions_from_db(db_path: Path) -> list[str]:
    """DuckDB から 6 type 全てを持つ submission を検索する。"""
    conn = duckdb.connect(str(db_path), read_only=True)
    rows = conn.execute("""
        SELECT Submission
        FROM accessions
        WHERE Submission IS NOT NULL
        GROUP BY Submission
        HAVING COUNT(DISTINCT Type) = 6
        ORDER BY Submission
    """).fetchall()
    conn.close()
    return [r[0] for r in rows]


def has_all_xml_files(submission: str) -> bool:
    """submission ディレクトリに 6 type 全ての XML が存在するか確認する。"""
    prefix = submission[:6]
    sub_dir = XML_BASE / prefix / submission
    return all(
        (sub_dir / f"{submission}.{xml_type}.xml").exists()
        for xml_type in XML_TYPES
    )


def main() -> None:
    print(f"=== DuckDB: {SRA_DB_PATH} ===")
    if not SRA_DB_PATH.exists():
        print(f"ERROR: DB ファイルが見つかりません: {SRA_DB_PATH}")
        return

    print("6 type 完備の submission を DuckDB から検索中...")
    all_subs = find_6type_submissions_from_db(SRA_DB_PATH)
    print(f"  DuckDB 上で 6 type 完備: {len(all_subs)} 件")

    # DRA / SRA / ERA に分類
    prefixes = {"DRA": [], "SRA": [], "ERA": []}
    for sub in all_subs:
        for pfx in prefixes:
            if sub.startswith(pfx):
                prefixes[pfx].append(sub)
                break

    for pfx, subs in prefixes.items():
        print(f"  {pfx}: {len(subs)} 件 (DuckDB)")

    # XML ファイル存在確認
    print()
    print("=== XML ファイル存在確認 ===")
    results: dict[str, list[str]] = {"DRA": [], "SRA": [], "ERA": []}

    for pfx in ["DRA", "SRA", "ERA"]:
        subs = prefixes[pfx]
        found = 0
        checked = 0
        for sub in subs:
            if found >= TARGET_COUNT:
                break
            checked += 1
            if has_all_xml_files(sub):
                results[pfx].append(sub)
                found += 1
            if checked % 100 == 0:
                print(f"  {pfx}: checked {checked}, found {found}...")

        print(f"  {pfx}: {found} 件 (XML 6 type 完備, {checked} 件チェック)")

    # 結果出力
    print()
    print("=" * 60)
    print("=== fetch_test_fixtures.sh にハードコードする submission ===")
    print("=" * 60)
    for pfx in ["DRA", "SRA", "ERA"]:
        subs = results[pfx]
        print(f"\n# {pfx} ({len(subs)} 件)")
        for sub in subs:
            print(f"  {sub}")

    # bash 配列形式でも出力
    print()
    print("=== bash 配列形式 ===")
    for pfx in ["DRA", "SRA", "ERA"]:
        subs = results[pfx]
        items = " ".join(f'"{s}"' for s in subs)
        print(f'{pfx}_SUBS=({items})')

    # ALL_SUBMISSIONS も出力
    all_results = results["DRA"] + results["SRA"] + results["ERA"]
    items = " ".join(f'"{s}"' for s in all_results)
    print(f'ALL_SUBMISSIONS=({items})')


if __name__ == "__main__":
    main()
