#!/usr/bin/env python3
"""
umbrella-bioproject として dblink.db に登録されているが、
BioProject XML に accession として存在しない ID を検出するスクリプト。

使い方:
    python check_missing_umbrella.py --const-dir /home/w3ddbjld/const --result-dir /path/to/result

前提:
    - dblink.db が const_dir/dblink/dblink.duckdb に存在
    - BioProject 分割 XML が result_dir/bioproject/tmp_xml/{YYYYMMDD}/ に存在
"""
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Set

import duckdb


def get_umbrella_ids_from_db(db_path: Path) -> Set[str]:
    """dblink.db から全ての umbrella-bioproject ID を取得。"""
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute("""
            SELECT DISTINCT
                CASE
                    WHEN src_type = 'umbrella-bioproject' THEN src_accession
                    ELSE dst_accession
                END AS umbrella_id
            FROM relation
            WHERE src_type = 'umbrella-bioproject' OR dst_type = 'umbrella-bioproject'
        """).fetchall()
    return {row[0] for row in rows}


def get_accessions_from_xml(xml_path: Path) -> Set[str]:
    """BioProject XML から全ての accession を取得。"""
    accessions: Set[str] = set()
    with xml_path.open("r", encoding="utf-8") as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            tag = elem.tag.split("}")[-1]
            if tag == "ArchiveID":
                accession = elem.attrib.get("accession")
                if accession and accession.startswith("PRJ"):
                    accessions.add(accession)
                elem.clear()
    return accessions


def main():
    parser = argparse.ArgumentParser(description="Check missing umbrella BioProject IDs")
    parser.add_argument("--const-dir", type=Path, default=Path("/home/w3ddbjld/const"))
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--date", type=str, help="Date string (YYYYMMDD) for tmp_xml dir")
    args = parser.parse_args()

    # dblink.db path
    db_path = args.const_dir / "dblink" / "dblink.duckdb"
    if not db_path.exists():
        print(f"ERROR: dblink.db not found: {db_path}")
        return

    # tmp_xml dir (find latest if date not specified)
    tmp_xml_base = args.result_dir / "bioproject" / "tmp_xml"
    if args.date:
        tmp_xml_dir = tmp_xml_base / args.date
    else:
        dirs = sorted(tmp_xml_base.glob("*"), reverse=True)
        if not dirs:
            print(f"ERROR: No tmp_xml directories found in {tmp_xml_base}")
            return
        tmp_xml_dir = dirs[0]
        print(f"Using tmp_xml dir: {tmp_xml_dir}")

    # Get umbrella IDs from dblink.db
    print("Getting umbrella IDs from dblink.db...")
    umbrella_ids = get_umbrella_ids_from_db(db_path)
    print(f"Found {len(umbrella_ids)} umbrella IDs in dblink.db")

    # Get accessions from XML
    print("Getting accessions from BioProject XML...")
    xml_accessions: Set[str] = set()
    xml_files = list(tmp_xml_dir.glob("*.xml"))
    print(f"Found {len(xml_files)} XML files")

    for i, xml_path in enumerate(xml_files, 1):
        accessions = get_accessions_from_xml(xml_path)
        xml_accessions.update(accessions)
        if i % 10 == 0:
            print(f"  Processed {i}/{len(xml_files)} files...")

    print(f"Found {len(xml_accessions)} accessions in XML")

    # Find missing umbrella IDs
    missing = umbrella_ids - xml_accessions
    print(f"\n=== Result ===")
    print(f"Umbrella IDs in dblink.db: {len(umbrella_ids)}")
    print(f"Accessions in XML: {len(xml_accessions)}")
    print(f"Missing umbrella IDs: {len(missing)}")

    if missing:
        print(f"\nMissing umbrella IDs:")
        for acc in sorted(missing):
            print(f"  {acc}")

        # Save to file
        output_path = Path("missing_umbrella_ids.txt")
        with output_path.open("w") as f:
            for acc in sorted(missing):
                f.write(f"{acc}\n")
        print(f"\nSaved to {output_path}")
    else:
        print("\nNo missing umbrella IDs found!")


if __name__ == "__main__":
    main()
