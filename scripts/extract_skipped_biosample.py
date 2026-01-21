#!/usr/bin/env python3
"""
JSONL ログファイルからスキップされた biosample ID を抽出するスクリプト。

使い方:
    python extract_skipped_biosample.py /path/to/log.jsonl
    python extract_skipped_biosample.py /path/to/logs/*.jsonl

出力形式 (TSV):
    accession<TAB>file1,file2,...
"""
import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Set


def extract_skipped_biosample(log_path: Path) -> Dict[str, Set[str]]:
    """ログファイルからスキップされた biosample ID と由来ファイルを抽出。"""
    skipped: Dict[str, Set[str]] = defaultdict(set)
    with log_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            message = record.get("message", "")
            if "skipping invalid biosample" in message:
                accession = record.get("extra", {}).get("accession")
                file = record.get("extra", {}).get("file", "")
                if accession:
                    skipped[accession].add(file)
    return skipped


def main():
    parser = argparse.ArgumentParser(description="Extract skipped biosample IDs from JSONL log")
    parser.add_argument("log_files", type=Path, nargs="+", help="JSONL log file(s)")
    args = parser.parse_args()

    all_skipped: Dict[str, Set[str]] = defaultdict(set)
    for log_path in args.log_files:
        if not log_path.exists():
            print(f"WARNING: File not found: {log_path}")
            continue
        skipped = extract_skipped_biosample(log_path)
        for accession, files in skipped.items():
            all_skipped[accession].update(files)

    for accession in sorted(all_skipped.keys()):
        files = ",".join(sorted(all_skipped[accession]))
        print(f"{accession}\t{files}")


if __name__ == "__main__":
    main()
