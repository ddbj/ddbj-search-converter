"""
IDF/SDRF ファイルのパース共通ユーティリティ。
GEA と MetaboBank で使用する。
"""
from pathlib import Path
from typing import Optional, Set


def parse_idf_file(idf_path: Path) -> Optional[str]:
    """IDF ファイルから BioProject ID を抽出する。"""
    with idf_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("Comment[BioProject]"):
                parts = line.split("\t")
                if len(parts) >= 2 and parts[1]:
                    return parts[1].strip()
    return None


def parse_sdrf_file(sdrf_path: Path) -> Set[str]:
    """SDRF ファイルから BioSample ID を抽出する。"""
    result: Set[str] = set()

    with sdrf_path.open("r", encoding="utf-8") as f:
        header = f.readline().strip().split("\t")

        # Comment[BioSample] カラムのインデックスを取得
        bs_index = -1
        for i, col in enumerate(header):
            if col == "Comment[BioSample]":
                bs_index = i
                break

        if bs_index < 0:
            return result

        for line in f:
            cols = line.strip().split("\t")
            if bs_index < len(cols) and cols[bs_index]:
                result.add(cols[bs_index].strip())

    return result
