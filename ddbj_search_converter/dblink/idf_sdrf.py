"""
IDF/SDRF ファイルのパース共通ユーティリティ。
GEA と MetaboBank で使用する。
"""

from pathlib import Path


def parse_idf_file(idf_path: Path) -> str | None:
    """IDF ファイルから BioProject ID を抽出する。"""
    with idf_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if line.startswith("Comment[BioProject]"):
                parts = line.split("\t")
                if len(parts) >= 2 and parts[1]:
                    return parts[1].strip()
    return None


def parse_sdrf_file(sdrf_path: Path) -> set[str]:
    """SDRF ファイルから BioSample ID を抽出する。"""
    result: set[str] = set()

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


def process_idf_sdrf_dir(dir_path: Path) -> tuple[str, str | None, set[str]]:
    """
    IDF/SDRF を含むディレクトリを処理し、ID, BioProject, BioSample IDs を返す。

    Args:
        dir_path: IDF/SDRF ファイルを含むディレクトリ

    Returns:
        (entry_id, bp_id, bs_ids): エントリ ID、BioProject ID、BioSample ID セット
    """
    entry_id = dir_path.name

    idf_files = list(dir_path.glob("*.idf.txt"))
    bp_id: str | None = None
    if idf_files:
        bp_id = parse_idf_file(idf_files[0])

    sdrf_files = list(dir_path.glob("*.sdrf.txt"))
    bs_ids: set[str] = set()
    if sdrf_files:
        bs_ids = parse_sdrf_file(sdrf_files[0])

    return entry_id, bp_id, bs_ids
