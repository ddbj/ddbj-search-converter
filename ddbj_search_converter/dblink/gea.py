"""
GEA (Gene Expression Archive) の IDF/SDRF ファイルから関連を抽出し、DBLink DB に挿入する。

入力:
- GEA_BASE_PATH 配下の E-GEAD-* ディレクトリ
    - {E-GEAD-NNN}/{E-GEAD-NNN}.idf.txt (BioProject ID)
    - {E-GEAD-NNN}/{E-GEAD-NNN}.sdrf.txt (BioSample IDs)

出力:
- gea -> bioproject (IDF の Comment[BioProject] から)
- gea -> biosample (SDRF の Comment[BioSample] カラムから)
"""
from pathlib import Path
from typing import Iterator, Optional, Set, Tuple

from ddbj_search_converter.config import GEA_BASE_PATH, get_config
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.logging.logger import log_info, run_logger


def iterate_gea_dirs(base_path: Path) -> Iterator[Path]:
    """
    GEA 実験ディレクトリを iterate する。
    E-GEAD-000/, E-GEAD-1000/ などのサブディレクトリ配下の E-GEAD-NNN/ を走査する。
    """
    if not base_path.exists():
        return

    for prefix_dir in sorted(base_path.iterdir()):
        if not prefix_dir.is_dir() or not prefix_dir.name.startswith("E-GEAD-"):
            continue
        for gea_dir in sorted(prefix_dir.iterdir()):
            if gea_dir.is_dir() and gea_dir.name.startswith("E-GEAD-"):
                yield gea_dir


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


def process_gea_dir(gea_dir: Path) -> Tuple[str, Optional[str], Set[str]]:
    """
    1 つの GEA ディレクトリを処理し、GEA ID, BioProject ID, BioSample IDs を返す。
    IDF/SDRF が存在しない場合は None/空 set を返す。
    """
    gea_id = gea_dir.name

    # IDF ファイルを探す
    idf_files = list(gea_dir.glob("*.idf.txt"))
    bp_id: Optional[str] = None
    if idf_files:
        bp_id = parse_idf_file(idf_files[0])

    # SDRF ファイルを探す
    sdrf_files = list(gea_dir.glob("*.sdrf.txt"))
    bs_ids: Set[str] = set()
    if sdrf_files:
        bs_ids = parse_sdrf_file(sdrf_files[0])

    return gea_id, bp_id, bs_ids


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        gea_to_bp: IdPairs = set()
        gea_to_bs: IdPairs = set()

        dir_count = 0
        for gea_dir in iterate_gea_dirs(GEA_BASE_PATH):
            gea_id, bp_id, bs_ids = process_gea_dir(gea_dir)
            dir_count += 1

            if bp_id:
                gea_to_bp.add((gea_id, bp_id))

            for bs_id in bs_ids:
                gea_to_bs.add((gea_id, bs_id))

        log_info(f"processed {dir_count} GEA directories")
        log_info(f"extracted {len(gea_to_bp)} GEA -> BioProject relations")
        log_info(f"extracted {len(gea_to_bs)} GEA -> BioSample relations")

        if gea_to_bp:
            load_to_db(config, gea_to_bp, "gea", "bioproject")

        if gea_to_bs:
            load_to_db(config, gea_to_bs, "gea", "biosample")


if __name__ == "__main__":
    main()
