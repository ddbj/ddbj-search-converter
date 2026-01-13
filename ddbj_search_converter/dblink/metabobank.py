"""
MetaboBank の IDF/SDRF ファイルから関連を抽出し、DBLink DB に挿入する。

入力:
- METABOBANK_BASE_PATH 配下の MTBKS* ディレクトリ
    - {MTBKS{N}}/{MTBKS{N}}.idf.txt (BioProject ID)
    - {MTBKS{N}}/{MTBKS{N}}.sdrf.txt (BioSample IDs)

出力:
- metabobank -> bioproject (IDF の Comment[BioProject] から)
- metabobank -> biosample (SDRF の Comment[BioSample] カラムから)
"""
from pathlib import Path
from typing import Iterator, Optional, Set, Tuple

from ddbj_search_converter.config import METABOBANK_BASE_PATH, get_config
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.idf_sdrf import (parse_idf_file,
                                                   parse_sdrf_file)
from ddbj_search_converter.logging.logger import log_info, run_logger


def iterate_metabobank_dirs(base_path: Path) -> Iterator[Path]:
    """
    MetaboBank study ディレクトリを iterate する。
    GEA と異なり 1 層構造 (MTBKS* が直下に並ぶ)。
    """
    if not base_path.exists():
        return

    for mtb_dir in sorted(base_path.iterdir()):
        if mtb_dir.is_dir() and mtb_dir.name.startswith("MTBKS"):
            yield mtb_dir


def process_metabobank_dir(mtb_dir: Path) -> Tuple[str, Optional[str], Set[str]]:
    """
    1 つの MetaboBank ディレクトリを処理し、MetaboBank ID, BioProject ID, BioSample IDs を返す。
    IDF/SDRF が存在しない場合は None/空 set を返す。
    """
    mtb_id = mtb_dir.name

    # IDF ファイルを探す
    idf_files = list(mtb_dir.glob("*.idf.txt"))
    bp_id: Optional[str] = None
    if idf_files:
        bp_id = parse_idf_file(idf_files[0])

    # SDRF ファイルを探す
    sdrf_files = list(mtb_dir.glob("*.sdrf.txt"))
    bs_ids: Set[str] = set()
    if sdrf_files:
        bs_ids = parse_sdrf_file(sdrf_files[0])

    return mtb_id, bp_id, bs_ids


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        mtb_to_bp: IdPairs = set()
        mtb_to_bs: IdPairs = set()

        dir_count = 0
        for mtb_dir in iterate_metabobank_dirs(METABOBANK_BASE_PATH):
            mtb_id, bp_id, bs_ids = process_metabobank_dir(mtb_dir)
            dir_count += 1

            if bp_id:
                mtb_to_bp.add((mtb_id, bp_id))

            for bs_id in bs_ids:
                mtb_to_bs.add((mtb_id, bs_id))

        log_info(f"processed {dir_count} MetaboBank directories")
        log_info(f"extracted {len(mtb_to_bp)} MetaboBank -> BioProject relations")
        log_info(f"extracted {len(mtb_to_bs)} MetaboBank -> BioSample relations")

        if mtb_to_bp:
            load_to_db(config, mtb_to_bp, "metabobank", "bioproject")

        if mtb_to_bs:
            load_to_db(config, mtb_to_bs, "metabobank", "biosample")


if __name__ == "__main__":
    main()
