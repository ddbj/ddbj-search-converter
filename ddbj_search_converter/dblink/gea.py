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
from typing import Iterator

from ddbj_search_converter.config import GEA_BASE_PATH, get_config
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.idf_sdrf import process_idf_sdrf_dir
from ddbj_search_converter.dblink.utils import (filter_pairs_by_blacklist,
                                                load_blacklist)
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  run_logger)
from ddbj_search_converter.logging.schema import DebugCategory


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


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist, bs_blacklist = load_blacklist(config)

        gea_to_bp: IdPairs = set()
        gea_to_bs: IdPairs = set()

        dir_count = 0
        for gea_dir in iterate_gea_dirs(GEA_BASE_PATH):
            gea_id, bp_id, bs_ids = process_idf_sdrf_dir(gea_dir)
            dir_count += 1

            if bp_id:
                if is_valid_accession(bp_id, "bioproject"):
                    gea_to_bp.add((gea_id, bp_id))
                else:
                    log_debug(
                        f"skipping invalid bioproject: {bp_id}",
                        accession=bp_id,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )

            for bs_id in bs_ids:
                if is_valid_accession(bs_id, "biosample"):
                    gea_to_bs.add((gea_id, bs_id))
                else:
                    log_debug(
                        f"skipping invalid biosample: {bs_id}",
                        accession=bs_id,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )

        log_info(f"processed {dir_count} GEA directories")
        log_info(f"extracted {len(gea_to_bp)} GEA -> BioProject relations")
        log_info(f"extracted {len(gea_to_bs)} GEA -> BioSample relations")

        # Blacklist 適用
        gea_to_bp = filter_pairs_by_blacklist(gea_to_bp, bp_blacklist, "right")
        gea_to_bs = filter_pairs_by_blacklist(gea_to_bs, bs_blacklist, "right")

        if gea_to_bp:
            load_to_db(config, gea_to_bp, "gea", "bioproject")

        if gea_to_bs:
            load_to_db(config, gea_to_bs, "gea", "biosample")


if __name__ == "__main__":
    main()
