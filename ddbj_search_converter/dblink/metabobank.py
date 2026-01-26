"""
MetaboBank の IDF/SDRF ファイルから関連を抽出し、DBLink DB に挿入する。

入力:
- METABOBANK_BASE_PATH 配下の MTBKS* ディレクトリ
    - {MTBKS{N}}/{MTBKS{N}}.idf.txt (BioProject ID)
    - {MTBKS{N}}/{MTBKS{N}}.sdrf.txt (BioSample IDs)
- Preserve ファイル (const_dir/metabobank/)
    - mtb_id_bioproject_preserve.tsv
    - mtb_id_biosample_preserve.tsv

出力:
- metabobank -> bioproject (IDF の Comment[BioProject] + preserve から)
- metabobank -> biosample (SDRF の Comment[BioSample] + preserve から)
"""
from pathlib import Path
from typing import Iterator, Optional, Set, Tuple

from ddbj_search_converter.config import (METABOBANK_BASE_PATH,
                                          MTB_BP_PRESERVED_REL_PATH,
                                          MTB_BS_PRESERVED_REL_PATH, Config,
                                          get_config)
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.idf_sdrf import (parse_idf_file,
                                                   parse_sdrf_file)
from ddbj_search_converter.dblink.utils import (filter_pairs_by_blacklist,
                                                load_blacklist)
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger


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


def load_preserve_file(config: Config) -> Tuple[IdPairs, IdPairs]:
    """Preserve ファイルから MetaboBank -> BP/BS 関連を読み込む。"""
    mtb_to_bp: IdPairs = set()
    mtb_to_bs: IdPairs = set()

    bp_preserve_path = config.const_dir.joinpath(MTB_BP_PRESERVED_REL_PATH)
    bs_preserve_path = config.const_dir.joinpath(MTB_BS_PRESERVED_REL_PATH)

    if not bp_preserve_path.exists():
        raise FileNotFoundError(
            f"MetaboBank BP preserve file not found: {bp_preserve_path}"
        )
    with open(bp_preserve_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                mtb_to_bp.add((parts[0], parts[1]))
    log_info(f"loaded {len(mtb_to_bp)} MetaboBank -> BioProject from preserve",
             file=str(bp_preserve_path))

    if not bs_preserve_path.exists():
        raise FileNotFoundError(
            f"MetaboBank BS preserve file not found: {bs_preserve_path}"
        )
    with open(bs_preserve_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                mtb_to_bs.add((parts[0], parts[1]))
    log_info(f"loaded {len(mtb_to_bs)} MetaboBank -> BioSample from preserve",
             file=str(bs_preserve_path))

    return mtb_to_bp, mtb_to_bs


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
        bp_blacklist, bs_blacklist = load_blacklist(config)

        mtb_to_bp: IdPairs = set()
        mtb_to_bs: IdPairs = set()

        # IDF/SDRF ファイルから抽出
        dir_count = 0
        for mtb_dir in iterate_metabobank_dirs(METABOBANK_BASE_PATH):
            mtb_id, bp_id, bs_ids = process_metabobank_dir(mtb_dir)
            dir_count += 1

            if bp_id:
                mtb_to_bp.add((mtb_id, bp_id))

            for bs_id in bs_ids:
                mtb_to_bs.add((mtb_id, bs_id))

        log_info(f"processed {dir_count} MetaboBank directories")
        log_info(f"extracted {len(mtb_to_bp)} MetaboBank -> BioProject from IDF")
        log_info(f"extracted {len(mtb_to_bs)} MetaboBank -> BioSample from SDRF")

        # Preserve ファイルから追加
        preserve_bp, preserve_bs = load_preserve_file(config)
        mtb_to_bp.update(preserve_bp)
        mtb_to_bs.update(preserve_bs)

        log_info(f"total {len(mtb_to_bp)} MetaboBank -> BioProject relations (after preserve)")
        log_info(f"total {len(mtb_to_bs)} MetaboBank -> BioSample relations (after preserve)")

        # Blacklist 適用
        mtb_to_bp = filter_pairs_by_blacklist(mtb_to_bp, bp_blacklist, "right")
        mtb_to_bs = filter_pairs_by_blacklist(mtb_to_bs, bs_blacklist, "right")

        if mtb_to_bp:
            load_to_db(config, mtb_to_bp, "metabobank", "bioproject")

        if mtb_to_bs:
            load_to_db(config, mtb_to_bs, "metabobank", "biosample")


if __name__ == "__main__":
    main()
