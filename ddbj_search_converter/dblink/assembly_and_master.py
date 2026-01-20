"""
Assembly/Master 関連を抽出し、DBLink データベースに挿入する。

このモジュールは TSV ファイルを直接出力しない。代わりに、関連を
dblink.tmp.duckdb データベースに書き込む。TSV ファイルは後から
dump_dblink_files CLI コマンドで出力する。

入力:
- assembly_summary_genbank.txt (NCBI FTP からストリーミング)
    - GenBank の Assembly summary ファイル
    - 以下の関連を抽出:
        - insdc-assembly <-> bioproject
        - insdc-assembly <-> biosample
        - insdc-assembly <-> insdc-master
        - insdc-master <-> bioproject (assembly 由来)
        - insdc-master <-> biosample (assembly 由来)
        - biosample <-> bioproject (assembly 由来)
- TRAD organism list ファイル:
    - /lustre9/open/shared_data/trad/wgs/WGS_ORGANISM_LIST.txt
    - /lustre9/open/shared_data/trad/tls/TLS_ORGANISM_LIST.txt
    - /lustre9/open/shared_data/trad/tsa/TSA_ORGANISM_LIST.txt
    - /lustre9/open/shared_data/trad/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt
    - /lustre9/open/shared_data/trad/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt
    - /lustre9/open/shared_data/trad/tpa/tls/TPA_TLS_ORGANISM_LIST.txt
    - 以下の関連を抽出:
        - insdc-master <-> bioproject
        - insdc-master <-> biosample

出力:
- dblink.tmp.duckdb (relation テーブル) に挿入
"""
import httpx

from ddbj_search_converter.config import (ASSEMBLY_SUMMARY_URL, TRAD_BASE_PATH,
                                          get_config)
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.utils import (filter_by_blacklist,
                                                filter_pairs_by_blacklist,
                                                load_blacklist)
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger

TRAD_FILES = [
    TRAD_BASE_PATH.joinpath("wgs/WGS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tls/TLS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tsa/TSA_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/wgs/TPA_WGS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/tsa/TPA_TSA_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/tls/TPA_TLS_ORGANISM_LIST.txt"),
]


def strip_version_suffix(raw_id: str) -> str:
    """assembly_summary 由来の assembly ID 用。version suffix のみ削除。"""
    return raw_id.split(".", 1)[0]


def normalize_master_id(raw_master_id: str) -> str:
    """同一シリーズの master を同じ ID として扱うため、数字を全て 0 に置換。"""
    # バージョン接尾辞を削除 (例: ".1")
    base_id = raw_master_id.split(".", 1)[0]
    # ハイフン以降を削除 (例: "-1")
    base_id = base_id.split("-", 1)[0]
    # 全ての数字を 0 に置換
    return "".join("0" if char.isdigit() else char for char in base_id)


def process_assembly_summary_file(
    assembly_to_bp: IdPairs,
    assembly_to_bs: IdPairs,
    assembly_to_insdc: IdPairs,
    master_to_bp: IdPairs,
    master_to_bs: IdPairs,
    bs_to_bp: IdPairs,
) -> None:
    """cols: [0]=assembly, [1]=bioproject, [2]=biosample, [3]=wgs_master"""
    log_info("streaming assembly_summary_genbank.txt", url=ASSEMBLY_SUMMARY_URL)

    relations = [
        ("asm", "bp", assembly_to_bp),
        ("asm", "bs", assembly_to_bs),
        ("asm", "master", assembly_to_insdc),
        ("master", "bp", master_to_bp),
        ("master", "bs", master_to_bs),
        ("bs", "bp", bs_to_bp),
    ]

    with httpx.Client(follow_redirects=True, timeout=60.0) as client:
        with client.stream("GET", ASSEMBLY_SUMMARY_URL) as response:
            response.raise_for_status()

            for line in response.iter_lines():
                if not line or line.startswith("#"):
                    continue

                cols = line.rstrip("\r\n").split("\t")
                if len(cols) < 4:
                    continue

                values = {
                    "asm": strip_version_suffix(cols[0]),
                    "bp": cols[1],
                    "bs": cols[2],
                    "master": normalize_master_id(cols[3]),
                }

                for left, right, target_set in relations:
                    left_val = values[left]
                    right_val = values[right]
                    if left_val == "na" or right_val == "na":
                        continue

                    # bs_to_bp の場合は SAM/PRJ チェック
                    if target_set is bs_to_bp:
                        if not left_val.startswith("SAM"):
                            log_warn(
                                f"skipping invalid biosample: {left_val}",
                                accession=left_val,
                                file="assembly_summary_genbank.txt",
                            )
                            continue
                        if not right_val.startswith("PRJ"):
                            log_warn(
                                f"skipping invalid bioproject: {right_val}",
                                accession=right_val,
                                file="assembly_summary_genbank.txt",
                            )
                            continue

                    target_set.add((left_val, right_val))


def process_trad_files(
    master_to_bp: IdPairs,
    master_to_bs: IdPairs,
) -> None:
    """cols: [3]=master, [9]=bioproject, [10]=biosample"""
    log_info("processing trad organism list files")

    for path in TRAD_FILES:
        log_info(f"processing file: {path}", file=str(path))

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.startswith((" ", "\t", "-")):
                    continue

                cols = line.rstrip("\r\n").split("\t")
                if len(cols) < 11:
                    continue

                master = normalize_master_id(cols[3])
                bp = cols[9]
                bs = cols[10]

                if bp:
                    master_to_bp.add((master, bp))
                if bs:
                    master_to_bs.add((master, bs))


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist, bs_blacklist = load_blacklist(config)

        assembly_to_bp: IdPairs = set()
        assembly_to_bs: IdPairs = set()
        assembly_to_insdc: IdPairs = set()
        master_to_bp: IdPairs = set()
        master_to_bs: IdPairs = set()
        bs_to_bp: IdPairs = set()

        process_assembly_summary_file(
            assembly_to_bp,
            assembly_to_bs,
            assembly_to_insdc,
            master_to_bp,
            master_to_bs,
            bs_to_bp,
        )
        process_trad_files(master_to_bp, master_to_bs)

        # Apply blacklist filters
        assembly_to_bp = filter_pairs_by_blacklist(assembly_to_bp, bp_blacklist, "right")
        assembly_to_bs = filter_pairs_by_blacklist(assembly_to_bs, bs_blacklist, "right")
        master_to_bp = filter_pairs_by_blacklist(master_to_bp, bp_blacklist, "right")
        master_to_bs = filter_pairs_by_blacklist(master_to_bs, bs_blacklist, "right")
        bs_to_bp = filter_by_blacklist(bs_to_bp, bp_blacklist, bs_blacklist)

        log_info("loading relations into dblink database")

        load_to_db(config, assembly_to_bp, "insdc-assembly", "bioproject")
        load_to_db(config, assembly_to_bs, "insdc-assembly", "biosample")
        load_to_db(config, assembly_to_insdc, "insdc-assembly", "insdc-master")
        load_to_db(config, master_to_bp, "insdc-master", "bioproject")
        load_to_db(config, master_to_bs, "insdc-master", "biosample")
        load_to_db(config, bs_to_bp, "biosample", "bioproject")


if __name__ == "__main__":
    main()
