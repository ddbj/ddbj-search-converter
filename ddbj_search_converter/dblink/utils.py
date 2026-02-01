"""DBLink 処理用のユーティリティ関数。"""
from typing import Literal, Set, Tuple

from ddbj_search_converter.config import (BP_BLACKLIST_REL_PATH,
                                          BS_BLACKLIST_REL_PATH,
                                          SRA_BLACKLIST_REL_PATH, Config)
from ddbj_search_converter.dblink.db import IdPairs
from ddbj_search_converter.logging.logger import log_info


def load_blacklist(config: Config) -> Tuple[Set[str], Set[str]]:
    """BioProject/BioSample の blacklist ファイルを読み込む。"""
    bp_blacklist_path = config.const_dir.joinpath(BP_BLACKLIST_REL_PATH)
    bs_blacklist_path = config.const_dir.joinpath(BS_BLACKLIST_REL_PATH)

    bp_blacklist: Set[str] = set()
    bs_blacklist: Set[str] = set()

    if bp_blacklist_path.exists():
        bp_blacklist = set(bp_blacklist_path.read_text(encoding="utf-8").strip().split("\n"))
        bp_blacklist.discard("")
        log_info(f"loaded {len(bp_blacklist)} BioProject blacklist entries",
                 file=str(bp_blacklist_path))
    else:
        log_info(f"bioproject blacklist not found, skipping: {bp_blacklist_path}",
                 file=str(bp_blacklist_path))

    if bs_blacklist_path.exists():
        bs_blacklist = set(bs_blacklist_path.read_text(encoding="utf-8").strip().split("\n"))
        bs_blacklist.discard("")
        log_info(f"loaded {len(bs_blacklist)} BioSample blacklist entries",
                 file=str(bs_blacklist_path))
    else:
        log_info(f"biosample blacklist not found, skipping: {bs_blacklist_path}",
                 file=str(bs_blacklist_path))

    return bp_blacklist, bs_blacklist


def load_sra_blacklist(config: Config) -> Set[str]:
    """SRA の blacklist ファイルを読み込む。

    SRA blacklist には Study, Experiment, Run, Sample の accession が含まれる。
    コメント行 (#) は無視する。
    """
    sra_blacklist_path = config.const_dir.joinpath(SRA_BLACKLIST_REL_PATH)

    sra_blacklist: Set[str] = set()

    if sra_blacklist_path.exists():
        with sra_blacklist_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    sra_blacklist.add(line)
        log_info(f"loaded {len(sra_blacklist)} SRA blacklist entries",
                 file=str(sra_blacklist_path))
    else:
        log_info(f"sra blacklist not found, skipping: {sra_blacklist_path}",
                 file=str(sra_blacklist_path))

    return sra_blacklist


def filter_by_blacklist(
    bs_to_bp: IdPairs,
    bp_blacklist: Set[str],
    bs_blacklist: Set[str],
) -> IdPairs:
    """BioSample -> BioProject 関連を blacklist でフィルタ。"""
    original_count = len(bs_to_bp)
    filtered = {
        (bs, bp) for bs, bp in bs_to_bp
        if bs not in bs_blacklist and bp not in bp_blacklist
    }
    removed_count = original_count - len(filtered)

    if removed_count > 0:
        log_info(f"removed {removed_count} relations by blacklist")

    return filtered


def filter_pairs_by_blacklist(
    pairs: IdPairs,
    blacklist: Set[str],
    position: Literal["left", "right", "both"],
) -> IdPairs:
    """blacklist でペアをフィルタ。

    Args:
        pairs: フィルタ対象のペア
        blacklist: 除外する ID セット
        position: フィルタ位置
            - "left": 左側のみチェック
            - "right": 右側のみチェック
            - "both": 両側をチェック
    """
    original_count = len(pairs)

    if position == "left":
        filtered = {(a, b) for a, b in pairs if a not in blacklist}
    elif position == "right":
        filtered = {(a, b) for a, b in pairs if b not in blacklist}
    else:  # both
        filtered = {(a, b) for a, b in pairs if a not in blacklist and b not in blacklist}

    removed_count = original_count - len(filtered)
    if removed_count > 0:
        log_info(f"removed {removed_count} relations by blacklist ({position})")

    return filtered


def filter_sra_pairs_by_blacklist(
    pairs: IdPairs,
    blacklist: Set[str],
) -> IdPairs:
    """SRA 関連を blacklist でフィルタ。片側でも含まれていたら除外。"""
    if not blacklist:
        return pairs

    original_count = len(pairs)
    filtered = {(a, b) for a, b in pairs if a not in blacklist and b not in blacklist}
    removed_count = original_count - len(filtered)

    if removed_count > 0:
        log_info(f"removed {removed_count} sra relations by blacklist")

    return filtered
