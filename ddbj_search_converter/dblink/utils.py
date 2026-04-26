"""DBLink 処理用のユーティリティ関数。"""

from pathlib import Path
from typing import Literal

from ddbj_search_converter.config import (
    BP_BLACKLIST_REL_PATH,
    BS_BLACKLIST_REL_PATH,
    JGA_BLACKLIST_REL_PATH,
    SRA_BLACKLIST_REL_PATH,
    Config,
)
from ddbj_search_converter.dblink.db import IdPairs
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import log_debug, log_info
from ddbj_search_converter.logging.schema import DebugCategory


def _read_blacklist_file(path: Path, label: str) -> set[str]:
    """単一の blacklist ファイルを読み込んで accession の集合を返す。

    - 空行と ``#`` 始まりの行は無視
    - 各行は strip して accession のみ取り出す
    - ファイルが存在しないときは空集合を返し info ログを出す (CLI 直叩きや
      blacklist を持たない fixture でも落ちないようにするため)
    """
    if not path.exists():
        log_info(f"{label} blacklist not found, skipping: {path}", file=str(path))
        return set()

    accessions: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if line and not line.startswith("#"):
                accessions.add(line)
    log_info(f"loaded {len(accessions)} {label} blacklist entries", file=str(path))
    return accessions


def load_blacklist(config: Config) -> tuple[set[str], set[str]]:
    """BioProject/BioSample の blacklist ファイルを読み込む。"""
    bp_blacklist = _read_blacklist_file(config.const_dir.joinpath(BP_BLACKLIST_REL_PATH), "BioProject")
    bs_blacklist = _read_blacklist_file(config.const_dir.joinpath(BS_BLACKLIST_REL_PATH), "BioSample")
    return bp_blacklist, bs_blacklist


def load_sra_blacklist(config: Config) -> set[str]:
    """SRA の blacklist ファイルを読み込む。

    SRA blacklist には Study, Experiment, Run, Sample の accession が含まれる。
    コメント行 (#) は無視する。
    """
    return _read_blacklist_file(config.const_dir.joinpath(SRA_BLACKLIST_REL_PATH), "SRA")


def filter_by_blacklist(
    bs_to_bp: IdPairs,
    bp_blacklist: set[str],
    bs_blacklist: set[str],
) -> IdPairs:
    """BioSample -> BioProject 関連を blacklist でフィルタ。"""
    original_count = len(bs_to_bp)
    filtered = {(bs, bp) for bs, bp in bs_to_bp if bs not in bs_blacklist and bp not in bp_blacklist}
    removed_count = original_count - len(filtered)

    if removed_count > 0:
        log_info(f"removed {removed_count} relations by blacklist")

    return filtered


def filter_pairs_by_blacklist(
    pairs: IdPairs,
    blacklist: set[str],
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
    blacklist: set[str],
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


def load_jga_blacklist(config: Config) -> set[str]:
    """JGA の blacklist ファイルを読み込む。

    JGA blacklist には Study, Dataset, DAC, Policy の accession が含まれる。
    コメント行 (#) は無視する。
    """
    return _read_blacklist_file(config.const_dir.joinpath(JGA_BLACKLIST_REL_PATH), "JGA")


# Type alias (re-exported for convenience)
IdMapping = dict[str, str]  # numeric_id -> accession


def convert_id_if_needed(
    raw_id: str,
    id_type: str,
    id_to_accession: IdMapping,
    file_path: str,
    source: str,
) -> str | None:
    """Convert numeric ID to accession if needed.

    Returns:
        Converted accession, or None if conversion failed.
    """
    if is_valid_accession(raw_id, id_type):  # type: ignore[arg-type]
        return raw_id

    # Try to convert numeric ID to accession
    if raw_id in id_to_accession:
        return id_to_accession[raw_id]

    # Cannot convert - skip
    log_debug(
        f"skipping invalid {id_type}: {raw_id}",
        accession=raw_id,
        file=file_path,
        debug_category=DebugCategory.INVALID_ACCESSION_ID,
        source=source,
    )
    return None
