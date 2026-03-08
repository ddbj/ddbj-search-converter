"""
Livelist ファイルから BioProject/BioSample の status 情報を取得し、
DuckDB キャッシュを構築するモジュール。
"""

import datetime
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

from ddbj_search_converter.config import BP_LIVELIST_BASE_PATH, BS_LIVELIST_BASE_PATH, TODAY, Config
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.status_cache.db import (
    finalize_status_cache_db,
    init_status_cache_db,
    insert_bp_statuses,
    insert_bs_statuses,
)

FILE_STATUS_MAP: dict[str, str] = {
    "public": "live",
    "suppressed": "suppressed",
    "withdrawn": "withdrawn",
}

LivelistKind = Literal["bioproject", "biosample"]


def find_latest_livelist_date(base_path: Path, kind: LivelistKind) -> str | None:
    """TODAY から 180 日遡り、3 ファイル (public/suppressed/withdrawn) が全て存在する最新日付を返す。"""
    for days in range(180):
        check_date = TODAY - datetime.timedelta(days=days)
        date_str = check_date.strftime("%Y%m%d")

        all_exist = True
        for status in FILE_STATUS_MAP:
            path = base_path.joinpath(f"{date_str}.{kind}.ddbj.{status}.txt")
            if not path.exists():
                all_exist = False
                break

        if all_exist:
            return date_str

    return None


def _parse_livelist_file(file_path: Path, mapped_status: str) -> Iterator[tuple[str, str]]:
    """TSV ファイルを読みヘッダをスキップし、(accession, mapped_status) を yield する。"""
    with file_path.open("r", encoding="utf-8") as f:
        header = True
        for line in f:
            if header:
                header = False
                continue
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split("\t")
            if len(parts) >= 1:
                accession = parts[0]
                yield (accession, mapped_status)


def _iter_statuses(base_path: Path, kind: LivelistKind, date_str: str) -> Iterator[tuple[str, str]]:
    """public/suppressed/withdrawn 3 ファイルを順に parse して yield する。"""
    for file_status, mapped_status in FILE_STATUS_MAP.items():
        file_path = base_path.joinpath(f"{date_str}.{kind}.ddbj.{file_status}.txt")
        yield from _parse_livelist_file(file_path, mapped_status)


def build_status_cache(config: Config) -> None:
    log_info("initializing status cache db")
    init_status_cache_db(config)

    # BioProject
    bp_date = find_latest_livelist_date(BP_LIVELIST_BASE_PATH, "bioproject")
    if bp_date is None:
        log_info("no bioproject livelist files found, skipping bp statuses")
    else:
        log_info(f"found bioproject livelist date: {bp_date}")
        bp_rows = _iter_statuses(BP_LIVELIST_BASE_PATH, "bioproject", bp_date)
        bp_count = insert_bp_statuses(config, bp_rows)
        log_info(f"inserted {bp_count} bp_status rows")

    # BioSample
    bs_date = find_latest_livelist_date(BS_LIVELIST_BASE_PATH, "biosample")
    if bs_date is None:
        log_info("no biosample livelist files found, skipping bs statuses")
    else:
        log_info(f"found biosample livelist date: {bs_date}")
        bs_rows = _iter_statuses(BS_LIVELIST_BASE_PATH, "biosample", bs_date)
        bs_count = insert_bs_statuses(config, bs_rows)
        log_info(f"inserted {bs_count} bs_status rows")

    log_info("finalizing status cache db")
    finalize_status_cache_db(config)
    log_info("status cache build completed")
