"""古い日付ディレクトリを削除するメンテナンスコマンド。

result_dir 以下の複数の親ディレクトリについて、YYYYMMDD 形式の
サブディレクトリを日付順にソートし、最新 N 個を残して残りを削除する。

``--include-spill`` を指定すると DuckDB の spill ディレクトリ
(``{result_dir}/dblink/duckdb_tmp/{YYYYMMDD}/``) を ``--keep`` を無視して全件削除する。
"""

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

from ddbj_search_converter.config import (
    BP_BASE_DIR_NAME,
    BS_BASE_DIR_NAME,
    DATE_FORMAT,
    DBLINK_DIR_NAME,
    DBLINK_TMP_DIR_NAME,
    GEA_BASE_DIR_NAME,
    JGA_BASE_DIR_NAME,
    JSONL_DIR_NAME,
    LOG_DIR_NAME,
    METABOBANK_BASE_DIR_NAME,
    REGENERATE_DIR_NAME,
    SRA_BASE_DIR_NAME,
    TMP_XML_DIR_NAME,
    Config,
    get_config,
)
from ddbj_search_converter.logging.logger import log_debug, log_info, log_warn, run_logger

DUCKDB_TMP_DIR_NAME = "duckdb_tmp"


def get_cleanup_target_parents(config: Config) -> list[Path]:
    """cleanup 対象の親ディレクトリパスを返す。"""
    r = config.result_dir

    return [
        r.joinpath(LOG_DIR_NAME),
        r.joinpath(BP_BASE_DIR_NAME, TMP_XML_DIR_NAME),
        r.joinpath(BS_BASE_DIR_NAME, TMP_XML_DIR_NAME),
        r.joinpath(BP_BASE_DIR_NAME, JSONL_DIR_NAME),
        r.joinpath(BS_BASE_DIR_NAME, JSONL_DIR_NAME),
        r.joinpath(SRA_BASE_DIR_NAME, JSONL_DIR_NAME),
        r.joinpath(JGA_BASE_DIR_NAME, JSONL_DIR_NAME),
        r.joinpath(GEA_BASE_DIR_NAME, JSONL_DIR_NAME),
        r.joinpath(METABOBANK_BASE_DIR_NAME, JSONL_DIR_NAME),
        r.joinpath(REGENERATE_DIR_NAME),
        r.joinpath(DBLINK_DIR_NAME, DBLINK_TMP_DIR_NAME),
    ]


def find_date_dirs(parent: Path) -> list[tuple[str, Path]]:
    """parent 内の YYYYMMDD ディレクトリを日付降順で返す。

    parent が存在しない場合は空リストを返す。
    YYYYMMDD にパースできないディレクトリや symlink は無視する。
    """
    if not parent.exists():
        return []

    date_dirs: list[tuple[str, Path]] = []
    for entry in parent.iterdir():
        if entry.is_symlink():
            continue
        if not entry.is_dir():
            continue
        # strptime は桁数を厳密にチェックしないため、8桁であることを先に確認
        if len(entry.name) != 8:
            continue
        try:
            datetime.strptime(entry.name, DATE_FORMAT)
        except ValueError:
            continue
        date_dirs.append((entry.name, entry))

    date_dirs.sort(key=lambda x: x[0], reverse=True)

    return date_dirs


def get_spill_dir(config: Config) -> Path:
    """DuckDB spill ディレクトリの親パスを返す (``{result_dir}/dblink/duckdb_tmp``)。"""
    return config.result_dir.joinpath(DBLINK_DIR_NAME, DUCKDB_TMP_DIR_NAME)


def cleanup(config: Config, keep: int, dry_run: bool) -> tuple[list[Path], list[tuple[Path, Exception]]]:
    """古い日付ディレクトリを削除する。

    Returns:
        (removed, failed) のタプル。
        dry_run=True の場合、removed は削除対象候補、failed は空リスト。
    """
    parents = get_cleanup_target_parents(config)
    removed: list[Path] = []
    failed: list[tuple[Path, Exception]] = []

    for parent in parents:
        date_dirs = find_date_dirs(parent)
        if len(date_dirs) <= keep:
            continue

        dirs_to_remove = date_dirs[keep:]
        for _, dir_path in dirs_to_remove:
            if dry_run:
                removed.append(dir_path)
            else:
                try:
                    shutil.rmtree(dir_path)
                    removed.append(dir_path)
                except Exception as e:
                    failed.append((dir_path, e))

    return removed, failed


def cleanup_spill(config: Config, dry_run: bool) -> tuple[list[Path], list[tuple[Path, Exception]]]:
    """DuckDB spill ディレクトリ配下の YYYYMMDD ディレクトリを ``--keep`` 無視で全件削除する。

    spill は finalize 後に意味を持たない一時データなので、保持の必要はない。今日分も
    削除対象に含まれる点に注意 (実行中の pipeline と競合しないよう docs に注意書きあり)。
    """
    parent = get_spill_dir(config)
    date_dirs = find_date_dirs(parent)
    removed: list[Path] = []
    failed: list[tuple[Path, Exception]] = []

    for _, dir_path in date_dirs:
        if dry_run:
            removed.append(dir_path)
        else:
            try:
                shutil.rmtree(dir_path)
                removed.append(dir_path)
            except Exception as e:
                failed.append((dir_path, e))

    return removed, failed


def parse_args(args: list[str]) -> tuple[Config, int, bool, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Clean up old date directories under result_dir.",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=3,
        help="Number of latest date directories to keep per location (default: 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    parser.add_argument(
        "--include-spill",
        action="store_true",
        help=(
            "Also delete all YYYYMMDD directories under "
            "{result_dir}/dblink/duckdb_tmp/ (--keep is ignored for this path)."
        ),
    )

    parsed = parser.parse_args(args)
    if parsed.keep < 1:
        parser.error("--keep must be at least 1")
    config = get_config()

    return config, parsed.keep, parsed.dry_run, parsed.include_spill


def main() -> None:
    config, keep, dry_run, include_spill = parse_args(sys.argv[1:])
    with run_logger(run_name="cleanup_old_results", config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info(
            f"cleanup_old_results: keep={keep}, dry_run={dry_run}, include_spill={include_spill}"
        )

        removed, failed = cleanup(config, keep, dry_run)

        if include_spill:
            spill_removed, spill_failed = cleanup_spill(config, dry_run)
            removed.extend(spill_removed)
            failed.extend(spill_failed)

        for path in removed:
            if dry_run:
                log_info(f"[dry-run] would remove: {path}")
            else:
                log_info(f"removed: {path}")

        for path, error in failed:
            log_warn(f"failed to remove: {path}", error=str(error))

        if failed:
            log_warn("some directories could not be removed")
            sys.exit(1)

        log_info("cleanup_old_results completed")
