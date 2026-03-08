"""DRA ファイルインデックス構築・クエリモジュール。

DRA ファイルシステムをスキャンして FASTQ ディレクトリと SRA ファイルの
存在情報を DuckDB インデックスとして構築する。
JSONL 生成時にファイルシステムを直接叩かず、このインデックスを参照する。
"""

import shutil
from pathlib import Path

import duckdb

from ddbj_search_converter.config import (
    DRA_BASE_PATH,
    DRA_FILE_INDEX_DB_FILE_NAME,
    TMP_DRA_FILE_INDEX_DB_FILE_NAME,
    Config,
)
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.sra.dra_tar import iter_all_dra_submissions
from ddbj_search_converter.sra.paths import get_sra_tar_dir


def get_dra_file_index_db_path(config: Config) -> Path:
    """DRA ファイルインデックス DB のパスを返す。"""

    return get_sra_tar_dir(config).joinpath(DRA_FILE_INDEX_DB_FILE_NAME)


def _tmp_db_path(config: Config) -> Path:

    return get_sra_tar_dir(config).joinpath(TMP_DRA_FILE_INDEX_DB_FILE_NAME)


def dra_file_index_exists(config: Config) -> bool:
    """DRA ファイルインデックス DB が存在するかを返す。"""

    return get_dra_file_index_db_path(config).exists()


def build_dra_file_index(config: Config) -> None:
    """FS をスキャンして DRA ファイルインデックス DuckDB を構築する。

    FASTQ: 各 submission ディレクトリ内の experiment サブディレクトリを収集
    SRA: DRA_BASE_PATH/sra/ByExp/sra/DRX/ ツリーの .sra ファイルから run を抽出
    """
    tmp_path = _tmp_db_path(config)
    final_path = get_dra_file_index_db_path(config)
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    # 既存の tmp を削除
    tmp_path.unlink(missing_ok=True)

    log_info("building dra file index...")

    with duckdb.connect(str(tmp_path)) as conn:
        conn.execute("CREATE TABLE dra_fastq_dir (submission TEXT NOT NULL, experiment TEXT NOT NULL)")
        conn.execute("CREATE TABLE dra_sra_file (run TEXT NOT NULL)")

        # FASTQ ディレクトリをスキャン
        fastq_count = 0
        sub_count = 0
        for submission in iter_all_dra_submissions(config):
            sub_dir = DRA_BASE_PATH.joinpath("fastq", submission[:6], submission)
            try:
                entries = list(sub_dir.iterdir())
            except FileNotFoundError:
                continue

            experiments = [e.name for e in entries if e.is_dir() and e.name.startswith("DRX")]
            if experiments:
                conn.executemany(
                    "INSERT INTO dra_fastq_dir VALUES (?, ?)",
                    [(submission, exp) for exp in experiments],
                )
                fastq_count += len(experiments)

            sub_count += 1
            if sub_count % 10000 == 0:
                log_info(f"scanned {sub_count} submissions ({fastq_count} fastq dirs)")

        log_info(f"fastq scan complete: {sub_count} submissions, {fastq_count} experiment dirs")

        # SRA ファイルをスキャン
        sra_base = DRA_BASE_PATH.joinpath("sra", "ByExp", "sra", "DRX")
        sra_count = 0
        if sra_base.exists():
            for sra_file in sra_base.rglob("*.sra"):
                run_accession = sra_file.stem
                conn.execute("INSERT INTO dra_sra_file VALUES (?)", [run_accession])
                sra_count += 1
                if sra_count % 10000 == 0:
                    log_info(f"scanned {sra_count} sra files")

        log_info(f"sra scan complete: {sra_count} sra files")

        # インデックス作成
        conn.execute("CREATE INDEX idx_dra_fastq_sub ON dra_fastq_dir(submission)")
        conn.execute("CREATE INDEX idx_dra_sra_run ON dra_sra_file(run)")

    # tmp -> final
    if final_path.exists():
        final_path.unlink()
    shutil.move(str(tmp_path), str(final_path))

    log_info(f"dra file index built: {fastq_count} fastq dirs, {sra_count} sra files")


def query_fastq_dirs_bulk(config: Config, submissions: list[str]) -> dict[str, set[str]]:
    """submission のリストから FASTQ experiment ディレクトリの存在情報を一括取得する。

    Returns:
        {submission: {experiment1, experiment2, ...}}
    """
    if not submissions:
        return {}

    db_path = get_dra_file_index_db_path(config)
    if not db_path.exists():
        return {}

    result: dict[str, set[str]] = {}
    with duckdb.connect(str(db_path), read_only=True) as conn:
        placeholders = ", ".join(["?"] * len(submissions))
        rows = conn.execute(
            f"SELECT submission, experiment FROM dra_fastq_dir WHERE submission IN ({placeholders})",
            submissions,
        ).fetchall()

    for sub, exp in rows:
        result.setdefault(sub, set()).add(exp)

    return result


def query_sra_files_bulk(config: Config, runs: list[str]) -> set[str]:
    """.sra ファイルが存在する run の集合を返す。"""
    if not runs:
        return set()

    db_path = get_dra_file_index_db_path(config)
    if not db_path.exists():
        return set()

    with duckdb.connect(str(db_path), read_only=True) as conn:
        placeholders = ", ".join(["?"] * len(runs))
        rows = conn.execute(
            f"SELECT run FROM dra_sra_file WHERE run IN ({placeholders})",
            runs,
        ).fetchall()

    return {row[0] for row in rows}
