"""DRA ファイルインデックス構築・クエリモジュール。

DRA ファイルシステムをスキャンして FASTQ ディレクトリと SRA ファイルの
存在情報を DuckDB インデックスとして構築する。
JSONL 生成時にファイルシステムを直接叩かず、このインデックスを参照する。
"""

import os
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

from ddbj_search_converter.config import (
    DRA_BASE_PATH,
    DRA_FILE_INDEX_DB_FILE_NAME,
    TMP_DRA_FILE_INDEX_DB_FILE_NAME,
    Config,
)
from ddbj_search_converter.duckdb_bulk import load_tsv_into_table, write_rows_to_tsv
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.sra.dra_tar import iter_all_dra_submissions
from ddbj_search_converter.sra.paths import get_sra_tar_dir

SCAN_THREADS = 16


def get_dra_file_index_db_path(config: Config) -> Path:
    """DRA ファイルインデックス DB のパスを返す。"""

    return get_sra_tar_dir(config).joinpath(DRA_FILE_INDEX_DB_FILE_NAME)


def _tmp_db_path(config: Config) -> Path:

    return get_sra_tar_dir(config).joinpath(TMP_DRA_FILE_INDEX_DB_FILE_NAME)


def dra_file_index_exists(config: Config) -> bool:
    """DRA ファイルインデックス DB が存在するかを返す。"""

    return get_dra_file_index_db_path(config).exists()


def _scan_submission_dir(submission: str) -> tuple[str, list[str], list[str]] | None:
    """submission ディレクトリ直下の DRX / DRZ サブディレクトリ名を返す。ディレクトリが無ければ None。

    ``os.scandir`` はディレクトリ読み出しの結果から種別が分かる場合 stat を発行しない。
    エントリごとに stat するとネットワーク FS への往復がエントリ数だけ増える。
    """
    sub_dir = DRA_BASE_PATH.joinpath("fastq", submission[:6], submission)
    experiments: list[str] = []
    analyses: list[str] = []
    try:
        with os.scandir(sub_dir) as entries:
            for entry in entries:
                if entry.name.startswith("DRX"):
                    if entry.is_dir():
                        experiments.append(entry.name)
                elif entry.name.startswith("DRZ") and entry.is_dir():
                    analyses.append(entry.name)
    except FileNotFoundError:
        return None
    return submission, experiments, analyses


def _scan_sra_dir(directory: str) -> tuple[list[str], list[str]]:
    """directory 直下の ``*.sra`` の stem と、サブディレクトリ (symlink は辿らない) のパスを返す。"""
    runs: list[str] = []
    subdirs: list[str] = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.name.endswith(".sra"):
                runs.append(entry.name[: -len(".sra")])
            if entry.is_dir(follow_symlinks=False):
                subdirs.append(entry.path)
    return runs, subdirs


def _walk_sra_runs(root: str) -> list[str]:
    """root 以下を再帰的に辿り、``*.sra`` の stem を返す。"""
    runs: list[str] = []
    pending = [root]
    while pending:
        found, subdirs = _scan_sra_dir(pending.pop())
        runs.extend(found)
        pending.extend(subdirs)
    return runs


def _bulk_load(
    conn: duckdb.DuckDBPyConnection,
    tmp_db_path: Path,
    table_name: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[str]],
) -> None:
    tsv_path = tmp_db_path.with_name(f"{tmp_db_path.name}.{table_name}.tsv")
    try:
        written = write_rows_to_tsv(tsv_path, rows)
        load_tsv_into_table(conn, table_name, columns, tsv_path, written)
    finally:
        tsv_path.unlink(missing_ok=True)


def build_dra_file_index(config: Config) -> None:
    """FS をスキャンして DRA ファイルインデックス DuckDB を構築する。

    FASTQ (experiment): 各 submission ディレクトリ内の DRX サブディレクトリ
    FASTQ (analysis): 各 submission ディレクトリ内の DRZ サブディレクトリ
    SRA: DRA_BASE_PATH/sra/ByExp/sra/DRX/ ツリーの .sra ファイルから run を抽出

    スキャンは FS の応答待ちが支配的なのでスレッドで並列化する。DuckDB への投入は
    行単位の INSERT だと 1 行ごとに statement の実行コストがかかるため、TSV 経由で一括ロードする。
    """
    tmp_path = _tmp_db_path(config)
    final_path = get_dra_file_index_db_path(config)
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    # 既存の tmp を削除
    tmp_path.unlink(missing_ok=True)

    log_info("building dra file index...")

    # FASTQ ディレクトリをスキャン
    fastq_rows: list[tuple[str, str]] = []
    analysis_rows: list[tuple[str, str]] = []
    sub_count = 0
    with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
        for scanned in executor.map(_scan_submission_dir, iter_all_dra_submissions(config)):
            if scanned is None:
                continue
            submission, experiments, analyses = scanned
            fastq_rows.extend((submission, exp) for exp in experiments)
            analysis_rows.extend((submission, ana) for ana in analyses)

            sub_count += 1
            if sub_count % 10000 == 0:
                log_info(
                    f"scanned {sub_count} submissions "
                    f"({len(fastq_rows)} fastq dirs, {len(analysis_rows)} analysis dirs)"
                )

    fastq_count = len(fastq_rows)
    analysis_count = len(analysis_rows)
    log_info(
        f"fastq scan complete: {sub_count} submissions, {fastq_count} experiment dirs, {analysis_count} analysis dirs"
    )

    # SRA ファイルをスキャン
    sra_base = DRA_BASE_PATH.joinpath("sra", "ByExp", "sra", "DRX")
    sra_rows: list[tuple[str]] = []
    if sra_base.exists():
        top_level_runs, top_level_dirs = _scan_sra_dir(str(sra_base))
        sra_rows.extend((run,) for run in top_level_runs)
        with ThreadPoolExecutor(max_workers=SCAN_THREADS) as executor:
            for runs in executor.map(_walk_sra_runs, top_level_dirs):
                logged = len(sra_rows) // 100000
                sra_rows.extend((run,) for run in runs)
                if len(sra_rows) // 100000 > logged:
                    log_info(f"scanned {len(sra_rows)} sra files")
    sra_count = len(sra_rows)
    log_info(f"sra scan complete: {sra_count} sra files")

    with duckdb.connect(str(tmp_path)) as conn:
        conn.execute("CREATE TABLE dra_fastq_dir (submission TEXT NOT NULL, experiment TEXT NOT NULL)")
        conn.execute("CREATE TABLE dra_fastq_analysis_dir (submission TEXT NOT NULL, analysis TEXT NOT NULL)")
        conn.execute("CREATE TABLE dra_sra_file (run TEXT NOT NULL)")

        _bulk_load(conn, tmp_path, "dra_fastq_dir", ("submission", "experiment"), fastq_rows)
        _bulk_load(conn, tmp_path, "dra_fastq_analysis_dir", ("submission", "analysis"), analysis_rows)
        _bulk_load(conn, tmp_path, "dra_sra_file", ("run",), sra_rows)

        # インデックス作成
        conn.execute("CREATE INDEX idx_dra_fastq_sub ON dra_fastq_dir(submission)")
        conn.execute("CREATE INDEX idx_dra_fastq_analysis_sub ON dra_fastq_analysis_dir(submission)")
        conn.execute("CREATE INDEX idx_dra_sra_run ON dra_sra_file(run)")

    # tmp -> final
    tmp_path.replace(final_path)

    log_info(f"dra file index built: {fastq_count} fastq dirs, {analysis_count} analysis dirs, {sra_count} sra files")


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


def query_analysis_dirs_bulk(config: Config, submissions: list[str]) -> dict[str, set[str]]:
    """submission のリストから analysis (DRZ) ディレクトリの存在情報を一括取得する。

    Returns:
        {submission: {analysis1, analysis2, ...}}
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
            f"SELECT submission, analysis FROM dra_fastq_analysis_dir WHERE submission IN ({placeholders})",
            submissions,
        ).fetchall()

    for sub, ana in rows:
        result.setdefault(sub, set()).add(ana)

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
