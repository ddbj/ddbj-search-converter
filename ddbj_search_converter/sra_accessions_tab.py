"""
SRA/DRA Accessions データベースの構築と関連抽出を行うモジュール。

SRA_Accessions.tab と DRA_Accessions.tab を DuckDB にロードし、
BioProject/BioSample と SRA Accession 間の関連を高速に取得できるようにする。

入力ファイル:
- SRA_Accessions.tab
    - パス: {SRA_ACCESSIONS_BASE_PATH}/{YYYY}/{MM}/SRA_Accessions.tab.{YYYYMMDD}
    - NCBI SRA の全 accession 情報 (約 2GB)
- DRA_Accessions.tab
    - パス: {DRA_ACCESSIONS_BASE_PATH}/{YYYYMMDD}.DRA_Accessions.tab
    - DDBJ DRA の accession 情報

出力ファイル:
- {const_dir}/sra/sra_accessions.duckdb
- {const_dir}/sra/dra_accessions.duckdb
"""
import datetime
import shutil
from pathlib import Path
from typing import Iterator, Literal, Optional, Tuple

import duckdb

from ddbj_search_converter.config import (DRA_ACCESSIONS_BASE_PATH,
                                          DRA_DB_FILE_NAME,
                                          SRA_ACCESSIONS_BASE_PATH,
                                          SRA_DB_FILE_NAME,
                                          TMP_DRA_DB_FILE_NAME,
                                          TMP_SRA_DB_FILE_NAME, TODAY, Config,
                                          get_config)
from ddbj_search_converter.logging.logger import log_info, run_logger

TABLE_NAME = "accessions"


def _tmp_sra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", TMP_SRA_DB_FILE_NAME)


def _final_sra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", SRA_DB_FILE_NAME)


def _tmp_dra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", TMP_DRA_DB_FILE_NAME)


def _final_dra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", DRA_DB_FILE_NAME)


def find_latest_sra_accessions_tab_file() -> Optional[Path]:
    """
    最新の SRA_Accessions.tab ファイルを探す。

    TODAY から 180 日遡って、最初に見つかったファイルを返す。
    """
    for days in range(180):
        check_date = TODAY - datetime.timedelta(days=days)
        year = check_date.strftime("%Y")
        month = check_date.strftime("%m")
        yyyymmdd = check_date.strftime("%Y%m%d")

        path = SRA_ACCESSIONS_BASE_PATH.joinpath(f"{year}/{month}/SRA_Accessions.tab.{yyyymmdd}")
        if path.exists():
            return path

    return None


def find_latest_dra_accessions_tab_file() -> Optional[Path]:
    """
    最新の DRA_Accessions.tab ファイルを探す。

    TODAY から 180 日遡って、最初に見つかったファイルを返す。
    """
    for days in range(180):
        check_date = TODAY - datetime.timedelta(days=days)
        yyyymmdd = check_date.strftime("%Y%m%d")

        path = DRA_ACCESSIONS_BASE_PATH.joinpath(
            f"{yyyymmdd}.DRA_Accessions.tab"
        )
        if path.exists():
            return path

    return None


def init_accession_db(tmp_db_path: Path) -> None:
    """
    空の accessions テーブルを持つ一時 DB を初期化する。

    既存のファイルは削除される。
    """
    tmp_db_path.parent.mkdir(parents=True, exist_ok=True)
    if tmp_db_path.exists():
        tmp_db_path.unlink()

    with duckdb.connect(tmp_db_path) as conn:
        conn.execute(
            """
            CREATE TABLE accessions (
                Accession   TEXT,
                BioSample   TEXT,
                BioProject  TEXT,
                Study       TEXT,
                Experiment  TEXT,
                Type        TEXT,
                Status      TEXT,
                Visibility  TEXT,
                Updated     TIMESTAMP,
                Published   TIMESTAMP,
                Received    TIMESTAMP
            )
            """
        )


def load_tsv_to_tmp_db(
    tsv_path: Path,
    tmp_db_path: Path,
) -> None:
    """
    TSV ファイルを DuckDB に直接ロードする。

    DuckDB の read_csv() を使用して効率的にロードする。
    '-' と空文字列は NULL として扱われる。

    Args:
        tsv_path: 入力 TSV ファイルパス
        tmp_db_path: 一時 DB ファイルパス
    """
    with duckdb.connect(tmp_db_path) as conn:
        conn.execute(
            f"""
            INSERT INTO accessions
            SELECT
                Accession,
                BioSample,
                BioProject,
                Study,
                Experiment,
                Type,
                Status,
                Visibility,
                CAST(NULLIF(Updated, '') AS TIMESTAMP),
                CAST(NULLIF(Published, '') AS TIMESTAMP),
                CAST(NULLIF(Received, '') AS TIMESTAMP)
            FROM read_csv(
                '{tsv_path}',
                delim='\\t',
                header=true,
                nullstr='-',
                all_varchar=true
            )
            """
        )


def finalize_db(tmp_path: Path, final_path: Path) -> None:
    with duckdb.connect(tmp_path) as conn:
        conn.execute("CREATE INDEX idx_bp ON accessions(BioProject)")
        conn.execute("CREATE INDEX idx_bs ON accessions(BioSample)")
        conn.execute("CREATE INDEX idx_acc ON accessions(Accession)")

    if final_path.exists():
        final_path.unlink()

    shutil.move(str(tmp_path), str(final_path))


# === Public builders ===


def build_sra_accessions_db(config: Config) -> Path:
    tsv_path = find_latest_sra_accessions_tab_file()
    if tsv_path is None:
        raise FileNotFoundError("SRA_Accessions.tab not found in last 180 days")

    tmp_db = _tmp_sra_db_path(config)
    final_db = _final_sra_db_path(config)

    init_accession_db(tmp_db)
    load_tsv_to_tmp_db(tsv_path, tmp_db)
    finalize_db(tmp_db, final_db)

    return final_db


def build_dra_accessions_db(config: Config) -> Path:
    tsv_path = find_latest_dra_accessions_tab_file()
    if tsv_path is None:
        raise FileNotFoundError("DRA_Accessions.tab not found in last 180 days")
    tmp_db = _tmp_dra_db_path(config)
    final_db = _final_dra_db_path(config)

    init_accession_db(tmp_db)
    load_tsv_to_tmp_db(tsv_path, tmp_db)
    finalize_db(tmp_db, final_db)

    return final_db


SourceKind = Literal["sra", "dra"]


def iter_bp_bs_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    BioProject <-> BioSample 関連をイテレートする。

    accessions テーブルから DISTINCT な (BioProject, BioSample) ペアを抽出する。
    NULL 値は除外される。
    """
    db_path = (
        _final_sra_db_path(config)
        if source == "sra"
        else _final_dra_db_path(config)
    )

    with duckdb.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT
                BioProject,
                BioSample
            FROM accessions
            WHERE
                BioProject IS NOT NULL
                AND BioSample IS NOT NULL
            """
        ).fetchall()

    yield from rows


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_info("building SRA accessions database")
        sra_final_db = build_sra_accessions_db(config)
        log_info("SRA accessions database built", file=str(sra_final_db))

        log_info("building DRA accessions database")
        dra_final_db = build_dra_accessions_db(config)
        log_info("DRA accessions database built", file=str(dra_final_db))


if __name__ == "__main__":
    main()
