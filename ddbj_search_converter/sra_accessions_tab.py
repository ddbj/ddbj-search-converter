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
from typing import Dict, Iterator, List, Literal, Optional, Set, Tuple

import duckdb

from ddbj_search_converter.config import (DEFAULT_MARGIN_DAYS,
                                          DRA_ACCESSIONS_BASE_PATH,
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
                Submission  TEXT,
                BioSample   TEXT,
                BioProject  TEXT,
                Study       TEXT,
                Experiment  TEXT,
                Sample      TEXT,
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
                Submission,
                BioSample,
                BioProject,
                Study,
                Experiment,
                Sample,
                Type,
                Status,
                Visibility,
                CAST(Updated AS TIMESTAMP),
                CAST(Published AS TIMESTAMP),
                CAST(Received AS TIMESTAMP)
            FROM read_csv(
                '{tsv_path}',
                delim='\\t',
                header=true,
                nullstr=['-', ''],
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


def iter_study_experiment_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Study <-> Experiment 関連をイテレートする。

    EXPERIMENT 行から Accession (Experiment) と Study の関連を抽出する。
    NULL 値は除外される。

    Returns:
        (study_accession, experiment_accession) のイテレータ
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
                Study,
                Accession
            FROM accessions
            WHERE
                Type = 'EXPERIMENT'
                AND Study IS NOT NULL
                AND Accession IS NOT NULL
            """
        ).fetchall()

    yield from rows


def iter_experiment_run_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Experiment <-> Run 関連をイテレートする。

    RUN 行から Experiment と Accession (Run) の関連を抽出する。
    NULL 値は除外される。

    Returns:
        (experiment_accession, run_accession) のイテレータ
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
                Experiment,
                Accession
            FROM accessions
            WHERE
                Type = 'RUN'
                AND Experiment IS NOT NULL
                AND Accession IS NOT NULL
            """
        ).fetchall()

    yield from rows


def iter_experiment_sample_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Experiment <-> Sample 関連をイテレートする。

    EXPERIMENT 行から Accession (Experiment) と Sample の関連を抽出する。
    NULL 値は除外される。

    Returns:
        (experiment_accession, sample_accession) のイテレータ
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
                Accession,
                Sample
            FROM accessions
            WHERE
                Type = 'EXPERIMENT'
                AND Accession IS NOT NULL
                AND Sample IS NOT NULL
            """
        ).fetchall()

    yield from rows


def iter_run_sample_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Run <-> Sample 関連をイテレートする。

    RUN 行から Accession (Run) と Sample の関連を抽出する。
    NULL 値は除外される。

    Returns:
        (run_accession, sample_accession) のイテレータ
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
                Accession,
                Sample
            FROM accessions
            WHERE
                Type = 'RUN'
                AND Accession IS NOT NULL
                AND Sample IS NOT NULL
            """
        ).fetchall()

    yield from rows


def iter_submission_study_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Submission <-> Study 関連をイテレートする。

    STUDY 行から Submission と Accession (Study) の関連を抽出する。
    NULL 値は除外される。

    Returns:
        (submission_accession, study_accession) のイテレータ
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
                Submission,
                Accession
            FROM accessions
            WHERE
                Type = 'STUDY'
                AND Submission IS NOT NULL
                AND Accession IS NOT NULL
            """
        ).fetchall()

    yield from rows


def iter_study_analysis_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Study <-> Analysis 関連をイテレートする。

    ANALYSIS 行から Study と Accession (Analysis) の関連を抽出する。
    NULL 値は除外される。

    Returns:
        (study_accession, analysis_accession) のイテレータ
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
                Study,
                Accession
            FROM accessions
            WHERE
                Type = 'ANALYSIS'
                AND Study IS NOT NULL
                AND Accession IS NOT NULL
            """
        ).fetchall()

    yield from rows


def iter_submission_analysis_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Submission <-> Analysis 関連をイテレートする。

    ANALYSIS 行から Submission と Accession (Analysis) の関連を抽出する。
    Study が NULL の場合でも Submission 経由で関連を持てるようにする。
    NULL 値は除外される。

    Returns:
        (submission_accession, analysis_accession) のイテレータ
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
                Submission,
                Accession
            FROM accessions
            WHERE
                Type = 'ANALYSIS'
                AND Submission IS NOT NULL
                AND Accession IS NOT NULL
            """
        ).fetchall()

    yield from rows


# === Query functions for JSONL generation ===


AccessionInfo = Tuple[str, str, Optional[str], Optional[str], Optional[str], str]
# (status, visibility, received, updated, published, type)


def get_accession_info_bulk(
    config: Config,
    source: SourceKind,
    accessions: List[str],
) -> Dict[str, AccessionInfo]:
    """
    Accessions DB から status, visibility, 日付を一括取得する。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        accessions: 取得する accession のリスト

    Returns:
        {accession: (status, visibility, received, updated, published, type)}
    """
    if not accessions:
        return {}

    db_path = (
        _final_sra_db_path(config)
        if source == "sra"
        else _final_dra_db_path(config)
    )

    result: Dict[str, AccessionInfo] = {}

    with duckdb.connect(db_path, read_only=True) as conn:
        # バッチサイズを制限してクエリを実行
        batch_size = 10000
        for i in range(0, len(accessions), batch_size):
            batch = accessions[i:i + batch_size]
            placeholders = ", ".join(["?"] * len(batch))
            rows = conn.execute(
                f"""
                SELECT
                    Accession,
                    Status,
                    Visibility,
                    strftime(Received, '%Y-%m-%dT%H:%M:%SZ'),
                    strftime(Updated, '%Y-%m-%dT%H:%M:%SZ'),
                    strftime(Published, '%Y-%m-%dT%H:%M:%SZ'),
                    Type
                FROM accessions
                WHERE Accession IN ({placeholders})
                """,
                batch,
            ).fetchall()

            for row in rows:
                acc, status, visibility, received, updated, published, type_ = row
                result[acc] = (
                    status or "public",
                    visibility or "public",
                    received,
                    updated,
                    published,
                    type_ or "",
                )

    return result


def iter_all_submissions(
    config: Config,
    source: SourceKind,
) -> Iterator[str]:
    """
    全 submission を取得する。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"

    Returns:
        submission accession のイテレータ
    """
    db_path = (
        _final_sra_db_path(config)
        if source == "sra"
        else _final_dra_db_path(config)
    )

    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT Accession
            FROM accessions
            WHERE Type = 'SUBMISSION'
            ORDER BY Accession
            """
        ).fetchall()

    for row in rows:
        yield row[0]


def iter_updated_submissions(
    config: Config,
    source: SourceKind,
    since: str,
    margin_days: int = DEFAULT_MARGIN_DAYS,
) -> Iterator[str]:
    """
    since 以降に更新された submission を取得する。

    Updated カラムで since - margin_days 以降に更新されたエントリを持つ
    submission を取得する。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        since: ISO8601 形式の日時文字列
        margin_days: マージン日数 (デフォルト 7)

    Returns:
        submission accession のイテレータ
    """
    db_path = (
        _final_sra_db_path(config)
        if source == "sra"
        else _final_dra_db_path(config)
    )

    # since から margin_days を引いた日付を計算
    # 形式: "2026-01-19T00:00:00Z" -> "2026-01-16"
    since_date = since.split("T")[0]
    since_dt = datetime.datetime.strptime(since_date, "%Y-%m-%d")
    margin_dt = since_dt - datetime.timedelta(days=margin_days)
    margin_date = margin_dt.strftime("%Y-%m-%d")

    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT Submission
            FROM accessions
            WHERE Updated >= ?
            ORDER BY Submission
            """,
            [margin_date],
        ).fetchall()

    for row in rows:
        if row[0] is not None:
            yield row[0]


def lookup_submissions_for_accessions(
    config: Config,
    source: SourceKind,
    accessions: List[str],
) -> Dict[str, str]:
    """
    Accession から所属 Submission を逆引きする。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        accessions: 逆引き対象の accession のリスト

    Returns:
        {accession: submission}
    """
    if not accessions:
        return {}

    db_path = (
        _final_sra_db_path(config)
        if source == "sra"
        else _final_dra_db_path(config)
    )

    result: Dict[str, str] = {}

    with duckdb.connect(db_path, read_only=True) as conn:
        batch_size = 10000
        for i in range(0, len(accessions), batch_size):
            batch = accessions[i:i + batch_size]
            placeholders = ", ".join(["?"] * len(batch))
            rows = conn.execute(
                f"""
                SELECT DISTINCT Accession, Submission
                FROM accessions
                WHERE Accession IN ({placeholders})
                    AND Submission IS NOT NULL
                """,
                batch,
            ).fetchall()

            for acc, sub in rows:
                result[acc] = sub

    return result


def get_submission_accessions(
    config: Config,
    source: SourceKind,
    submissions: Set[str],
) -> Dict[str, List[str]]:
    """
    指定された submission に含まれる全 accession を取得する。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        submissions: submission accession の集合

    Returns:
        {submission: [accession1, accession2, ...]}
    """
    if not submissions:
        return {}

    db_path = (
        _final_sra_db_path(config)
        if source == "sra"
        else _final_dra_db_path(config)
    )

    result: Dict[str, List[str]] = {s: [] for s in submissions}

    with duckdb.connect(db_path, read_only=True) as conn:
        # バッチサイズを制限してクエリを実行
        batch_size = 10000
        submission_list = list(submissions)
        for i in range(0, len(submission_list), batch_size):
            batch = submission_list[i:i + batch_size]
            placeholders = ", ".join(["?"] * len(batch))
            rows = conn.execute(
                f"""
                SELECT Submission, Accession
                FROM accessions
                WHERE Submission IN ({placeholders})
                ORDER BY Submission, Accession
                """,
                batch,
            ).fetchall()

            for submission, accession in rows:
                if submission in result:
                    result[submission].append(accession)

    return result


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
