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
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import duckdb

from ddbj_search_converter.config import (
    DEFAULT_MARGIN_DAYS,
    DRA_ACCESSIONS_BASE_PATH,
    DRA_DB_FILE_NAME,
    ISO8601_UTC_FORMAT,
    SRA_ACCESSIONS_BASE_PATH,
    SRA_DB_FILE_NAME,
    TMP_DRA_DB_FILE_NAME,
    TMP_SRA_DB_FILE_NAME,
    TODAY,
    Config,
    get_config,
)
from ddbj_search_converter.logging.logger import log_info, run_logger

TABLE_NAME = "accessions"
QUERY_BATCH_SIZE = 10000


def _tmp_sra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", TMP_SRA_DB_FILE_NAME)


def _final_sra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", SRA_DB_FILE_NAME)


def _tmp_dra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", TMP_DRA_DB_FILE_NAME)


def _final_dra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", DRA_DB_FILE_NAME)


def find_latest_sra_accessions_tab_file() -> Path | None:
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


def find_latest_dra_accessions_tab_file() -> Path | None:
    """
    最新の DRA_Accessions.tab ファイルを探す。

    TODAY から 180 日遡って、最初に見つかったファイルを返す。
    """
    for days in range(180):
        check_date = TODAY - datetime.timedelta(days=days)
        yyyymmdd = check_date.strftime("%Y%m%d")

        path = DRA_ACCESSIONS_BASE_PATH.joinpath(f"{yyyymmdd}.DRA_Accessions.tab")
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
                Updated     TIMESTAMPTZ,
                Published   TIMESTAMPTZ,
                Received    TIMESTAMPTZ
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
            """
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
                CAST(Updated AS TIMESTAMPTZ),
                CAST(Published AS TIMESTAMPTZ),
                CAST(Received AS TIMESTAMPTZ)
            FROM read_csv(
                ?,
                delim='\t',
                header=true,
                nullstr=['-', ''],
                all_varchar=true
            )
            """,
            (str(tsv_path),),
        )


def finalize_db(tmp_path: Path, final_path: Path) -> None:
    with duckdb.connect(tmp_path) as conn:
        conn.execute("CREATE INDEX idx_bp ON accessions(BioProject)")
        conn.execute("CREATE INDEX idx_bs ON accessions(BioSample)")
        conn.execute("CREATE INDEX idx_acc ON accessions(Accession)")

    if final_path.exists():
        final_path.unlink()

    tmp_path.replace(final_path)


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

# `_iter_relation` で使う安全な column 名集合。f-string で SQL に埋め込むため、
# allowlist で SQL injection を防ぐ (DuckDB は SELECT 列名を `?` で bind できない)。
_ALLOWED_COLUMNS: frozenset[str] = frozenset(
    {
        "Accession",
        "Submission",
        "BioSample",
        "BioProject",
        "Study",
        "Experiment",
        "Sample",
    }
)


def _iter_relation(
    config: Config,
    *,
    source: SourceKind,
    col_a: str,
    col_b: str,
    type_filter: str | None = None,
) -> Iterator[tuple[str, str]]:
    """``accessions`` テーブルから DISTINCT な (col_a, col_b) ペアを抽出する内部ヘルパ。

    NULL 値は除外。``type_filter`` が指定された場合は ``Type = ?`` 条件を追加する。
    """
    if col_a not in _ALLOWED_COLUMNS:
        raise ValueError(f"col_a not in allowed columns: {col_a!r}")
    if col_b not in _ALLOWED_COLUMNS:
        raise ValueError(f"col_b not in allowed columns: {col_b!r}")

    db_path = _final_sra_db_path(config) if source == "sra" else _final_dra_db_path(config)

    where_clauses = [f"{col_a} IS NOT NULL", f"{col_b} IS NOT NULL"]
    params: list[str] = []
    if type_filter is not None:
        where_clauses.insert(0, "Type = ?")
        params.append(type_filter)

    query = f"SELECT DISTINCT {col_a}, {col_b} FROM accessions WHERE {' AND '.join(where_clauses)}"

    with duckdb.connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    yield from rows


# 18 個の `iter_*_relations` は (col_a, col_b, type_filter) のみが異なる薄い wrapper。
# 重複を `_iter_relation` 経由で 1 行ずつに集約しつつ、関数名は明示する (mypy / IDE 補完
# のため、また `from ddbj_search_converter.sra_accessions_tab import iter_*` 互換のため)。


def iter_bp_bs_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioProject <-> BioSample (Type フィルタなし、NULL 値除外、DISTINCT)。"""
    yield from _iter_relation(config, source=source, col_a="BioProject", col_b="BioSample")


def iter_study_experiment_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Study <-> Experiment (Type='EXPERIMENT')。"""
    yield from _iter_relation(config, source=source, col_a="Study", col_b="Accession", type_filter="EXPERIMENT")


def iter_experiment_run_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Experiment <-> Run (Type='RUN')。"""
    yield from _iter_relation(config, source=source, col_a="Experiment", col_b="Accession", type_filter="RUN")


def iter_experiment_sample_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Experiment <-> Sample (Type='EXPERIMENT')。"""
    yield from _iter_relation(config, source=source, col_a="Accession", col_b="Sample", type_filter="EXPERIMENT")


def iter_run_sample_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Run <-> Sample (Type='RUN')。"""
    yield from _iter_relation(config, source=source, col_a="Accession", col_b="Sample", type_filter="RUN")


def iter_submission_study_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Submission <-> Study (Type='STUDY')。"""
    yield from _iter_relation(config, source=source, col_a="Submission", col_b="Accession", type_filter="STUDY")


def iter_study_analysis_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Study <-> Analysis (Type='ANALYSIS')。"""
    yield from _iter_relation(config, source=source, col_a="Study", col_b="Accession", type_filter="ANALYSIS")


def iter_submission_analysis_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """Submission <-> Analysis (Type='ANALYSIS')。Study が NULL でも Submission 経由で関連を保つ。"""
    yield from _iter_relation(config, source=source, col_a="Submission", col_b="Accession", type_filter="ANALYSIS")


# === BioProject/BioSample <-> SRA relation iterators ===


def iter_bp_study_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioProject <-> Study (Type='STUDY')。"""
    yield from _iter_relation(config, source=source, col_a="BioProject", col_b="Accession", type_filter="STUDY")


def iter_bp_experiment_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioProject <-> Experiment (Type='EXPERIMENT')。"""
    yield from _iter_relation(config, source=source, col_a="BioProject", col_b="Accession", type_filter="EXPERIMENT")


def iter_bp_run_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioProject <-> Run (Type='RUN')。"""
    yield from _iter_relation(config, source=source, col_a="BioProject", col_b="Accession", type_filter="RUN")


def iter_bp_analysis_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioProject <-> Analysis (Type='ANALYSIS')。"""
    yield from _iter_relation(config, source=source, col_a="BioProject", col_b="Accession", type_filter="ANALYSIS")


def iter_bs_sample_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioSample <-> Sample (Type='SAMPLE')。"""
    yield from _iter_relation(config, source=source, col_a="BioSample", col_b="Accession", type_filter="SAMPLE")


def iter_bs_experiment_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioSample <-> Experiment (Type='EXPERIMENT')。"""
    yield from _iter_relation(config, source=source, col_a="BioSample", col_b="Accession", type_filter="EXPERIMENT")


def iter_bs_run_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioSample <-> Run (Type='RUN')。"""
    yield from _iter_relation(config, source=source, col_a="BioSample", col_b="Accession", type_filter="RUN")


def iter_bs_analysis_relations(config: Config, *, source: SourceKind) -> Iterator[tuple[str, str]]:
    """BioSample <-> Analysis (Type='ANALYSIS')。"""
    yield from _iter_relation(config, source=source, col_a="BioSample", col_b="Accession", type_filter="ANALYSIS")


# === Query functions for JSONL generation ===


AccessionInfo = tuple[str, str, str | None, str | None, str | None, str]
# (status, visibility, received, updated, published, type)


# SPEC: docs/data-architecture.md §SRA Accessions: 同 accession の status 重複時の優先順位
# 小さいほど「強い」(= 表示すべき) status。同 accession が複数行で出現するとき、
# priority が小さい status を勝たせる。tie は dict 挿入順で先勝ち。
STATUS_PRIORITY: dict[str, int] = {
    "live": 0,
    "public": 1,
    "suppressed": 2,
    "withdrawn": 3,
}
_DEFAULT_STATUS_STRENGTH = STATUS_PRIORITY["public"]


def _status_strength(status: str | None) -> int:
    if status is None:
        return _DEFAULT_STATUS_STRENGTH
    return STATUS_PRIORITY.get(status, _DEFAULT_STATUS_STRENGTH)


def get_accession_info_bulk(
    config: Config,
    source: SourceKind,
    accessions: list[str],
) -> dict[str, AccessionInfo]:
    """
    Accessions DB から status, visibility, 日付を一括取得する。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        accessions: 取得する accession のリスト

    Returns:
        {accession: (status, visibility, received, updated, published, type)}

    同一 accession が複数行ある場合、``STATUS_PRIORITY`` の小さい順 (live > public
    > suppressed > withdrawn) で 1 件に集約する。tie は先勝ち。
    """
    if not accessions:
        return {}

    db_path = _final_sra_db_path(config) if source == "sra" else _final_dra_db_path(config)

    result: dict[str, AccessionInfo] = {}

    with duckdb.connect(db_path, read_only=True) as conn:
        for i in range(0, len(accessions), QUERY_BATCH_SIZE):
            batch = accessions[i : i + QUERY_BATCH_SIZE]
            placeholders = ", ".join(["?"] * len(batch))
            rows = conn.execute(
                f"""
                SELECT
                    Accession,
                    Status,
                    Visibility,
                    strftime(Received, '{ISO8601_UTC_FORMAT}'),
                    strftime(Updated, '{ISO8601_UTC_FORMAT}'),
                    strftime(Published, '{ISO8601_UTC_FORMAT}'),
                    Type
                FROM accessions
                WHERE Accession IN ({placeholders})
                """,
                batch,
            ).fetchall()

            for row in rows:
                acc, status, visibility, received, updated, published, type_ = row
                new_info: AccessionInfo = (
                    status or "public",
                    visibility or "public",
                    received,
                    updated,
                    published,
                    type_ or "",
                )
                existing = result.get(acc)
                if existing is None or _status_strength(new_info[0]) < _status_strength(existing[0]):
                    result[acc] = new_info

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
    db_path = _final_sra_db_path(config) if source == "sra" else _final_dra_db_path(config)

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
    db_path = _final_sra_db_path(config) if source == "sra" else _final_dra_db_path(config)

    # since から margin_days を引いた日付を計算
    # 形式: "2026-01-19T00:00:00Z" -> "2026-01-16"
    since_date = since.split("T", maxsplit=1)[0]
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
    accessions: list[str],
) -> dict[str, str]:
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

    db_path = _final_sra_db_path(config) if source == "sra" else _final_dra_db_path(config)

    result: dict[str, str] = {}

    with duckdb.connect(db_path, read_only=True) as conn:
        for i in range(0, len(accessions), QUERY_BATCH_SIZE):
            batch = accessions[i : i + QUERY_BATCH_SIZE]
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

            result.update(dict(rows))

    return result


def get_submission_accessions(
    config: Config,
    source: SourceKind,
    submissions: set[str],
) -> dict[str, list[str]]:
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

    db_path = _final_sra_db_path(config) if source == "sra" else _final_dra_db_path(config)

    result: dict[str, list[str]] = {s: [] for s in submissions}

    with duckdb.connect(db_path, read_only=True) as conn:
        submission_list = list(submissions)
        for i in range(0, len(submission_list), QUERY_BATCH_SIZE):
            batch = submission_list[i : i + QUERY_BATCH_SIZE]
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
