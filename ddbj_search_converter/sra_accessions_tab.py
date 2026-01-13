"""\
- SRA_Accessions.tab から、BioProject/BioSample ID と SRA Accession ID の関連を取得するための module
- relation ids (dbXrefs) の bulk insert のために使われる
"""
import datetime
import shutil
from pathlib import Path
from typing import Iterator, Literal, Tuple

import duckdb

from ddbj_search_converter.config import (
    DRA_ACCESSIONS_BASE_PATH,
    DRA_DB_FILE_NAME,
    SRA_ACCESSIONS_BASE_PATH,
    SRA_DB_FILE_NAME,
    TMP_DRA_DB_FILE_NAME,
    TMP_SRA_DB_FILE_NAME,
    TODAY,
    Config,
    get_config,
)
from ddbj_search_converter.logging.logger import log_debug, log_info, run_logger

TABLE_NAME = "accessions"


# === DB path helpers ===


def _tmp_sra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", TMP_SRA_DB_FILE_NAME)


def _final_sra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", SRA_DB_FILE_NAME)


def _tmp_dra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", TMP_DRA_DB_FILE_NAME)


def _final_dra_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("sra", DRA_DB_FILE_NAME)


# === Locate latest accession files ===


def find_latest_sra_accessions_tab_file() -> Path:
    """
    Search backward from TODAY to find the latest SRA_Accessions.tab.YYYYMMDD.
    """
    for days in range(180):
        check_date = TODAY - datetime.timedelta(days=days)
        year = check_date.strftime("%Y")
        month = check_date.strftime("%m")
        yyyymmdd = check_date.strftime("%Y%m%d")

        path = SRA_ACCESSIONS_BASE_PATH.joinpath(
            f"{year}/{month}/SRA_Accessions.tab.{yyyymmdd}"
        )
        if path.exists():
            return path

    raise FileNotFoundError("SRA_Accessions.tab not found in last 180 days")


def find_latest_dra_accessions_tab_file() -> Path:
    """
    Search backward from TODAY to find the latest DRA_Accessions.tab.
    """
    for days in range(180):
        check_date = TODAY - datetime.timedelta(days=days)
        yyyymmdd = check_date.strftime("%Y%m%d")

        path = DRA_ACCESSIONS_BASE_PATH.joinpath(
            f"{yyyymmdd}.DRA_Accessions.tab"
        )
        if path.exists():
            return path

    raise FileNotFoundError("DRA_Accessions.tab not found in last 180 days")


# === DB initialization and loading ===


def init_accession_db(tmp_db_path: Path) -> None:
    """
    Initialize an empty tmp accession DB.
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
    Load TSV directly into DuckDB using read_csv.
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
                CAST(Updated AS TIMESTAMP),
                CAST(Published AS TIMESTAMP),
                CAST(Received AS TIMESTAMP)
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
    """
    Atomically replace final DB with tmp DB.
    """
    with duckdb.connect(tmp_path) as conn:
        conn.execute("CREATE INDEX idx_bp ON accessions(BioProject)")
        conn.execute("CREATE INDEX idx_bs ON accessions(BioSample)")
        conn.execute("CREATE INDEX idx_acc ON accessions(Accession)")

    if final_path.exists():
        final_path.unlink()

    shutil.move(str(tmp_path), str(final_path))


# === Public builders ===


def build_sra_accessions_db(config: Config) -> Path:
    """
    Build sra_accessions.duckdb from the latest SRA_Accessions.tab.
    """
    tsv_path = find_latest_sra_accessions_tab_file()
    tmp_db = _tmp_sra_db_path(config)
    final_db = _final_sra_db_path(config)

    init_accession_db(tmp_db)
    load_tsv_to_tmp_db(tsv_path, tmp_db)
    finalize_db(tmp_db, final_db)

    return final_db


def build_dra_accessions_db(config: Config) -> Path:
    """
    Build dra_accessions.duckdb from the latest DRA_Accessions.tab.
    """
    tsv_path = find_latest_dra_accessions_tab_file()
    tmp_db = _tmp_dra_db_path(config)
    final_db = _final_dra_db_path(config)

    init_accession_db(tmp_db)
    load_tsv_to_tmp_db(tsv_path, tmp_db)
    finalize_db(tmp_db, final_db)

    return final_db


# === Relation extraction ===


SourceKind = Literal["sra", "dra"]


def iter_bp_bs_relations(
    config: Config,
    *,
    source: SourceKind,
) -> Iterator[Tuple[str, str]]:
    """
    Iterate BioProject <-> BioSample relations.
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


# === main ===


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        log_info("building SRA accessions database")
        sra_final_db = build_sra_accessions_db(config)
        log_info("SRA accessions database built", file=sra_final_db)

        log_info("building DRA accessions database")
        dra_final_db = build_dra_accessions_db(config)
        log_info("DRA accessions database built", file=dra_final_db)


if __name__ == "__main__":
    main()
