"""\
- SRA_Accessions.tab から、BioProject/BioSample ID と SRA Accession ID の関連を取得するための module
- relation ids (dbXrefs) の bulk insert のために使われる
"""
import datetime
from pathlib import Path
from typing import Dict, Set

import duckdb
import httpx

from ddbj_search_converter.config import AccessionType, Config

DRA_ACCESSION_TAB_BASE_PATH = Path("/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public")
SRA_ACCESSION_TAB_BASE_PATH = Path()

SRA_ACCESSIONS_FILE_NAME = "SRA_Accessions.tab"


def find_latest_sra_accessions_tab_file(config: Config) -> Path:
    """\
    - スパコン上の SRA_Accessions.tab file の位置として、/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions 以下に存在する
    - `{year}/{month}/SRA_Accessions.tab.{yyyymmdd}` という path で保存されている
    - Today から遡って、最初に見つかったファイルを返す
    """
    today = datetime.date.today()
    for days in range(180):  # Search for the last 180 days
        check_date = today - datetime.timedelta(days=days)
        year, month, yyyymmdd = check_date.strftime("%Y"), check_date.strftime("%m"), check_date.strftime("%Y%m%d")
        sra_accessions_tab_file_path = config.sra_accessions_tab_base_path.joinpath(f"{year}/{month}/SRA_Accessions.tab.{yyyymmdd}")  # type: ignore
        if sra_accessions_tab_file_path.exists():
            return sra_accessions_tab_file_path

    raise FileNotFoundError("SRA_Accessions.tab file not found in the last 180 days")


SRA_ACCESSIONS_TAB_FILE_PATH = Path(
    "/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions/2025/11/SRA_Accessions.tab.20251125"
)

TABLE_NAME = "sra_accessions"


SRA_SCHEMA_SQL = """
    Accession     VARCHAR,
    Submission    VARCHAR,
    Status        VARCHAR,
    Updated       TIMESTAMP,
    Published     TIMESTAMP,
    Received      TIMESTAMP,
    Type          VARCHAR,
    Center        VARCHAR,
    Visibility    VARCHAR,
    Alias         VARCHAR,
    Experiment    VARCHAR,
    Sample        VARCHAR,
    Study         VARCHAR,
    Loaded        VARCHAR,
    Spots         BIGINT,
    Bases         BIGINT,
    Md5sum        VARCHAR,
    BioSample     VARCHAR,
    BioProject    VARCHAR,
    ReplacedBy    VARCHAR
"""


def tsv_to_parquet(
    tsv_path: Path,
    parquet_path: Path,
    overwrite: bool = True,
) -> None:
    if parquet_path.exists():
        if overwrite:
            parquet_path.unlink()
        else:
            raise FileExistsError(parquet_path)

    con = duckdb.connect()

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM read_csv(
                '{tsv_path}',
                delim='\\t',
                header=true,
                nullstr='-',
                all_varchar=true
            )
        )
        TO '{parquet_path}'
        (FORMAT PARQUET)
        """
    )

    con.close()
    print(f"TSV → Parquet: {parquet_path}")


def load_parquet_to_duckdb(
    parquet_path: Path,
    db_path: Path,
    table_name: str = TABLE_NAME,
    overwrite: bool = True,
) -> None:
    con = duckdb.connect(db_path)

    if overwrite:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(
        f"""
        CREATE TABLE {table_name} (
            {SRA_SCHEMA_SQL}
        )
        """
    )

    con.execute(
        f"""
        INSERT INTO {table_name}
        SELECT *
        FROM parquet_scan('{parquet_path}')
        """
    )

    con.close()
    print(f"Parquet → DuckDB: {db_path}:{table_name}")


def tsv_to_duckdb_direct(
    tsv_path: Path,
    db_path: Path,
    table_name: str = TABLE_NAME,
    overwrite: bool = True,
) -> None:
    con = duckdb.connect(db_path)

    if overwrite:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT *
        FROM read_csv(
            '{tsv_path}',
            delim='\\t',
            header=true,
            nullstr='-',
            all_varchar=true
        )
        """
    )

    con.close()
    print(f"TSV → DuckDB (direct): {db_path}:{table_name}")
