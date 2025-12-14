from pathlib import Path

import duckdb
import pandas as pd

SRA_ACCESSIONS_TAB_FILE_PATH = Path("/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions/2025/11/SRA_Accessions.tab.20251125")


def convert_tsv_to_parquet(
    tsv_path: Path,
    parquet_path: Path,
    chunksize: int = 1000000
) -> None:
    if parquet_path.exists():
        parquet_path.unlink()

    con = duckdb.connect()

    reader = pd.read_csv(
        tsv_path,
        sep="\t",
        chunksize=chunksize,
        dtype=str,
        na_values=["-"],
        keep_default_na=False,
    )

    first = True
    for chunk in reader:
        con.register("chunk", chunk)
        if first:
            con.execute(f"COPY chunk TO '{parquet_path}' (FORMAT PARQUET)")
            first = False
        else:
            con.execute(f"COPY chunk TO '{parquet_path}' (FORMAT PARQUET, APPEND true)")

    print(f"Converted {tsv_path} to {parquet_path}")


def load_parquet_to_duckdb(
    parquet_path: Path,
    db_path: Path,
    table_name="sra_accessions"
):
    con = duckdb.connect(db_path)

    con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(f"""
    CREATE TABLE {table_name} (
        Accession VARCHAR PRIMARY KEY,
        Submission VARCHAR,
        Status VARCHAR,
        Updated TIMESTAMP,
        Published TIMESTAMP,
        Received TIMESTAMP,
        Type VARCHAR,
        Center VARCHAR,
        Visibility VARCHAR,
        Alias VARCHAR,
        Experiment VARCHAR,
        Sample VARCHAR,
        Study VARCHAR,
        Loaded VARCHAR,
        Spots BIGINT,
        Bases BIGINT,
        Md5sum VARCHAR,
        BioSample VARCHAR,
        BioProject VARCHAR,
        ReplacedBy VARCHAR,
    )
    """)

    con.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM parquet_scan('{parquet_path}')
    """)

    con.close()

    print(f"Loaded {parquet_path} into {db_path} as table {table_name}")


if __name__ == "__main__":
    parquet_file_path = Path("/app/sra_accessions.parquet")
    duckdb_file_path = Path("/app/sra_accessions.duckdb")
    convert_tsv_to_parquet(
        SRA_ACCESSIONS_TAB_FILE_PATH,
        parquet_file_path,
    )
    load_parquet_to_duckdb(
        parquet_file_path,
        duckdb_file_path,
    )
