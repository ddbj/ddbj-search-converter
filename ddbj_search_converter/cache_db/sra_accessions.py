"""\
- SRA_Accessions.tab から、BioProject/BioSample ID と SRA Accession ID の関連を取得するための module
- relation ids (dbXrefs) の bulk insert のために使われる
"""
import datetime
from pathlib import Path
from typing import Dict, Literal, Set

import httpx

from ddbj_search_converter.config import Config

SRA_ACCESSIONS_FILE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
SRA_ACCESSIONS_FILE_NAME = "SRA_Accessions.tab"


AccessionType = Literal["bioproject", "biosample"]


# ref.: https://github.com/linsalrob/SRA_Metadata/blob/master/README.md
ID_INDEX = 0
TYPE_INDEX = 6
BIOSAMPLE_INDEX = 17
BIOPROJECT_INDEX = 18
TYPE_FILTERS = {
    "bioproject": ["STUDY", "EXPERIMENT", "RUN"],
    "biosample": ["SAMPLE", "EXPERIMENT", "RUN"],
}


def load_sra_accessions_tab(sra_accessions_tab_file: Path, accession_type: AccessionType) -> Dict[str, Set[str]]:
    """\
    - Return:
        - Key: BioProject/BioSample ID
        - Value: Set of DBLink IDs
    """

    id_to_relation_ids: Dict[str, Set[str]] = {}

    with sra_accessions_tab_file.open("r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            fields = line.strip().split("\t")
            if fields[TYPE_INDEX] not in TYPE_FILTERS[accession_type]:
                continue

            bp_bs_id = fields[BIOPROJECT_INDEX] if accession_type == "bioproject" else fields[BIOSAMPLE_INDEX]
            if bp_bs_id not in id_to_relation_ids:
                id_to_relation_ids[bp_bs_id] = set()
            id_to_relation_ids[bp_bs_id].add(fields[ID_INDEX])

    return id_to_relation_ids


def download_sra_accessions_tab_file(config: Config) -> Path:
    try:
        with httpx.stream("GET", SRA_ACCESSIONS_FILE_URL, timeout=30) as response:
            response.raise_for_status()
            with config.work_dir.joinpath(SRA_ACCESSIONS_FILE_NAME).open("wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
    except Exception as e:
        raise Exception(f"Failed to download SRA_Accessions.tab file: {e}") from e

    return config.work_dir.joinpath(SRA_ACCESSIONS_FILE_NAME)


def find_latest_sra_accessions_tab_file(config: Config) -> Path:
    """\
    - スパコン上の SRA_Accessions.tab file の位置として、/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions 以下に存在する
    - `{year}/{month}/SRA_Accessions.tab.{yyyymmdd}` という path で保存されている
    - Today から遡って、最初に見つかったファイルを返す
    """
    today = datetime.date.today()
    for days in range(90):  # Search for the last 90 days
        check_date = today - datetime.timedelta(days=days)
        year, month, yyyymmdd = check_date.strftime("%Y"), check_date.strftime("%m"), check_date.strftime("%Y%m%d")
        sra_accessions_tab_file_path = config.sra_accessions_tab_base_path.joinpath(f"{year}/{month}/SRA_Accessions.tab.{yyyymmdd}")  # type: ignore
        if sra_accessions_tab_file_path.exists():
            return sra_accessions_tab_file_path

    raise FileNotFoundError("SRA_Accessions.tab file not found in the last 90 days")
