import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import BaseModel

RESULT_DIR = Path.cwd().joinpath("ddbj_search_converter_results")  # Path to dump jsonl files and logs
CONST_DIR = Path("/home/w3ddbjld/const")  # Path to store constant/shared resources
DATE_FORMAT = "%Y%m%d"
LOCAL_TZ = ZoneInfo("Asia/Tokyo")
TODAY = datetime.now(LOCAL_TZ).date()
TODAY_STR = TODAY.strftime(DATE_FORMAT)

BP_JSONL_DIR_NAME = "bioproject_jsonl"
BS_JSONL_DIR_NAME = "biosample_jsonl"
SRA_JSONL_DIR_NAME = "sra_jsonl"
JGA_JSONL_DIR_NAME = "jga_jsonl"
LOG_DIR_NAME = "logs"


# === Base paths (container mount points) ===
TRAD_BASE_PATH = Path("/usr/local/resources/trad")
BIOPROJECT_BASE_PATH = Path("/usr/local/resources/bioproject")
BIOSAMPLE_BASE_PATH = Path("/usr/local/resources/biosample")
DRA_BASE_PATH = Path("/usr/local/resources/dra")
GEA_BASE_PATH = Path("/usr/local/resources/gea/experiment")
METABOBANK_BASE_PATH = Path("/usr/local/shared_data/metabobank/study")
JGA_BASE_PATH = Path("/usr/local/shared_data/jga/metadata-history/metadata")
DBLINK_OUTPUT_PATH = Path("/usr/local/shared_data/dblink")

# DB file names
LOG_DB_FILE_NAME = "log.duckdb"
SRA_DB_FILE_NAME = "sra_accessions.duckdb"
TMP_SRA_DB_FILE_NAME = "sra_accessions.tmp.duckdb"
DRA_DB_FILE_NAME = "dra_accessions.duckdb"
TMP_DRA_DB_FILE_NAME = "dra_accessions.tmp.duckdb"
DBLINK_DB_FILE_NAME = "dblink.duckdb"
TMP_DBLINK_DB_FILE_NAME = "dblink.tmp.duckdb"

# External resource paths (relative to const_dir)
BPBS_PRESERVED_REL_PATH = "dblink/bp_bs_preserved.tsv"
BP_BLACKLIST_REL_PATH = "bp/blacklist.txt"
BS_BLACKLIST_REL_PATH = "bs/blacklist.txt"

# Accessions base paths
SRA_ACCESSIONS_BASE_PATH = Path(
    "/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions"
)
DRA_ACCESSIONS_BASE_PATH = Path(
    "/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public"
)

# BioSample XML paths (/usr/local/resources is a mount point to /lustre9)
NCBI_BIOSAMPLE_XML = BIOSAMPLE_BASE_PATH.joinpath("biosample_set.xml.gz")
DDBJ_BIOSAMPLE_XML = BIOSAMPLE_BASE_PATH.joinpath("ddbj_biosample_set.xml.gz")

# NCBI Assembly summary URL
ASSEMBLY_SUMMARY_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt"


class Config(BaseModel):
    debug: bool = False
    result_dir: Path = RESULT_DIR
    const_dir: Path = CONST_DIR
    postgres_url: str = "postgresql://const:const@at098:54301"  # format is postgresql://{username}:{password}@{host}:{port}
    es_url: str = "http://ddbj-search-elasticsearch:9200"


default_config = Config()
ENV_PREFIX = "DDBJ_SEARCH_CONVERTER"


def get_config() -> Config:
    return Config(
        result_dir=Path(os.environ.get(f"{ENV_PREFIX}_RESULT_DIR", default_config.result_dir)),
        const_dir=Path(os.environ.get(f"{ENV_PREFIX}_CONST_DIR", default_config.const_dir)),
        postgres_url=os.environ.get(f"{ENV_PREFIX}_POSTGRES_URL", default_config.postgres_url),
        es_url=os.environ.get(f"{ENV_PREFIX}_ES_URL", default_config.es_url),
    )
