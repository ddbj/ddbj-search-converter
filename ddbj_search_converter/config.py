import os
from datetime import datetime
from pathlib import Path
from typing import Literal
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
# DBLINK_OUTPUT_PATH = Path("/usr/local/shared_data/dblink")
DBLINK_OUTPUT_PATH = Path("/app/ddbj_search_converter_results/dblink")

# DB file names
LOG_DB_FILE_NAME = "log.duckdb"
SRA_DB_FILE_NAME = "sra_accessions.duckdb"
TMP_SRA_DB_FILE_NAME = "sra_accessions.tmp.duckdb"
DRA_DB_FILE_NAME = "dra_accessions.duckdb"
TMP_DRA_DB_FILE_NAME = "dra_accessions.tmp.duckdb"
DBLINK_DB_FILE_NAME = "dblink.duckdb"
TMP_DBLINK_DB_FILE_NAME = "dblink.tmp.duckdb"

# External resource paths (relative to const_dir)
BP_BS_PRESERVED_REL_PATH = "dblink/bp_bs_preserved.tsv"
BP_BLACKLIST_REL_PATH = "bp/blacklist.txt"
BS_BLACKLIST_REL_PATH = "bs/blacklist.txt"
MTB_BP_PRESERVED_REL_PATH = "metabobank/mtb_id_bioproject_preserve.tsv"
MTB_BS_PRESERVED_REL_PATH = "metabobank/mtb_id_biosample_preserve.tsv"

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

# BioProject XML paths
NCBI_BIOPROJECT_XML = BIOPROJECT_BASE_PATH.joinpath("bioproject.xml")
DDBJ_BIOPROJECT_XML = BIOPROJECT_BASE_PATH.joinpath("ddbj_core_bioproject.xml")

# JGA XML/CSV paths
JGA_STUDY_XML = JGA_BASE_PATH.joinpath("jga-study.xml")
JGA_DATASET_ANALYSIS_CSV = JGA_BASE_PATH.joinpath("dataset-analysis-relation.csv")
JGA_ANALYSIS_STUDY_CSV = JGA_BASE_PATH.joinpath("analysis-study-relation.csv")
JGA_DATASET_DATA_CSV = JGA_BASE_PATH.joinpath("dataset-data-relation.csv")
JGA_DATA_EXPERIMENT_CSV = JGA_BASE_PATH.joinpath("data-experiment-relation.csv")
JGA_EXPERIMENT_STUDY_CSV = JGA_BASE_PATH.joinpath("experiment-study-relation.csv")
JGA_DATASET_POLICY_CSV = JGA_BASE_PATH.joinpath("dataset-policy-relation.csv")
JGA_POLICY_DAC_CSV = JGA_BASE_PATH.joinpath("policy-dac-relation.csv")

# XML split wrappers
BIOSAMPLE_WRAPPER_START = b'<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n'
BIOSAMPLE_WRAPPER_END = b'</BioSampleSet>'
BIOPROJECT_WRAPPER_START = b'<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>\n'
BIOPROJECT_WRAPPER_END = b'</PackageSet>'

# NCBI Assembly summary URL
ASSEMBLY_SUMMARY_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt"

# === SRA/DRA tar configuration ===
# NCBI SRA Metadata tar.gz URLs
NCBI_SRA_METADATA_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata"

# tar file names (stored in {const_dir}/sra/)
SRA_TAR_DIR_NAME = "sra"
NCBI_SRA_TAR_FILE_NAME = "NCBI_SRA_Metadata.tar"
DRA_TAR_FILE_NAME = "DRA_Metadata.tar"
NCBI_LAST_MERGED_FILE_NAME = "ncbi_last_merged.txt"
DRA_LAST_UPDATED_FILE_NAME = "dra_last_updated.txt"


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
