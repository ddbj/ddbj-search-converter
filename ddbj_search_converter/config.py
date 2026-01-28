import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel

RESULT_DIR = Path.cwd().joinpath("ddbj_search_converter_results")  # Path to dump jsonl files and logs
CONST_DIR = Path("/home/w3ddbjld/const")  # Path to store constant/shared resources
DATE_FORMAT = "%Y%m%d"
LOCAL_TZ = ZoneInfo("Asia/Tokyo")
_date_override = os.environ.get("DDBJ_SEARCH_CONVERTER_DATE")
if _date_override:
    TODAY = datetime.strptime(_date_override, DATE_FORMAT).date()
else:
    TODAY = datetime.now(LOCAL_TZ).date()
TODAY_STR = TODAY.strftime(DATE_FORMAT)

BP_BASE_DIR_NAME = "bioproject"
BS_BASE_DIR_NAME = "biosample"
SRA_BASE_DIR_NAME = "sra"
JGA_BASE_DIR_NAME = "jga"
TMP_XML_DIR_NAME = "tmp_xml"
JSONL_DIR_NAME = "jsonl"
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
DATE_CACHE_DB_FILE_NAME = "bp_bs_date.duckdb"
TMP_DATE_CACHE_DB_FILE_NAME = "bp_bs_date.tmp.duckdb"

# const_dir relative paths (blacklist, preserved)
# These are relative to config.const_dir, which can be overridden by env var
BP_BS_PRESERVED_REL_PATH = "dblink/bp_bs_preserved.tsv"
BP_BLACKLIST_REL_PATH = "bp/blacklist.txt"
BS_BLACKLIST_REL_PATH = "bs/blacklist.txt"
SRA_BLACKLIST_REL_PATH = "sra/blacklist.txt"
MTB_BP_PRESERVED_REL_PATH = "metabobank/mtb_id_bioproject_preserve.tsv"
MTB_BS_PRESERVED_REL_PATH = "metabobank/mtb_id_biosample_preserve.tsv"

# Accessions base paths
SRA_ACCESSIONS_BASE_PATH = Path(
    "/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions"
)
DRA_ACCESSIONS_BASE_PATH = Path(
    "/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public"
)

# TRAD ORGANISM_LIST paths
TRAD_WGS_ORGANISM_LIST = TRAD_BASE_PATH.joinpath("wgs/WGS_ORGANISM_LIST.txt")
TRAD_TLS_ORGANISM_LIST = TRAD_BASE_PATH.joinpath("tls/TLS_ORGANISM_LIST.txt")
TRAD_TSA_ORGANISM_LIST = TRAD_BASE_PATH.joinpath("tsa/TSA_ORGANISM_LIST.txt")
TRAD_TPA_WGS_ORGANISM_LIST = TRAD_BASE_PATH.joinpath("tpa/wgs/TPA_WGS_ORGANISM_LIST.txt")
TRAD_TPA_TSA_ORGANISM_LIST = TRAD_BASE_PATH.joinpath("tpa/tsa/TPA_TSA_ORGANISM_LIST.txt")
TRAD_TPA_TLS_ORGANISM_LIST = TRAD_BASE_PATH.joinpath("tpa/tls/TPA_TLS_ORGANISM_LIST.txt")

# BioProject XML paths
NCBI_BIOPROJECT_XML = BIOPROJECT_BASE_PATH.joinpath("bioproject.xml")
DDBJ_BIOPROJECT_XML = BIOPROJECT_BASE_PATH.joinpath("ddbj_core_bioproject.xml")

# BioSample XML paths
NCBI_BIOSAMPLE_XML = BIOSAMPLE_BASE_PATH.joinpath("biosample_set.xml.gz")
DDBJ_BIOSAMPLE_XML = BIOSAMPLE_BASE_PATH.joinpath("ddbj_biosample_set.xml.gz")

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
NCBI_SRA_METADATA_LOCAL_PATH = Path(
    "/lustre9/open/database/ddbj-dbt/dra-private/mirror/Metadata/Metadata"
)

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
    postgres_url: str = ""
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


# === last_run.json utilities ===

LAST_RUN_FILE_NAME = "last_run.json"

DEFAULT_MARGIN_DAYS = 7


def apply_margin(since: str, margin_days: int = DEFAULT_MARGIN_DAYS) -> str:
    """since から margin_days を引いた ISO8601 日時文字列を返す。"""
    dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    margin_dt = dt - timedelta(days=margin_days)
    return margin_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

DataType = Literal["bioproject", "biosample", "sra", "jga"]


def get_last_run_path(config: Config) -> Path:
    """last_run.json のパスを返す。"""
    return config.result_dir / LAST_RUN_FILE_NAME


def read_last_run(config: Config) -> Dict[DataType, Optional[str]]:
    """
    last_run.json を読み込む。

    Returns:
        {data_type: ISO8601 datetime string or None}

    Example:
        {
            "bioproject": "2026-01-19T00:00:00Z",
            "biosample": "2026-01-19T00:00:00Z",
            "sra": "2026-01-19T00:00:00Z",
            "jga": null
        }
    """
    path = get_last_run_path(config)
    if not path.exists():
        return {"bioproject": None, "biosample": None, "sra": None, "jga": None}

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    return {
        "bioproject": data.get("bioproject"),
        "biosample": data.get("biosample"),
        "sra": data.get("sra"),
        "jga": data.get("jga"),
    }


def write_last_run(config: Config, data_type: DataType, timestamp: Optional[str] = None) -> None:
    """
    last_run.json の指定したデータタイプのタイムスタンプを更新する。

    Args:
        config: Config オブジェクト
        data_type: 更新するデータタイプ
        timestamp: ISO8601 形式のタイムスタンプ。None の場合は現在時刻を使用。
    """
    if timestamp is None:
        timestamp = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    last_run = read_last_run(config)
    last_run[data_type] = timestamp

    path = get_last_run_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        json.dump(last_run, f, indent=2)
        f.write("\n")
