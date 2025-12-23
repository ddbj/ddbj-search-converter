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

TRAD_BASE_PATH = Path("/lustre9/open/shared_data/trad")
# DBLINK_BASE_PATH = Path("/lustre9/open/shared_data/dblink")
DBLINK_BASE_PATH = RESULT_DIR.joinpath("dblink")  # TODO: for test


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
