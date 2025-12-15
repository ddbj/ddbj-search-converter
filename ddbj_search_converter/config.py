"""
- default 値と、環境変数から取得する値を取り扱う
- 各所で get_config() として呼び出すことも可能だが、それぞれの cli script が argparse を使っているため、設定の不都合が生じる可能性がある
    - そのため、各 script の main 関数や parse_args で config の値の上書きをし、その後、config のバケツリレーを行う
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel

WORK_DIR = Path.cwd().joinpath("ddbj_search_converter_results")
DATE_FORMAT = "%Y%m%d"
LOCAL_TZ = ZoneInfo("Asia/Tokyo")
TODAY = datetime.now(LOCAL_TZ).date()
TODAY_STR = TODAY.strftime(DATE_FORMAT)

BP_JSONL_DIR_NAME = "bioproject_jsonl"
BS_JSONL_DIR_NAME = "biosample_jsonl"
DRA_JSONL_DIR_NAME = "dra_jsonl"
JGA_JSONL_DIR_NAME = "jga_jsonl"
LOG_DIR_NAME = "logs"
AccessionType = Literal["bioproject", "biosample"]

TRAD_BASE_PATH = Path("/lustre9/open/shared_data/trad")
# DBLINK_BASE_PATH = Path("/lustre9/open/shared_data/dblink")
DBLINK_BASE_PATH = WORK_DIR.joinpath("dblink")


class Config(BaseModel):
    """\
    - そこまで細かい Config は書きたくないが、debug とか work_dir など、環境変数で設定されそうな項目を一括管理する
    - 逆に、ある file path といった、command line argument level のことは、各 script で管理する
    """
    debug: bool = False
    work_dir: Path = WORK_DIR
    postgres_url: str = "postgresql://const:const@at098:54301"  # format is postgresql://{username}:{password}@{host}:{port}
    es_url: str = "http://ddbj-search-elasticsearch:9200"
    # sra_accessions_tab_base_path: Optional[Path] = None
    # sra_accessions_tab_file_path: Path = WORK_DIR.joinpath("SRA_Accessions.tab")
    # dra_base_path: Path = Path("/lustre9/open/shared_data/dra")
    # jga_base_path: Path = Path("/lustre9/open/shared_data/jga/metadata-history/metadata")
    # dra_xml_tar_file_path: Path = dra_base_path.joinpath("/app/NCBI_SRA_Metadata_Full_20251017.tar.gz")


default_config = Config()
ENV_PREFIX = "DDBJ_SEARCH_CONVERTER"


def get_config() -> Config:
    """\
    notice: This config is generated from default values and environment variables.
    """
    # sra_accessions_tab_base_path: Optional[Path] = None
    # env_sra_accessions_tab_base_path = os.environ.get(f"{ENV_PREFIX}_SRA_ACCESSIONS_TAB_BASE_PATH", None)
    # if env_sra_accessions_tab_base_path is not None:
    #     sra_accessions_tab_base_path = Path(env_sra_accessions_tab_base_path)

    return Config(
        debug=bool(os.environ.get(f"{ENV_PREFIX}_DEBUG", default_config.debug)),
        work_dir=Path(os.environ.get(f"{ENV_PREFIX}_WORK_DIR", default_config.work_dir)),
        postgres_url=os.environ.get(f"{ENV_PREFIX}_POSTGRES_URL", default_config.postgres_url),
        es_url=os.environ.get(f"{ENV_PREFIX}_ES_URL", default_config.es_url),
        # sra_accessions_tab_base_path=sra_accessions_tab_base_path,
        # sra_accessions_tab_file_path=Path(os.environ.get(f"{ENV_PREFIX}_SRA_ACCESSIONS_TAB_FILE_PATH", default_config.sra_accessions_tab_file_path)),
        # dra_base_path=Path(os.environ.get(f"{ENV_PREFIX}_DRA_BASE_PATH", default_config.dra_base_path)),
        # jga_base_path=Path(os.environ.get(f"{ENV_PREFIX}_JGA_BASE_PATH", default_config.jga_base_path)),
    )
