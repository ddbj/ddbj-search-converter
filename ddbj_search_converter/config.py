"""
- default 値と、環境変数から取得する値を取り扱う
- 各所で get_config() として呼び出すことも可能だが、それぞれの cli script が argparse を使っているため、設定の不都合が生じる可能性がある
    - そのため、各 script の main 関数や parse_args で config の値の上書きをし、その後、config のバケツリレーを行う

"""

import datetime
import os
from pathlib import Path

from pydantic import BaseModel

WORK_DIR = Path.cwd().joinpath("converter_results")
TODAY = datetime.date.today().strftime("%Y%m%d")


class Config(BaseModel):
    accessions_dir: Path = WORK_DIR.joinpath(f"sra_accessions/{TODAY}")
    accessions_db_path: Path = WORK_DIR.joinpath("sra_accessions.sqlite")
    process_pool_size: int = 8
    dblink_db_path: Path = WORK_DIR.joinpath("ddbj_dblink.sqlite")
    dblink_files_base_path: Path = Path("/lustre9/open/shared_data/dblink")


default_config = Config()
ENV_PREFIX = "DDBJ_SEARCH_CONVERTER"


def get_config() -> Config:
    """\
    notice: This config is generated from default values and environment variables.
    """

    return Config(
        accessions_dir=Path(os.environ.get(f"{ENV_PREFIX}_ACCESSIONS_DIR", default_config.accessions_dir)),
        accessions_db_path=Path(os.environ.get(f"{ENV_PREFIX}_ACCESSIONS_DB_PATH", default_config.accessions_db_path)),
        process_pool_size=int(os.environ.get(f"{ENV_PREFIX}_PROCESS_POOL_SIZE", default_config.process_pool_size)),
        dblink_db_path=Path(os.environ.get(f"{ENV_PREFIX}_DBLINK_DB_PATH", default_config.dblink_db_path)),
        dblink_files_base_path=Path(os.environ.get(f"{ENV_PREFIX}_DBLINK_FILES_BASE_PATH", default_config.dblink_files_base_path)),
    )
