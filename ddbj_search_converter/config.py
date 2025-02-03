"""
- default 値と、環境変数から取得する値を取り扱う
- 各所で get_config() として呼び出すことも可能だが、それぞれの cli script が argparse を使っているため、設定の不都合が生じる可能性がある
    - そのため、各 script の main 関数や parse_args で config の値の上書きをし、その後、config のバケツリレーを行う
"""

import datetime
import logging
import logging.config
import os
from pathlib import Path

from pydantic import BaseModel

DATE_FORMAT = "%Y%m%d"
WORK_DIR = Path.cwd().joinpath("ddbj_search_converter_results")
TODAY = datetime.date.today().strftime(DATE_FORMAT)
BP_JSONL_DIR_NAME = "bioproject_jsonl"
BS_JSONL_DIR_NAME = "biosample_jsonl"


class Config(BaseModel):
    """\
    - そこまで細かい Config は書きたくないが、debug とか work_dir など、環境変数で設定されそうな項目を一括管理する
    - 逆に、ある file path といった、command line argument level のことは、各 script で管理する
    """
    debug: bool = False
    work_dir: Path = WORK_DIR
    postgres_url: str = "postgresql://const:@at098:54301"  # e.g., postgresql://{username}:{password}@{host}:{port}
    es_url: str = "http://localhost:9200"
    # postgres_url
    # accessions_dir: Path = WORK_DIR.joinpath(f"sra_accessions/{TODAY}")
    # accessions_db_path: Path = WORK_DIR.joinpath("sra_accessions.sqlite")
    # process_pool_size: int = 8
    # dblink_db_path: Path = WORK_DIR.joinpath("ddbj_dblink.sqlite")
    # dblink_files_base_path: Path = Path("/lustre9/open/shared_data/dblink")


default_config = Config()
ENV_PREFIX = "DDBJ_SEARCH_CONVERTER"


def get_config() -> Config:
    """\
    notice: This config is generated from default values and environment variables.
    """
    return Config(
        debug=bool(os.environ.get(f"{ENV_PREFIX}_DEBUG", default_config.debug)),
        work_dir=Path(os.environ.get(f"{ENV_PREFIX}_WORK_DIR", default_config.work_dir)),
        postgres_url=os.environ.get(f"{ENV_PREFIX}_POSTGRES_URL", default_config.postgres_url),
        es_url=os.environ.get(f"{ENV_PREFIX}_ES_URL", default_config.es_url),
        # sra_accessions_tab_file_path=
        # accessions_dir=Path(os.environ.get(f"{ENV_PREFIX}_ACCESSIONS_DIR", default_config.accessions_dir)),
        # accessions_db_path=Path(os.environ.get(f"{ENV_PREFIX}_ACCESSIONS_DB_PATH", default_config.accessions_db_path)),
        # process_pool_size=int(os.environ.get(f"{ENV_PREFIX}_PROCESS_POOL_SIZE", default_config.process_pool_size)),
        # dblink_db_path=Path(os.environ.get(f"{ENV_PREFIX}_DBLINK_DB_PATH", default_config.dblink_db_path)),
        # dblink_files_base_path=Path(os.environ.get(f"{ENV_PREFIX}_DBLINK_FILES_BASE_PATH", default_config.dblink_files_base_path)),
        # es_base_url=os.environ.get(f"{ENV_PREFIX}_ES_BASE_URL", default_config.es_base_url),
    )


# === logging ===


def set_logging_config() -> None:
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(levelprefix)s %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "sqlalchemy": {
                "format": "%(levelprefix)s SQLAlchemy - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "sqlalchemy": {
                "formatter": "sqlalchemy",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "ddbj_search_converter": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False
            },
            "sqlalchemy.engine": {
                "handlers": ["sqlalchemy"],
                "level": "WARNING",
                "propagate": False
            },
        },
    }

    logging.config.dictConfig(config)


set_logging_config()
LOGGER = logging.getLogger("ddbj_search_converter")


def set_logging_level(debug: bool) -> None:
    if debug:
        logging.getLogger("ddbj_search_converter").setLevel(logging.DEBUG)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    else:
        logging.getLogger("ddbj_search_converter").setLevel(logging.INFO)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
