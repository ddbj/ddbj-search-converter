from ddbj_search_converter.config import get_config
from ddbj_search_converter.dblink.db import init_dblink_db
from ddbj_search_converter.logging.logger import log_debug, run_logger


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        init_dblink_db(config)


if __name__ == "__main__":
    main()
