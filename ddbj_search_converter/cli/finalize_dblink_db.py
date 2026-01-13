from ddbj_search_converter.config import get_config
from ddbj_search_converter.dblink.db import finalize_relation_db
from ddbj_search_converter.logging.logger import log_info, run_logger


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_info("finalizing dblink database")
        finalize_relation_db(config)


if __name__ == "__main__":
    main()
