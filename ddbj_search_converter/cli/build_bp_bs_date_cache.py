from ddbj_search_converter.config import get_config
from ddbj_search_converter.date_cache.build import build_date_cache
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  run_logger)


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info("building date cache from postgresql")
        build_date_cache(config)


if __name__ == "__main__":
    main()
