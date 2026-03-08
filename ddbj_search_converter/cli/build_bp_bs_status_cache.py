from ddbj_search_converter.config import get_config
from ddbj_search_converter.logging.logger import log_debug, log_info, run_logger
from ddbj_search_converter.status_cache.build import build_status_cache


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info("building status cache from livelist files")
        build_status_cache(config)


if __name__ == "__main__":
    main()
