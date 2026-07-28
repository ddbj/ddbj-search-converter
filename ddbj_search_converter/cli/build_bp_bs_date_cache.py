import argparse
import sys

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.date_cache.build import build_date_cache
from ddbj_search_converter.logging.logger import log_debug, run_logger


def parse_args(args: list[str]) -> tuple[Config, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(description="Build the BioProject/BioSample date cache from XSM PostgreSQL.")
    parser.add_argument(
        "--full",
        help="Rebuild the entire cache instead of fetching only recently modified rows.",
        action="store_true",
    )

    parsed = parser.parse_args(args)

    return get_config(), parsed.full


def main() -> None:
    config, full = parse_args(sys.argv[1:])

    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_debug(f"full rebuild: {full}")
        build_date_cache(config, full=full)


if __name__ == "__main__":
    main()
