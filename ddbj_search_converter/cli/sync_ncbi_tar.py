"""\
Sync NCBI SRA Metadata tar with daily updates.

Downloads and appends daily tar.gz files since last sync.
"""
import argparse
import sys
from typing import List, Tuple

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.logging.logger import log_info, run_logger
from ddbj_search_converter.sra.ncbi_tar import get_ncbi_tar_path, sync_ncbi_tar


def parse_args(args: List[str]) -> Tuple[Config, bool]:
    parser = argparse.ArgumentParser(
        description="Sync NCBI SRA Metadata tar with daily updates"
    )
    parser.add_argument(
        "--force-full",
        action="store_true",
        help="Force download Full tar.gz instead of daily updates",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed = parser.parse_args(args)

    config = get_config()
    if parsed.debug:
        config.debug = True

    return config, parsed.force_full


def main() -> None:
    config, force_full = parse_args(sys.argv[1:])

    with run_logger(config=config):
        ncbi_tar_path = get_ncbi_tar_path(config)
        if not ncbi_tar_path.exists() and not force_full:
            log_info("NCBI tar does not exist, will download Full tar.gz")

        sync_ncbi_tar(config, force_full=force_full)


if __name__ == "__main__":
    main()
