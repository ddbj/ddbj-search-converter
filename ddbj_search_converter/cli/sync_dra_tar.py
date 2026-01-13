"""\
Sync DRA Metadata tar with latest data.

Builds or updates DRA Metadata tar from DRA XML files.
Uses DRA_Accessions.tab Updated field to identify changed submissions.
"""
import argparse
import sys
from typing import List, Tuple

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.logging.logger import log_info, run_logger
from ddbj_search_converter.sra.dra_tar import (build_dra_tar, get_dra_tar_path,
                                               sync_dra_tar)


def parse_args(args: List[str]) -> Tuple[Config, bool]:
    parser = argparse.ArgumentParser(
        description="Sync DRA Metadata tar with latest data"
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild tar from scratch instead of incremental sync",
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

    return config, parsed.force_rebuild


def main() -> None:
    config, force_rebuild = parse_args(sys.argv[1:])

    with run_logger(config=config):
        dra_tar_path = get_dra_tar_path(config)

        if force_rebuild:
            log_info("Force rebuilding DRA Metadata tar...")
            build_dra_tar(config)
        else:
            if not dra_tar_path.exists():
                log_info("DRA tar does not exist, building from scratch")
            sync_dra_tar(config)


if __name__ == "__main__":
    main()
