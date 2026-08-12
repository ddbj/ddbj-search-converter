"""\
Sync DRA Metadata tar with latest data.

Builds or updates DRA Metadata tar from DRA XML files.
Uses DRA_Accessions.tab Updated field to identify changed submissions.
"""

import argparse
import sys

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.logging.logger import log_info, run_logger
from ddbj_search_converter.sra.dra_tar import build_dra_tar, get_dra_tar_path, repair_dra_tar, sync_dra_tar


def parse_args(args: list[str]) -> tuple[Config, bool, bool]:
    parser = argparse.ArgumentParser(description="Sync DRA Metadata tar with latest data")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild tar from scratch instead of incremental sync",
    )
    mode.add_argument(
        "--repair",
        action="store_true",
        help="Append submissions that are in DRA_Accessions.tab but missing from the tar",
    )

    parsed = parser.parse_args(args)

    config = get_config()

    return config, parsed.force_rebuild, parsed.repair


def main() -> None:
    config, force_rebuild, repair = parse_args(sys.argv[1:])

    with run_logger(config=config):
        dra_tar_path = get_dra_tar_path(config)

        if force_rebuild:
            log_info("force rebuilding dra metadata tar...")
            build_dra_tar(config)
        elif repair:
            log_info("repairing dra metadata tar...")
            repair_dra_tar(config)
        else:
            if not dra_tar_path.exists():
                log_info("dra tar does not exist, building from scratch")
            sync_dra_tar(config)


if __name__ == "__main__":
    main()
