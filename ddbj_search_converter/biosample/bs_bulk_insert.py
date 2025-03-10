"""\
- 生成された jsonl files を bs に bulk insert する
- insert する file を探す処理も含んでいる
    - --dry-run option とかで、探す処理だけの実行を想定する
"""
import sys

from ddbj_search_converter.bioproject.bp_bulk_insert import (bulk_insert_to_es,
                                                             parse_args)
from ddbj_search_converter.config import (BS_JSONL_DIR_NAME, LOGGER,
                                          set_logging_level)
from ddbj_search_converter.utils import (find_insert_target_files,
                                         get_recent_dirs)


def main() -> None:
    LOGGER.info("Bulk inserting BioSample data into Elasticsearch")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    latest_dir, prior_dir = get_recent_dirs(config.work_dir.joinpath(BS_JSONL_DIR_NAME), args.latest_dir, args.prior_dir)
    LOGGER.info("Latest dir: %s", latest_dir)
    LOGGER.info("Prior dir: %s", prior_dir)
    if prior_dir is None:
        LOGGER.info("No prior dir found. Inserting only the latest data.")

    jsonl_files = find_insert_target_files(latest_dir, prior_dir)
    LOGGER.info("Inserting %d files", len(jsonl_files))
    if args.dry_run:
        LOGGER.info("Dry run mode. Exiting without inserting.")
        LOGGER.info("These files to be inserted:\n%s", "\n".join(str(f) for f in jsonl_files))
        sys.exit(0)

    bulk_insert_to_es(config, jsonl_files, "biosample")

    LOGGER.info("Finished inserting BioSample data into Elasticsearch")


if __name__ == "__main__":
    main()
