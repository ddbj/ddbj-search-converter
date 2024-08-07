"""\
bp_bulk_insert の実装

- 差分がある jsonl を Elasticsearch に bulk insert する
- 引数の dir (previous_dir, current_dir) から差分を取得し、bulk insert する
- それぞれの dir は日付で命名されているとする
- 基本的に落ちないように実装しており previous_dir が存在しない (and 指定されていない) 場合は、前の日付の dir を自動検索する
    - 更に、その前の日の dir が存在しない場合は、current_dir に存在するファイル全てを bulk insert する
    - この挙動を抑制したい場合、--disable-find-prev-dir オプションを指定する
"""
import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from pydantic import BaseModel

from ddbj_search_converter.config import (LOGGER, Config, default_config,
                                          get_config, set_logging_level)
from ddbj_search_converter.utils import (bulk_insert_to_es, find_previous_dir,
                                         get_diff_files)


class Args(BaseModel):
    previous_dir: Optional[Path]
    current_dir: Path


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(description="Bulk insert JSON Lines data with differences to Elasticsearch")
    parser.add_argument(
        "--previous_dir",
        help="Path to the previous directory, if not specified, the previous directory will be searched automatically (e.g., /path/to/jsonl/20240801)",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--current_dir",
        help="Path to the current directory (e.g., /path/to/jsonl/20240802)"
    )
    parser.add_argument(
        "--disable-find-prev-dir",
        action="store_true",
        help="Disable automatic search for the previous directory"
    )
    parser.add_argument(
        "--es-base-url",
        help="Elasticsearch base URL (default: http://localhost:9200)",
        default=default_config.es_base_url,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.es_base_url != default_config.es_base_url:
        config.es_base_url = parsed_args.es_base_url
    if parsed_args.debug:
        config.debug = parsed_args.debug

    find_prev_dir = not parsed_args.disable_find_prev_dir

    # Args の型変換と validation
    previous_dir = None
    if parsed_args.previous_dir is not None:
        previous_dir = Path(parsed_args.previous_dir)
        if not previous_dir.exists():
            if find_prev_dir:
                LOGGER.info("The specified previous directory %s does not exist, so it will be searched automatically", previous_dir)
                previous_dir = None
            else:
                LOGGER.error("The specified previous directory %s does not exist", previous_dir)
                sys.exit(1)
    else:
        if not find_prev_dir:
            LOGGER.error("The previous directory is not specified")
            sys.exit(1)

    current_dir = Path(parsed_args.current_dir)
    if not current_dir.exists():
        LOGGER.error("The current directory %s does not exist", current_dir)
        sys.exit(1)

    return (config, Args(
        previous_dir=previous_dir,
        current_dir=current_dir,
    ))


# def logging_bulk_insert(file_id: str, message: str) -> None:
#     dir_name = os.path.dirname(args.later)
#     today = datetime.date.today()
#     formatted_data = today.strftime('%Y%m%d')

#     log_dir = Path(f"")

#     f"{dir_name}/{formatted_data}/logs"
#     log_file = f"{log_dir}/{file_name}_log.json"
#     if not os.path.exists(log_dir):
#         os.makedirs(log_dir)
#     with open(log_file, "w", encoding="utf-8") as f:
#         json.dump(message, f)


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.info("Start bulk inserting BioProject JSON-Lines data with differences to Elasticsearch")
    LOGGER.info("Config: %s", config.model_dump())
    LOGGER.info("Args: %s", args.model_dump())

    previous_dir = None
    if args.previous_dir is None:
        # previous_dir が指定されていないため、自動検索する
        try:
            previous_dir = find_previous_dir(args.current_dir)
            LOGGER.info("Found the previous directory: %s", previous_dir)
        except Exception as e:
            # 存在しない場合は、current_dir に存在するファイル全てを bulk insert する
            LOGGER.info("Failed to find the previous directory, so all files in the current directory will be bulk inserted")
            LOGGER.debug("Error: %s", e)

    insert_files: List[Path] = []
    if previous_dir is None:
        insert_files = list(args.current_dir.glob("*.jsonl"))
    else:
        insert_files = get_diff_files(previous_dir, args.current_dir)

    error_files = []
    for file in insert_files:
        with file.open("r", encoding="utf-8") as f:
            data = f.read()
            try:
                bulk_insert_to_es(config.es_base_url, str_data=data, raise_on_error=True)
            except Exception as e:
                LOGGER.error("Failed to bulk insert to Elasticsearch: file=%s, error=%s", file, e)
                error_files.append(file)

    if len(error_files) > 0:
        LOGGER.error("The following files failed to bulk insert to Elasticsearch:\n%s", "\n".join(f"- {f}" for f in error_files))
        sys.exit(1)

    LOGGER.info("Finished bulk inserting BioProject JSON-Lines data with differences to Elasticsearch")


if __name__ == "__main__":
    main()
