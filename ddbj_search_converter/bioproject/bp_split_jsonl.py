"""\
bp_split_jsonl の実装

- bp_xml2jsonl で生成した jsonl ファイルを指定した行数で分割する
"""

import argparse
import shutil
import sys
from pathlib import Path
from typing import List, Tuple

from pydantic import BaseModel

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)


class Args(BaseModel):
    jsonl_file: Path
    output_dir: Path
    split_size: int = 2000


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="Split jsonl file generated by bp_xml2jsonl with specified number of lines"
    )
    parser.add_argument(
        "jsonl_file",
        help="JSON-Lines file to split, which is generated by bp_xml2jsonl",
    )
    parser.add_argument(
        "output_dir",
        help="Directory to output the split JSON-Lines files",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output directory if it already exists",
    )
    parser.add_argument(
        "--split-size",
        type=int,
        default=2000,
        help="Number of lines to split the JSON-Lines file (default: 2000)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode ",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.debug:
        config.debug = parsed_args.debug

    # Args の型変換と validation
    jsonl_file = Path(parsed_args.jsonl_file)
    if not jsonl_file.exists():
        LOGGER.error("Input JSON-Lines file does not exist: %s", jsonl_file)
        sys.exit(1)
    output_dir = Path(parsed_args.output_dir)
    if output_dir.exists():
        if parsed_args.overwrite:
            LOGGER.info("Output directory %s already exists, but will be overwritten", output_dir)
            shutil.rmtree(output_dir)
        else:
            LOGGER.error("Output directory already exists: %s", output_dir)
            sys.exit(1)
    output_dir.mkdir(parents=True, exist_ok=True)

    return (config, Args(
        jsonl_file=jsonl_file,
        output_dir=output_dir,
        split_size=parsed_args.split_size,
    ))


def split_file(jsonl_file: Path, output_dir: Path, split_size: int) -> None:
    """\
    jsonl ファイルを特定の行数のファイルに分割して出力する
    """
    with jsonl_file.open("r", encoding="utf-8") as f:
        file_count = 0
        lines = []
        for line_num, line in enumerate(f, start=1):
            lines.append(line)
            if line_num % split_size == 0:
                output_file = output_dir.joinpath(f"{jsonl_file.stem}_{file_count:06d}.jsonl")
                with output_file.open("w", encoding="utf-8") as f_out:
                    f_out.writelines(lines)
                file_count += 1
                lines.clear()

        # 残りのデータを出力
        if len(lines) > 0:
            output_file = output_dir.joinpath(f"{jsonl_file.stem}_{file_count:06d}.jsonl")
            with output_file.open("w", encoding="utf-8") as f_out:
                f_out.writelines(lines)


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.info("Start splitting BioProject JSON-Lines file %s", args.jsonl_file)
    LOGGER.info("Config: %s", config.model_dump())
    LOGGER.info("Args: %s", args.model_dump())

    split_file(args.jsonl_file, args.output_dir, args.split_size)

    LOGGER.info("Finished splitting BioProject JSON-Lines file %s", args.jsonl_file)


if __name__ == "__main__":
    main()
