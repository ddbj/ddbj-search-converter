"""\
- 生成された jsonl files を bs に bulk insert する
- insert する file を探す処理も含んでいる
    - --dry-run option とかで、探す処理だけの実行を想定する
"""
import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from pydantic import BaseModel

from ddbj_search_converter.config import (BP_JSONL_DIR_NAME, LOGGER,
                                          AccessionType, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.schema import BioProject, BioSample
from ddbj_search_converter.utils import (find_insert_target_files,
                                         get_recent_dirs)
from elasticsearch import Elasticsearch, helpers


def check_index_exists(es_client: Elasticsearch, index: str) -> bool:
    return es_client.indices.exists(index=index).meta.status == 200


def set_refresh_interval(es_client: Elasticsearch, index: str, interval: str) -> None:
    es_client.indices.put_settings(
        index=index,
        body={"index": {"refresh_interval": interval}},
    )


def bulk_insert_to_es(config: Config, jsonl_files: List[Path], accession_type: AccessionType) -> None:
    es_client = Elasticsearch(config.es_url)

    if not check_index_exists(es_client, accession_type):
        raise Exception(f"Index '{accession_type}' does not exist.")

    DocsClass = BioProject if accession_type == "bioproject" else BioSample

    def _generate_es_bulk_actions(file: Path) -> Iterator[Dict[str, Any]]:
        with file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line == "":
                    continue
                doc = DocsClass.model_validate_json(line)
                yield {
                    "_op_type": "index",
                    "_index": accession_type,
                    "_id": doc.identifier,
                    "_source": doc.model_dump(by_alias=True),
                }

    set_refresh_interval(es_client, accession_type, "-1")

    failed_docs: List[Dict[str, Any]] = []

    try:
        for file in jsonl_files:
            LOGGER.info("Inserting file: %s", file.name)
            # helpers の内部実装的に、500 ずつで bulk insert される
            _success, failed = helpers.bulk(
                es_client,
                _generate_es_bulk_actions(file),
                stats_only=False,
                raise_on_error=False,
                max_retries=3,
                request_timeout=300,
            )
            failed_docs.extend(failed)  # type: ignore
    finally:
        set_refresh_interval(es_client, accession_type, "1s")

    if failed_docs:
        LOGGER.error("Failed to insert some docs: \n%s", failed_docs)


# === CLI implementation ===


class Args(BaseModel):
    latest_dir: Optional[Path] = None
    prior_dir: Optional[Path] = None
    dry_run: bool = False
    debug: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Bulk insert JSONL files containing BioProject data into Elasticsearch.
            This script also includes logic to search for the files to be inserted.
            Use the --dry-run option to execute only the search process without performing the actual insertion.
        """
    )

    parser.add_argument(
        "--work-dir",
        help="""\
            The base directory where the script finds the JSONL files to be inserted.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--latest-dir",
        help="""\
            The directory where the latest JSONL files are stored.
            If not specified, the latest JSONL files are searched for in the work_dir.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--prior-dir",
        help="""\
                The directory where the prior JSONL files are stored.
                If not specified, the prior JSONL files are searched for in the work_dir.
            """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--es-url",
        help="The URL of the Elasticsearch server to insert the data into.",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        help="Execute only the search process without performing the actual insertion.",
        action="store_true",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.work_dir is not None:
        config.work_dir = Path(parsed_args.work_dir)
        config.work_dir.mkdir(parents=True, exist_ok=True)
    if parsed_args.es_url is not None:
        config.es_url = parsed_args.es_url
    latest_dir = None
    if parsed_args.latest_dir is not None:
        latest_dir = Path(parsed_args.latest_dir)
        if not latest_dir.exists():
            raise FileNotFoundError(f"Directory not found: {latest_dir}")
    prior_dir = None
    if parsed_args.prior_dir is not None:
        prior_dir = Path(parsed_args.prior_dir)
        if not prior_dir.exists():
            raise FileNotFoundError(f"Directory not found: {prior_dir}")

    return config, Args(
        latest_dir=latest_dir,
        prior_dir=prior_dir,
        dry_run=parsed_args.dry_run,
        debug=parsed_args.debug,
    )


def main() -> None:
    LOGGER.info("Bulk inserting BioProject data into Elasticsearch")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    latest_dir, prior_dir = get_recent_dirs(config.work_dir.joinpath(BP_JSONL_DIR_NAME), args.latest_dir, args.prior_dir)
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

    bulk_insert_to_es(config, jsonl_files, "bioproject")

    LOGGER.info("Finished inserting BioProject data into Elasticsearch")


if __name__ == "__main__":
    main()
