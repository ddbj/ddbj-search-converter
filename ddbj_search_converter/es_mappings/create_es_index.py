import argparse
import json
import sys
from pathlib import Path
from typing import Any, List, Literal, Tuple

from pydantic import BaseModel

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from elasticsearch import Elasticsearch

IndexName = Literal["bioproject", "biosample", "sra", "jga"]
SRA_INDEXES = ["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"]
JGA_INDEXES = ["jga-dac", "jga-dataset", "jga-policy", "jga-study"]


SETTINGS = {
    "index": {
        "refresh_interval": "1s",
        "mapping.nested_objects.limit": 100000,
    }
}


def load_mapping(index: IndexName) -> Any:
    here = Path(__file__).resolve().parent
    mapping_file = here.joinpath(f"{index}_mapping.json")
    with mapping_file.open("r", encoding="utf-8") as f:
        return json.load(f)


def create_es_index(config: Config, index: IndexName) -> None:
    es = Elasticsearch(config.es_url)
    try:
        if es.indices.exists(index=index):
            raise Exception(f"Index '{index}' already exists.")
        mapping = load_mapping(index)
        mapping["settings"] = SETTINGS
        if index == "sra":
            for sra_index in SRA_INDEXES:
                es.indices.create(index=sra_index, body=mapping)
        elif index == "jga":
            for jga_index in JGA_INDEXES:
                es.indices.create(index=jga_index, body=mapping)
        else:
            es.indices.create(index=index, body=mapping)
    except Exception as e:
        LOGGER.error("Failed to create index '%s': %s", index, e)
        raise


# === CLI implementation ===


class Args(BaseModel):
    index: IndexName


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create Elasticsearch index.
        """
    )

    parser.add_argument(
        "--index",
        help="""\
            Index name to create (required, bioproject, biosample, sra, or, jga)
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--es-url",
        help="The URL of the Elasticsearch server",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.index is None:
        raise Exception("Argument '--index' is required.")
    index = parsed_args.index
    if index not in ["bioproject", "biosample", "sra", "jga"]:
        raise Exception("Argument '--index' must be 'bioproject', 'biosample', 'sra', or 'jga'.")
    if parsed_args.es_url is not None:
        config.es_url = parsed_args.es_url
    if parsed_args.debug:
        config.debug = True

    return config, Args(
        index=index,
    )


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)

    LOGGER.info("Creating Elasticsearch index")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    LOGGER.info("Creating index '%s'", args.index)
    create_es_index(config, args.index)

    LOGGER.info("Done.")


if __name__ == "__main__":
    main()
