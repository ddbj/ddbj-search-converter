import argparse
import sys
from typing import List, Tuple

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from elasticsearch import Elasticsearch

INDEXES = ["jga-dac", "jga-dataset", "jga-policy", "jga-study",
           "sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"]


def delete_es_index(config: Config) -> None:
    es = Elasticsearch(config.es_url)
    print("Elasticsearch URL:", config.es_url)
    for index in INDEXES:
        try:
            if es.indices.exists(index=index):
                es.indices.delete(index=index)
                LOGGER.info("Deleted index: %s", index)
            else:
                LOGGER.warning("Index '%s' does not exist.", index)
        except Exception as e:
            LOGGER.error("Failed to delete index '%s': %s", index, e)
            raise


# === CLI implementation ===


def parse_args(args: List[str]) -> Tuple[Config, None]:
    parser = argparse.ArgumentParser(
        description="Delete Elasticsearch indexes",
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

    config = get_config()
    if parsed_args.es_url is not None:
        config.es_url = parsed_args.es_url
    if parsed_args.debug:
        config.debug = True

    return config, None


def main() -> None:
    config, _ = parse_args(sys.argv[1:])
    set_logging_level(config.debug)

    LOGGER.info("Deleting Elasticsearch indexes")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))

    delete_es_index(config)

    LOGGER.info("Finished deleting Elasticsearch indexes")
