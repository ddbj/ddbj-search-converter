import json
from pathlib import Path
from typing import Any, Dict, Iterator, List

from ddbj_search_converter.config import LOGGER, get_config
from elasticsearch import Elasticsearch, helpers

INDEX_NAMES = [
    "jga-dac",
    "jga-dataset",
    "jga-policy",
    "jga-study",
    "sra-analysis",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-study",
    "sra-submission",
]


def set_refresh_interval(es_client: Elasticsearch, index: str, interval: str) -> None:
    es_client.indices.put_settings(
        index=index,
        body={"index": {"refresh_interval": interval}},
    )


def _generate_es_bulk_actions(file: Path, index: str) -> Iterator[Dict[str, Any]]:
    with file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line == "":
                continue
            doc = json.loads(line)
            yield {
                "_op_type": "index",
                "_index": index,
                "_id": doc["identifier"],
                "_source": line,
            }


def main() -> None:
    config = get_config()
    es_client = Elasticsearch(config.es_url)

    LOGGER.info("Loading documents")

    for index_name in INDEX_NAMES:
        LOGGER.info("Loading docs for index: %s", index_name)
        jsonl_dir = config.work_dir.joinpath(f"jga_sra/{index_name}_jsonld")
        jsonl_files = list(jsonl_dir.glob("*.jsonl"))

        set_refresh_interval(es_client, index_name, "-1")
        failed_docs: List[Dict[str, Any]] = []

        try:
            for file in jsonl_files:
                LOGGER.info("Inserting file: %s", file.name)
                # helpers の内部実装的に、500 ずつで bulk insert される
                _success, failed = helpers.bulk(
                    es_client,
                    _generate_es_bulk_actions(file, index_name),
                    stats_only=False,
                    raise_on_error=False,
                    max_retries=3,
                    request_timeout=300,
                )
                failed_docs.extend(failed)  # type: ignore
        finally:
            set_refresh_interval(es_client, index_name, "1s")

        if failed_docs:
            LOGGER.error("Failed to insert some docs for index %s: \n%s", index_name, failed_docs)

    LOGGER.info("Finished loading documents")


if __name__ == "__main__":
    main()
