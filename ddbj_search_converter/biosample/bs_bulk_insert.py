"""\

"""
from pathlib import Path
from typing import Any, Dict, Iterator, List

from elasticsearch import Elasticsearch, helpers

from ddbj_search_converter.config import LOGGER, Config
from ddbj_search_converter.schema import BioProject


def bulk_insert_to_es(config: Config, jsonl_files: List[Path]) -> None:
    es_client = Elasticsearch(config.es_url)

    def _generate_es_bulk_actions(file: Path) -> Iterator[Dict[str, Any]]:
        with file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line == "":
                    continue
                doc = BioProject.model_validate_json(line)
                yield {"index": {"_index": "biosample"}, "_id": doc.identifier}
                yield doc.model_dump()

    failed_docs: List[Dict[str, Any]] = []

    for file in jsonl_files:
        # helpers の内部実装的に、500 ずつで bulk insert される
        _success, failed = helpers.bulk(
            es_client,
            _generate_es_bulk_actions(file),
            stats_only=False,
            raise_on_error=False
        )
        failed_docs.extend(failed)  # type: ignore

    if failed_docs:
        LOGGER.error("Failed to insert some docs: \n%s", failed_docs)
