import json

from ddbj_search_converter.config import get_config
from elasticsearch import Elasticsearch

PREV_ES_URL = "http://ddbjld-elasticsearch:9200"


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


def main() -> None:
    config = get_config()
    prev_es_client = Elasticsearch(PREV_ES_URL)
    mappings = prev_es_client.indices.get_mapping()

    output_dir = config.work_dir.joinpath("jga_sra/mappings")
    output_dir.mkdir(parents=True, exist_ok=True)

    for index_name in INDEX_NAMES:
        with output_dir.joinpath(f"{index_name}_mapping.json").open("w", encoding="utf-8") as f:
            json.dump(mappings[index_name], f, indent=2)


if __name__ == "__main__":
    main()
