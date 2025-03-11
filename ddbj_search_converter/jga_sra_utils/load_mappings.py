import json

from ddbj_search_converter.config import get_config
from elasticsearch import Elasticsearch

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


SETTINGS = {
    "index": {
        "refresh_interval": "1s",
        "mapping.nested_objects.limit": 100000,
    }
}


def main() -> None:
    config = get_config()
    es_client = Elasticsearch(config.es_url)

    mappings_dir = config.work_dir.joinpath("jga_sra/mappings")

    for index_name in INDEX_NAMES:
        with mappings_dir.joinpath(f"{index_name}_mapping.json").open("r", encoding="utf-8") as f:
            mapping = json.load(f)
            if "type" in mapping["mappings"]["properties"]:
                mapping["mappings"]["properties"]["type"] = {"type": "keyword"}
            if "organism" in mapping["mappings"]["properties"]:
                if "name" in mapping["mappings"]["properties"]["organism"]["properties"]:
                    mapping["mappings"]["properties"]["organism"]["properties"]["name"] = {"type": "keyword"}
            mapping["settings"] = SETTINGS
            es_client.indices.create(index=index_name, body=mapping)


if __name__ == "__main__":
    main()
