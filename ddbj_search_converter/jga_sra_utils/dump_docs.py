import json

from ddbj_search_converter.config import LOGGER, get_config
from elasticsearch import Elasticsearch, helpers

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

    LOGGER.info("Dumping documents")

    for index_name in INDEX_NAMES:
        LOGGER.info("Dumping mapping for index: %s", index_name)
        output_dir = config.work_dir.joinpath(f"jga_sra/{index_name}_jsonld")
        output_dir.mkdir(parents=True, exist_ok=True)
        buffer = []
        count = 1
        docs = helpers.scan(
            prev_es_client,
            index=index_name,
            query={"query": {"match_all": {}}},
        )

        for doc in docs:
            if "_source" not in doc:
                continue
            buffer.append(json.dumps(doc["_source"], ensure_ascii=False))
            if len(buffer) == 10000:
                print(len(buffer))
                output_file = output_dir.joinpath(f"{index_name}_{count}.jsonl")
                LOGGER.info("Writing to %s", output_file)
                with output_file.open("w", encoding="utf-8") as f:
                    f.write("\n".join(buffer))
                buffer = []
                count += 1

        if len(buffer) > 0:
            output_file = output_dir.joinpath(f"{index_name}_{count}.jsonl")
            LOGGER.info("Writing to %s", output_file)
            with output_file.open("w", encoding="utf-8") as f:
                f.write("\n".join(buffer))

    LOGGER.info("Finished dumping documents")


if __name__ == "__main__":
    main()
