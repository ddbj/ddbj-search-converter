import json
from time import sleep

from ddbj_search_converter.config import LOGGER, get_config
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


def set_refresh_interval(es_client: Elasticsearch, index: str, interval: str) -> None:
    es_client.indices.put_settings(
        index=index,
        body={"index": {"refresh_interval": interval}},
    )


def main() -> None:
    try:
        config = get_config()
        es_client = Elasticsearch(config.es_url)

        index_to_task_id = {}

        for index_name in INDEX_NAMES:
            set_refresh_interval(es_client, index_name, "-1")
            res = es_client.reindex(
                source={
                    "remote": {
                        "host": PREV_ES_URL,
                    },
                    "index": index_name,
                },
                dest={
                    "index": index_name,
                },
                wait_for_completion=False,
            )
            index_to_task_id[index_name] = res["task"]

        LOGGER.info("=== Task IDs ===")
        LOGGER.info(json.dumps(index_to_task_id, indent=2))

        sleep(5)

        progress = {
            index_name: 0
            for index_name in INDEX_NAMES
        }

        while True:
            for index_name, task_id in index_to_task_id.items():
                res = es_client.tasks.get(task_id=task_id)
                created = res["task"]["status"]["created"]
                updated = res["task"]["status"]["updated"]
                total = res["task"]["status"]["total"]
                percent = (created + updated) / total * 100
                percent = round(percent, 2)
                if res["completed"]:
                    percent = 100

                progress[index_name] = percent

            LOGGER.info("=== Progress ===")
            LOGGER.info(json.dumps(progress, indent=2))

            if all(percent == 100 for percent in progress.values()):
                break

            sleep(15)

        LOGGER.info("Done")
    finally:
        for index_name in INDEX_NAMES:
            set_refresh_interval(es_client, index_name, "1")


if __name__ == "__main__":
    main()
