import argparse
import json
import sys
from pathlib import Path
from typing import List, Tuple

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.schema import JGA, SRA

INDEXES = ["jga-dac", "jga-dataset", "jga-policy", "jga-study",
           "sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"]

PREV_DOCS_BASE_DIR = Path("/home/w3ddbjld/ddbj-search-bkp-20250219/ddbj-search-converter-results")


def listing_prev_files(index: str) -> List[Path]:
    jsonl_dir = PREV_DOCS_BASE_DIR.joinpath(f"jga_sra/{index}_jsonld")
    jsonl_files = list(jsonl_dir.glob("*.jsonl"))
    return jsonl_files


def rewrite_docs(config: Config, index: str) -> None:
    new_jsonl_dir = config.work_dir.joinpath("jga_sra")
    new_jsonl_dir.mkdir(parents=True, exist_ok=True)

    prev_files = listing_prev_files(index)
    LOGGER.info("Found %d files for index: %s", len(prev_files), index)
    for i, prev_jsonl_file in enumerate(prev_files):
        LOGGER.info("Rewriting file %d/%d: %s", i + 1, len(prev_files), prev_jsonl_file.name)
        docs = []
        with prev_jsonl_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line == "":
                    continue
                prev_doc = json.loads(line)
                if prev_doc["identifier"].startswith("D"):
                    continue

                if index.startswith("jga"):
                    prev_doc["dbXref"] = prev_doc.pop("dbXrefs", [])
                    organism_identifier = prev_doc.get("organism", {}).get("identifier")
                    if isinstance(organism_identifier, int):
                        prev_doc["organism"]["identifier"] = str(organism_identifier)
                    if "title" not in prev_doc:
                        prev_doc["title"] = prev_doc.get("identifier")
                    if "description" not in prev_doc:
                        prev_doc["description"] = None
                    if "sameAs" not in prev_doc:
                        prev_doc["sameAs"] = []
                    if prev_doc["sameAs"] is None:
                        prev_doc["sameAs"] = []
                    prev_doc["visibility"] = "controlled-access"

                    new_doc = JGA.model_validate(prev_doc)
                else:
                    prev_doc["dbXref"] = prev_doc.pop("dbXrefs", [])
                    if prev_doc["organism"] is not None:
                        organism_identifier = prev_doc.get("organism", {}).get("identifier")
                        if isinstance(organism_identifier, int):
                            prev_doc["organism"]["identifier"] = str(organism_identifier)
                    if "sameAs" not in prev_doc:
                        prev_doc["sameAs"] = []
                    if prev_doc["sameAs"] is None:
                        prev_doc["sameAs"] = []
                    if prev_doc["name"] is None:
                        prev_doc["name"] = prev_doc["identifier"]
                    prev_doc["visibility"] = "unrestricted-access"

                    new_doc = SRA.model_validate(prev_doc)  # type: ignore

                docs.append(new_doc)

        new_jsonl_file = new_jsonl_dir.joinpath(prev_jsonl_file.name)
        with new_jsonl_file.open("w", encoding="utf-8") as f:
            f.write("\n".join(doc.model_dump_json(by_alias=True) for doc in docs))


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

    LOGGER.info("Rewrite docs")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))

    for index_name in INDEXES:
        LOGGER.info("Rewriting docs for index: %s", index_name)
        rewrite_docs(config, index_name)

    LOGGER.info("Finished rewriting docs")
