import argparse
import csv
import re
import sys
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

import xmltodict
from pydantic import BaseModel

from ddbj_search_converter.cache_db.jga_relation_ids import \
    get_relation_ids_bulk
from ddbj_search_converter.config import (JGA_JSONL_DIR_NAME, LOGGER, TODAY,
                                          Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.schema import JGA, Distribution, Organism
from ddbj_search_converter.utils import format_date, to_xref

IndexName = Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"]
INDEX_NAMES: List[IndexName] = ["jga-study", "jga-dataset", "jga-dac", "jga-policy"]
XML_KEYS = {
    "jga-study": ["STUDY_SET", "STUDY"],  # to List[Dict[str, Any]]
    "jga-dataset": ["DATASETS", "DATASET"],  # to List[Dict[str, Any]]
    "jga-dac": ["DAC_SET", "DAC"],  # to Dict[str, Any]
    "jga-policy": ["POLICY_SET", "POLICY"]  # to List[Dict[str, Any]]
}
REL_MAP = {
    "jga-study": ["study_dataset", "study_dac", "study_policy"],
    "jga-dataset": ["dataset_study", "dataset_dac", "dataset_policy"],
    "jga-dac": ["dac_study", "dac_dataset", "dac_policy"],
    "jga-policy": ["policy_study", "policy_dataset", "policy_dac"],
}


def generate_jga_jsonl(
    config: Config,
    index_name: IndexName,
) -> None:
    xml_file_path = config.jga_base_path.joinpath(f"{index_name}.xml")
    if not xml_file_path.exists():
        raise FileNotFoundError(f"XML file for {index_name} does not exist: {xml_file_path}")

    xml_metadata = _load_xml_file(xml_file_path)

    try:
        entries = xml_metadata[XML_KEYS[index_name][0]][XML_KEYS[index_name][1]]
        if index_name == "jga-dac":
            entries = [entries]  # only one DAC entry, wrap it in a list
        if not isinstance(entries, list):
            raise ValueError(f"Expected a list for {index_name}, but got: {type(entries)}")
    except Exception as e:
        raise ValueError(f"Failed to parse XML for {index_name}: {e}") from e

    LOGGER.info("Processing %s entries from XML file: %s", len(entries), xml_file_path)

    jga_instances: Dict[str, JGA] = {}
    for entry in entries:
        jga_instance = jga_metadata_to_jga_instance(entry, index_name)
        jga_instances[jga_instance.identifier] = jga_instance

    # Update dbXref, dbXrefs, and dates
    accessions = list(jga_instances.keys())
    relation_ids_map = get_joined_relation_ids(config, index_name, accessions)
    date_map = load_date_map(config, index_name)
    for accession in accessions:
        if accession in relation_ids_map:
            relation_ids = relation_ids_map[accession]
            jga_instance = jga_instances[accession]
            jga_instance.dbXref = [to_xref(rel_id) for rel_id in relation_ids]
            jga_instance.dbXrefs = deepcopy(jga_instance.dbXref)
        if accession in date_map:
            date_created, date_published, date_modified = date_map[accession]
            jga_instance = jga_instances[accession]
            jga_instance.dateCreated = date_created
            jga_instance.datePublished = date_published
            jga_instance.dateModified = date_modified

    # Write to JSONL file
    output_file = config.work_dir.joinpath(JGA_JSONL_DIR_NAME).joinpath(TODAY).joinpath(f"{index_name}.jsonl")
    write_jsonl(output_file, list(jga_instances.values()))
    LOGGER.info("Wrote %d entries to JSONL file: %s", len(jga_instances), output_file)


def _load_xml_file(xml_file_path: Path) -> Dict[str, Any]:
    with xml_file_path.open("rb") as f:
        xml_bytes = f.read()
    xml_metadata = xmltodict.parse(xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False)

    return xml_metadata


def jga_metadata_to_jga_instance(
    jga_metadata: Dict[str, Any],  # A single entry from the XML
    index_name: IndexName,
) -> JGA:
    accession = jga_metadata["accession"]
    jga_instance = JGA(
        identifier=accession,
        properties=jga_metadata,
        distribution=[Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl=f"https://ddbj.nig.ac.jp/search/entry/{index_name}/{accession}.json",
        )],
        isPartOf="jga",
        type=index_name,
        name=_parse_name(accession, jga_metadata),
        url=f"https://ddbj.nig.ac.jp/search/entry/{index_name}/{accession}",
        organism=Organism(
            identifier="9606",
            name="Homo sapiens",
        ),
        title=_parse_title(jga_metadata, index_name),
        description=_parse_description(jga_metadata, index_name),
        dbXref=[],  # Update after
        dbXrefs=[],  # Update after
        sameAs=[],
        status="public",
        visibility="controlled-access",
        dateCreated=None,  # Update after
        dateModified=None,  # Update after
        datePublished=None,  # Update after
    )

    return jga_instance


def _parse_name(accession: str, jga_metadata: Dict[str, Any]) -> Optional[str]:
    return jga_metadata.get("alias", accession)  # type: ignore


def _parse_title(jga_metadata: Dict[str, Any], index_name: IndexName) -> Optional[str]:
    if index_name == "jga-study":
        return jga_metadata.get("DESCRIPTOR", {}).get("STUDY_TITLE", None)  # type: ignore
    if index_name == "jga-dataset":
        return jga_metadata.get("TITLE", None)  # type: ignore
    if index_name == "jga-policy":
        return jga_metadata.get("TITLE", None)  # type: ignore

    return None


def _parse_description(jga_metadata: Dict[str, Any], index_name: IndexName) -> Optional[str]:
    if index_name == "jga-study":
        return jga_metadata.get("DESCRIPTOR", {}).get("STUDY_ABSTRACT", None)  # type: ignore
    if index_name == "jga-dataset":
        return jga_metadata.get("DESCRIPTION", None)  # type: ignore

    return None


def get_joined_relation_ids(config: Config, index_name: IndexName, accessions: List[str]) -> Dict[str, List[str]]:
    relation_ids: Dict[str, Set[str]] = defaultdict(set)
    for rel_name in REL_MAP[index_name]:
        rel_map = get_relation_ids_bulk(config, rel_name, accessions)
        for accession, ids in rel_map.items():
            if ids:
                relation_ids[accession].update(ids)

    # Convert sets to sorted lists
    return {accession: sorted(ids) for accession, ids in relation_ids.items()}


_TZ_FIX = re.compile(r"([+-]\d{2})$")
_FRAC_FIX = re.compile(r'(\.\d{1,6})([+-])')     # .63+09:00 → .630000+09:00


def _format_date(value: str) -> str:
    """\
    CSV に入っている日付が、独自規格で ISO 8601 形式ではないため、ISO 8601 形式に変換する。
    """
    fixed_value = value.strip().replace(" ", "T")
    if _TZ_FIX.search(fixed_value):
        fixed_value = _TZ_FIX.sub(r"\1:00", fixed_value)
    fixed_value = fixed_value.replace("Z", "+00:00")
    fixed_value = _FRAC_FIX.sub(lambda m: f"{m.group(1).ljust(7, '0')}{m.group(2)}", fixed_value)
    date = datetime.fromisoformat(fixed_value)
    return format_date(date)  # type: ignore


def load_date_map(config: Config, index_name: IndexName) -> Dict[str, Tuple[str, str, str]]:
    # accession, dateCreated, datePublished, dateModified
    # dateFormat in CSV: 2014-07-07 14:00:37.208+09
    # return Format: utils.format_date (i.e., %Y-%m-%dT%H:%M:%SZ)

    type_name = index_name.replace("jga-", "")
    csv_file_path = config.jga_base_path.joinpath(f"{type_name}.date.csv")
    if not csv_file_path.exists():
        raise FileNotFoundError(f"CSV file for {index_name} date map does not exist: {csv_file_path}")
    date_map: Dict[str, Tuple[str, str, str]] = {}
    with csv_file_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if len(row) != 4:
                LOGGER.warning("Invalid row in date map CSV: %s", row)
                continue
            accession, date_created, date_published, date_modified = row
            date_map[accession] = (
                _format_date(date_created),
                _format_date(date_published),
                _format_date(date_modified)
            )

    return date_map


def write_jsonl(output_file: Path, docs: List[JGA], is_append: bool = False) -> None:
    """\
    - memory のほうが多いと見越して、一気に書き込む
    """
    mode = "a" if is_append else "w"
    is_file_exists = output_file.exists()
    with output_file.open(mode=mode, encoding="utf-8") as f:
        if is_append and is_file_exists:
            f.write("\n")
        f.write("\n".join(doc.model_dump_json(by_alias=True) for doc in docs))


# === CLI implementation ===


class Args(BaseModel):
    pass


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Generate JGA JSON-lines fires from JGA XML files.
        """
    )

    parser.add_argument(
        "--work-dir",
        help=f"""\
            The base directory where the script outputs are stored.
            By default, it is set to $PWD/ddbj_search_converter_results.
            The resulting JSON-lines file will be stored in {{work_dir}}/{JGA_JSONL_DIR_NAME}/{{date}}.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--debug",
        help="Enable debug mode.",
        action="store_true",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.work_dir is not None:
        config.work_dir = Path(parsed_args.work_dir)
        config.work_dir.mkdir(parents=True, exist_ok=True)

    if parsed_args.debug:
        config.debug = True

    return config, Args()


def main() -> None:
    LOGGER.info("Start generating JGA JSONL files")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    output_dir = config.work_dir.joinpath(JGA_JSONL_DIR_NAME).joinpath(TODAY)
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Output directory: %s", output_dir)
    # generate_dra_jsonl(
    #     config=config,
    #     output_dir=output_dir,
    #     batch_size=args.batch_size,
    #     parallel_num=args.parallel_num,
    # )
    for index_name in INDEX_NAMES:
        generate_jga_jsonl(config, index_name)

    LOGGER.info("Finished generating JGA JSONL files")


if __name__ == "__main__":
    main()
