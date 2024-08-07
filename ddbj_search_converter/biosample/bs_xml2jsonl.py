import argparse
import glob
import json
import shutil
import sys
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import xmltodict
from lxml import etree
from pydantic import BaseModel

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.dblink.create_dblink_db import \
    create_db_engine as create_dblink_db_engine
from ddbj_search_converter.dblink.get_dblink import DBXRef, select_dbxref
from ddbj_search_converter.dblink.id_relation_db import \
    create_db_engine as create_accession_db_engine

BATCH_SIZE = 10000
DDBJ_BIOSAMPLE_NAME = "ddbj_biosample"


# === type def. ===


class Organism(BaseModel):
    identifier: str
    name: str


class CommonDocument(BaseModel):
    accession: Optional[str]
    identifier: Optional[str]
    isPartOf: str = "BioSample"
    type: str = "biosample"
    organism: Optional[Organism]
    title: str
    description: str
    status: str = "public"
    visibility: str = "unrestricted-access"
    dbXrefs: List[DBXRef]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


# === functions ===


class Args(BaseModel):
    xml_dir: Path
    output_dir: Path


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(description="Convert BioSample XML to JSON-Lines")

    parser.add_argument(
        "xml_dir",
        help="Directory containing BioSample XML files",
    )
    parser.add_argument(
        "output_dir",
        help="Directory to output JSON-Lines files",
    )
    parser.add_argument(
        "--accessions-db-path",
        nargs="?",
        default=None,
        help="Path to the SQLite database file containing accession relations (default: ./converter_results/sra_accessions.sqlite)",
    )
    parser.add_argument(
        "--dblink-db-path",
        nargs="?",
        default=None,
        help="Path to the SQLite database file containing dblink relations (default: ./converter_results/dblink.sqlite)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.debug:
        config.debug = parsed_args.debug
    if parsed_args.accessions_db_path is not None:
        config.accessions_db_path = Path(parsed_args.accessions_db_path)
        if not config.accessions_db_path.exists():
            LOGGER.error("Accessions DB file does not exist: %s", config.accessions_db_path)
            sys.exit(1)
    if parsed_args.dblink_db_path is not None:
        config.dblink_db_path = Path(parsed_args.dblink_db_path)
        if not config.dblink_db_path.exists():
            LOGGER.error("DBLink DB file does not exist: %s", config.dblink_db_path)
            sys.exit(1)

    # Args の型変換と validation
    if not parsed_args.xml_dir.exists():
        LOGGER.error("Input XML directory does not exist: %s", parsed_args.xml_dir)
        sys.exit(1)
    if parsed_args.output_dir.exists():
        LOGGER.info("Output directory %s already exists, will overwrite files", parsed_args.output_dir)
        shutil.rmtree(parsed_args.output_dir)
    parsed_args.output_dir.mkdir(parents=True, exist_ok=True)

    return (config, Args(
        xml_dir=parsed_args.xml_dir,
        output_dir=parsed_args.output_dir,
    ))


def xml2jsonl(config: Config, xml_file: Path, output_file: Path, is_core: bool) -> None:
    accessions_engine = create_accession_db_engine(config)
    dblink_engine = create_dblink_db_engine(config)

    context = etree.iterparse(xml_file, tag="BioSample")
    docs: List[Dict[str, Any]] = []
    batch_count = 0
    for _events, element in context:
        if element.tag == "BioSample":
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            sample = metadata["BioSample"]

            doc = {
                "properties": sample
            }

            id_ = sample.get("accession", None)

            # 関係データを取得と project の更新
            common_doc = CommonDocument(
                accession=_parse_accession(sample, is_core),
                identifier=id_,
                isPartOf="BioSample",
                type="biosample",
                organism=_parse_organism(sample),
                title=_parse_title(sample),
                description=_parse_description(sample),
                status="public",
                visibility="unrestricted-access",
                dbXrefs=[] if id_ is None else select_dbxref(
                    accessions_engine,
                    dblink_engine,
                    id_,
                    "biosample",
                ),
                dateCreated=sample.get("submission_date", None),
                dateModified=sample.get("last_update", None),
                datePublished=sample.get("publication_date", None),
            )
            _update_owner_name(sample)
            _update_models(sample)

            doc.update(common_doc.dict())
            docs.append(doc)

            batch_count += 1
            if batch_count >= BATCH_SIZE:
                jsonl = docs_to_jsonl(docs)
                dump_to_file(output_file, jsonl)
                batch_count = 0
                docs = []

        # メモリリークを防ぐために要素をクリアする
        clear_element(element)

    # 残りのデータを出力
    if len(docs) > 0:
        jsonl = docs_to_jsonl(docs)
        dump_to_file(output_file, jsonl)


def _parse_accession(sample: Dict[str, Any], is_core: bool) -> Optional[str]:
    try:
        if is_core:
            id_obj = sample["Ids"]["Id"]
            if isinstance(id_obj, list):
                # doc["Ids"]["Id"] の namespace == BioSample の content を取得する
                bs_id = list(filter(lambda x: x["namespace"] == "BioSample", id_obj))
                return str(bs_id[0]["content"])
            elif isinstance(id_obj, dict):
                return str(id_obj["content"])
        else:
            return str(sample["accession"])
    except Exception as e:
        LOGGER.debug("Failed to parse accession from %s: %s", sample, e)
        return None

    return None


def _parse_organism(sample: Dict[str, Any]) -> Optional[Organism]:
    try:
        return Organism(
            identifier=sample["Organism"]["taxonomy_id"],
            name=sample["Organism"]["taxonomy_name"],
        )
    except Exception as e:
        LOGGER.debug("Failed to parse organism from %s: %s", sample, e)
        return None


def _parse_title(sample: Dict[str, Any]) -> str:
    # Description の子要素を DDBJ 共通 object の値に変換する
    try:
        return str(sample["Description"]["Title"])
    except Exception as e:
        LOGGER.debug("Failed to parse title from %s: %s", sample, e)
        return ""


def _parse_description(sample: Dict[str, Any]) -> str:
    # Description の子要素を DDBJ 共通 object の値に変換する
    try:
        comment_obj = sample["Description"]["Comment"]["Paragraph"]
        if isinstance(comment_obj, list):
            return str(comment_obj[0])
        elif isinstance(comment_obj, str):
            return str(comment_obj)
        else:
            return ""
    except Exception as e:
        LOGGER.debug("Failed to parse description from %s: %s", sample, e)
        return ""


def _update_owner_name(sample: Dict[str, Any]) -> None:
    # Owner.Name が文字列が記述されているケースの処理
    try:
        owner_name = sample["Owner"]["Name"]
        # owner_name の型が str であれば {"abbreviation": val, "content": val} に置き換える
        if isinstance(owner_name, str):
            sample["Owner"]["Name"] = {"abbreviation": owner_name, "content": owner_name}
    except Exception as e:
        LOGGER.debug("Failed to update owner name from %s: %s", sample, e)


def _update_models(sample: Dict[str, Any]) -> None:
    # Models.Modelにobjectが記述されているケースの処理
    try:
        models_obj = sample["Models"]["Model"]
        if isinstance(models_obj, dict):
            # Models.Model がオブジェクトの場合そのまま渡す
            sample["Models"]["Model"] = models_obj.get("content", None)
        elif isinstance(models_obj, list):
            # Models.Model がリストの場合、要素をそれぞれオブジェクトに変換する
            sample["Models"]["Model"] = [{"content": x} for x in models_obj]
        elif isinstance(models_obj, str):
            # Models.Model の値が文字列の場合 {"content": value} に変換する
            sample["Models"]["Model"] = [{"content": models_obj}]
    except Exception as e:
        LOGGER.debug("Failed to update models from %s: %s", sample, e)


def docs_to_jsonl(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """\
    JSON-Lines だが、実際には、ES へと bulk insert するための形式となっている
    そのため、index と body が交互になっている
    """
    jsonl = []
    for doc in docs:
        jsonl.append({"index": {"_index": "biosample", "_id": doc["accession"]}})
        jsonl.append(doc)

    return jsonl


def dump_to_file(output_file: Path, jsonl: List[Dict[str, Any]]) -> None:
    with output_file.open("a") as f:
        for line in jsonl:
            f.write(json.dumps(line) + "\n")


def clear_element(element: Any) -> None:
    try:
        element.clear()
        while element.getprevious() is not None:
            try:
                del element.getparent()[0]
            except Exception as e:
                LOGGER.debug("Failed to clear element: %s", e)
    except Exception as e:
        LOGGER.debug("Failed to clear element: %s", e)


def is_ddbj_biosample(file: Path) -> bool:
    return DDBJ_BIOSAMPLE_NAME in file.name


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.info("Start converting BioSample XML files in %s to JSON-Lines", args.xml_dir)
    LOGGER.info("Config: %s", config.model_dump())
    LOGGER.info("Args: %s", args.model_dump())

    input_files = [Path(f) for f in glob.glob(args.xml_dir.joinpath("*.xml").as_posix())]
    output_files = [args.output_dir.joinpath(f"{f.stem}.jsonl") for f in input_files]

    error_flag = False
    with Pool(config.process_pool_size) as p:
        try:
            p.starmap(xml2jsonl, [(
                config,
                input_file,
                output_file,
                is_ddbj_biosample(input_file)
            ) for input_file, output_file in zip(input_files, output_files)])
        except Exception as e:
            LOGGER.error("Failed to convert BioSample XML files: %s", e)
            error_flag = True

    if error_flag:
        LOGGER.error("Failed to convert BioSample XML files")
        sys.exit(1)

    LOGGER.info("Finished converting BioSample XML files in %s to JSON-Lines", args.xml_dir)


if __name__ == "__main__":
    main()
