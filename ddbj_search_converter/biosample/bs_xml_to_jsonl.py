"""\
- BioSample XML を JSON-Lines に変換する
- 生成される JSON-Lines は 1 line が 1 BioSample Accession に対応する
"""
import argparse
import gzip
import shutil
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import xmltodict
from pydantic import BaseModel

from ddbj_search_converter.bioproject.bp_xml_to_jsonl import (
    _iterate_xml_element, listing_tmp_xml_files, split_xml)
from ddbj_search_converter.cache_db.bs_date import get_dates as get_bs_dates
from ddbj_search_converter.cache_db.bs_date import \
    get_dates_bulk as get_bs_dates_bulk
from ddbj_search_converter.cache_db.bs_relation_ids import (
    get_relation_ids, get_relation_ids_bulk)
from ddbj_search_converter.config import (BS_JSONL_DIR_NAME, LOGGER, TODAY,
                                          Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.schema import (Attribute, BioSample, Distribution,
                                          Model, Organism, Package, Xref)
from ddbj_search_converter.utils import to_xref

DEFAULT_BATCH_SIZE = 10000
DEFAULT_PARALLEL_NUM = 64
TMP_XML_DIR_NAME = "tmp_xml"


def xml_to_jsonl(
    config: Config,
    xml_file: Path,
    output_dir: Path,
    is_ddbj: bool,
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
    remove_tmp_dir: bool = False,
    use_existing_tmp_dir: bool = False,
) -> None:
    tmp_xml_dir = output_dir.joinpath(TMP_XML_DIR_NAME)
    tmp_xml_dir.mkdir(parents=True, exist_ok=True)

    if use_existing_tmp_dir:
        if not tmp_xml_dir.exists():
            raise FileNotFoundError(f"Temporary directory not found: {tmp_xml_dir}")
        tmp_xml_files = listing_tmp_xml_files(tmp_xml_dir, is_ddbj, "biosample")
    else:
        # gz ファイルの場合は、一時ファイルに展開してから処理する
        if xml_file.suffix == ".gz":
            LOGGER.info("Extracting gz file: %s", xml_file)
            xml_file = extract_gz(xml_file, config.work_dir)

        # 先に xml を分割してから、並列化して処理する
        LOGGER.info("Splitting XML file: %s", xml_file)
        tmp_xml_files = split_xml(xml_file, tmp_xml_dir, is_ddbj, batch_size, "biosample")

    jsonl_files = [output_dir.joinpath(f"{xml_file.stem}.jsonl") for xml_file in tmp_xml_files]

    LOGGER.info("Starting parallel conversion of XML to JSON Lines. A total of %d JSON Lines files will be generated.", len(tmp_xml_files))

    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures = [
            executor.submit(
                xml_to_jsonl_worker,
                config,
                tmp_xml_file,
                jsonl_file,
                is_ddbj,
            )
            for tmp_xml_file, jsonl_file in zip(tmp_xml_files, jsonl_files)
        ]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                LOGGER.error("Failed to convert XML to JSON-Lines: %s", e)

    # 一時ファイルを削除
    if remove_tmp_dir:
        shutil.rmtree(tmp_xml_dir)


def extract_gz(gz_file: Path, output_dir: Path) -> Path:
    output_file = output_dir.joinpath(gz_file.stem)
    with gzip.open(gz_file, "rb") as f_in:
        with output_file.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return output_file


def xml_to_jsonl_worker(config: Config, xml_file: Path, jsonl_file: Path, is_ddbj: bool) -> None:
    LOGGER.info("Converting XML to JSON-Lines: %s", jsonl_file.name)

    docs: Dict[str, BioSample] = {}
    for xml_element in _iterate_xml_element(xml_file, "biosample"):
        bs_instance = xml_element_to_bs_instance(config, xml_element, is_ddbj, use_db=False)
        docs[bs_instance.identifier] = bs_instance

    # dbXref の一括取得
    relation_ids_map = get_relation_ids_bulk(config, docs.keys())
    for accession, relation_ids in relation_ids_map.items():
        docs[accession].dbXref = sorted(
            [to_xref(id_) for id_ in relation_ids],
            key=lambda x: x.identifier,
        )

    # date の一括取得
    if is_ddbj:
        date_map = get_bs_dates_bulk(config, docs.keys())
        for accession, dates in date_map.items():
            docs[accession].dateCreated = dates[0]
            docs[accession].dateModified = dates[1]
            docs[accession].datePublished = dates[2]

    write_jsonl(jsonl_file, list(docs.values()))


def xml_element_to_bs_instance(config: Config, xml_element: bytes, is_ddbj: bool, use_db: bool = False) -> BioSample:
    metadata = xmltodict.parse(xml_element, attr_prefix="", cdata_key="content", process_namespaces=False)

    sample = metadata["BioSample"]
    accession = _parse_accession(sample, is_ddbj)

    model = _parse_and_update_model(accession, sample)
    package = _parse_and_update_package(accession, sample, model, is_ddbj)

    if is_ddbj:
        if use_db:
            date_created, date_modified, date_published = get_bs_dates(config, accession)
        else:
            # 親関数とかで、bulk で取得し update する
            date_created, date_modified, date_published = None, None, None
    else:
        date_created, date_modified, date_published = _parse_date(sample)

    if use_db:
        dbXref = [to_xref(id_) for id_ in get_relation_ids(config, accession)]  # pylint: disable=C0103
    else:
        # 親関数とかで、bulk で取得し update する
        dbXref = []  # pylint: disable=C0103

    bs_instance = BioSample(
        identifier=accession,
        properties=sample,
        distribution=[Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl=f"https://ddbj.nig.ac.jp/search/entry/biosample/{accession}.json"
        )],
        isPartOf="BioSample",
        type="biosample",
        name=None,
        url=f"https://ddbj.nig.ac.jp/search/entry/biosample/{accession}",
        organism=_parse_organism(accession, sample, is_ddbj),
        title=_parse_title(accession, sample),
        description=_parse_description(accession, sample),
        attributes=_parse_attributes(accession, sample),
        model=model,
        package=package,
        dbXref=dbXref,
        sameAs=_parse_same_as(accession, sample),
        status="public",
        visibility="unrestricted-access",
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )

    # properties の中の object に対して整形を行う
    _update_owner(sample)

    return bs_instance


def _parse_accession(sample: Dict[str, Any], is_ddbj: bool) -> str:
    try:
        if is_ddbj:
            if isinstance(sample["Ids"]["Id"], list):
                accession = next(
                    id_["content"]
                    for id_ in sample["Ids"]["Id"]
                    if id_["namespace"] == "BioSample"
                )
            else:
                accession = sample["Ids"]["Id"]["content"]
        else:
            accession = sample["accession"]
    except Exception as e:
        LOGGER.warning("Failed to parse accession: %s", e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    if isinstance(accession, str):
        return accession
    else:
        LOGGER.error("Failed to parse accession: %s", accession)
        raise ValueError(f"Failed to parse accession: {accession}")


def _parse_organism(accession: str, sample: Dict[str, Any], is_ddbj: bool) -> Optional[Organism]:
    try:
        organism_obj = sample.get("Description", {}).get("Organism", None)
        if organism_obj is None:
            return None

        if is_ddbj:
            name = organism_obj["OrganismName"]
        else:
            name = organism_obj["taxonomy_name"]
        return Organism(
            identifier=organism_obj.get("taxonomy_id", None),
            name=name,
        )
    except Exception as e:
        LOGGER.warning("Failed to parse organism with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def _parse_title(accession: str, sample: Dict[str, Any]) -> Optional[str]:
    try:
        title = sample.get("Description", {}).get("Title", None)
        if title is None:
            return None

        return str(title)
    except Exception as e:
        LOGGER.warning("Failed to parse title with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def _parse_description(accession: str, sample: Dict[str, Any]) -> Optional[str]:
    try:
        description = sample.get("Description", {}).get("Comment", {}).get("Paragraph", None)
        if description is None:
            return None

        if isinstance(description, str):
            return description
        elif isinstance(description, list):
            return ",".join([item for item in description if isinstance(item, str)])
    except Exception as e:
        LOGGER.warning("Failed to parse description with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def _parse_attributes(accession: str, sample: Dict[str, Any]) -> List[Attribute]:
    try:
        attributes = sample.get("Attributes", None)
        if attributes is None:
            return []

        attribute_objs = attributes.get("Attribute", [])
        if isinstance(attribute_objs, dict):
            attribute_objs = [attribute_objs]

        return [Attribute(
            attribute_name=attribute_obj.get("attribute_name", None),
            display_name=attribute_obj.get("display_name", None),
            harmonized_name=attribute_obj.get("harmonized_name", None),
            content=attribute_obj.get("content", None),
        ) for attribute_obj in attribute_objs]
    except Exception as e:
        LOGGER.warning("Failed to parse attribute with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return []


def _parse_and_update_model(accession: str, sample: Dict[str, Any]) -> List[Model]:
    try:
        model_obj = sample.get("Models", {}).get("Model", None)
        if model_obj is None:
            return []

        if isinstance(model_obj, str):
            model_obj = [{"content": model_obj}]
        elif isinstance(model_obj, dict):
            model_obj = [model_obj]

        new_model_obj = [
            {"content": item}
            if isinstance(item, str)
            else {"content": item["content"], **({"version": item["version"]} if "version" in item else {})}  # やばい書き方をしている
            for item in model_obj
        ]
        sample["Models"]["Model"] = new_model_obj

        return [Model(name=item["content"]) for item in new_model_obj]
    except Exception as e:
        LOGGER.warning("Failed to parse model with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return []


def _parse_and_update_package(accession: str, sample: Dict[str, Any], model: List[Model], is_ddbj: bool) -> Optional[Package]:
    try:
        if is_ddbj:
            return Package(
                name=model[0].name,
                display_name=model[0].name,
            )
        else:
            return Package(
                name=sample["Package"]["content"],
                display_name=sample["Package"]["display_name"],
            )
    except Exception as e:
        LOGGER.warning("Failed to parse package with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def _parse_same_as(accession: str, sample: Dict[str, Any]) -> List[Xref]:
    try:
        same_as = []
        sample_objs = sample.get("Ids", {}).get("Id", [])
        if isinstance(sample_objs, dict):
            sample_objs = [sample_objs]
        for sample_obj in sample_objs:
            if accession == sample_obj["content"]:
                continue
            if sample_obj.get("db", None) == "SRA" or sample_obj.get("namespace", None) == "SRA":
                same_as.append(Xref(
                    identifier=sample_obj["content"],
                    type="sra-sample",
                    url=f"https://ddbj.nig.ac.jp/resource/sra-sample/{sample_obj['content']}"
                ))
        return same_as
    except Exception as e:
        LOGGER.warning("Failed to parse sameAs with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return []


def _parse_date(sample: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """\
    Return (dateCreated, dateModified, datePublished)
    """
    date_created = sample.get("submission_date", None)
    date_modified = sample.get("last_update", None)
    date_published = sample.get("publication_date", None)

    return (date_created, date_modified, date_published)


def _update_owner(sample: Dict[str, Any]) -> None:
    try:
        owner_name = sample.get("Owner", {}).get("Name", None)
        if owner_name is None:
            return None

        if isinstance(owner_name, str):
            sample["Owner"]["Name"] = {"abbreviation": owner_name, "content": owner_name}
        elif isinstance(owner_name, list):
            sample["Owner"]["Name"] = [
                item
                if isinstance(item, dict)
                else {"content": item}
                for item in owner_name
            ]
    except Exception as e:
        LOGGER.warning("Failed to update owner: %s", e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())


def write_jsonl(output_file: Path, docs: List[BioSample], is_append: bool = False) -> None:
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
    xml_file: Path
    is_ddbj: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE
    parallel_num: int = DEFAULT_PARALLEL_NUM
    remove_tmp_dir: bool = False
    use_existing_tmp_dir: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Convert BioSample XML to JSON-Lines
        """
    )

    parser.add_argument(
        "--xml-file",
        help="""\
            BioSample XML file path (Required).
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--work-dir",
        help=f"""\
            The base directory where the script outputs are stored.
            By default, it is set to $PWD/ddbj_search_converter_results.
            The resulting JSON-Lines files will be stored in {{work_dir}}/{BS_JSONL_DIR_NAME}/{{date}}.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--is-ddbj",
        action="store_true",
        help="""\
            Whether the input xml file is ddbj or not.
            This bool determines the processing branch.
        """,
    )
    parser.add_argument(
        "--batch-size",
        help=f"The number of records to store in a single JSON-Lines file. Default is {DEFAULT_BATCH_SIZE}",
        type=int,
        default=DEFAULT_BATCH_SIZE,
    )
    parser.add_argument(
        "--parallel-num",
        help=f"The number of parallel processes to use. Default is {DEFAULT_PARALLEL_NUM}",
        type=int,
        default=DEFAULT_PARALLEL_NUM,
    )
    parser.add_argument(
        "--remove-tmp-dir",
        help="Remove the temporary directory after processing",
        action="store_true",
    )
    parser.add_argument(
        "--use-existing-tmp-dir",
        help="Use existing temporary directory",
        action="store_true",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.xml_file is None:
        raise Exception("Argument '--xml-file' is required.")
    xml_file = Path(parsed_args.xml_file).resolve()
    if xml_file.exists() is False:
        raise FileNotFoundError(f"File not found: {xml_file}")
    if parsed_args.work_dir is not None:
        config.work_dir = Path(parsed_args.work_dir)
        config.work_dir.mkdir(parents=True, exist_ok=True)
    if parsed_args.debug:
        config.debug = True

    return config, Args(
        xml_file=xml_file,
        is_ddbj=parsed_args.is_ddbj,
        batch_size=parsed_args.batch_size,
        parallel_num=parsed_args.parallel_num,
        remove_tmp_dir=parsed_args.remove_tmp_dir,
        use_existing_tmp_dir=parsed_args.use_existing_tmp_dir,
    )


def main() -> None:
    LOGGER.info("Start converting BioSample XML to JSON-Lines")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    output_dir = config.work_dir.joinpath(BS_JSONL_DIR_NAME).joinpath(TODAY)
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Output directory: %s", output_dir)
    xml_to_jsonl(
        config=config,
        xml_file=args.xml_file,
        output_dir=output_dir,
        is_ddbj=args.is_ddbj,
        batch_size=args.batch_size,
        parallel_num=args.parallel_num,
        remove_tmp_dir=args.remove_tmp_dir,
        use_existing_tmp_dir=args.use_existing_tmp_dir,
    )

    LOGGER.info("Finished converting BioSample XML to JSON-Lines")


if __name__ == "__main__":
    main()
