"""\
- BioSample XML を JSON-Lines に変換する
- 生成される JSON-Lines は 1 line が 1 BioSample Accession に対応する
"""
import argparse
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import xmltodict
from lxml import etree
from pydantic import BaseModel

from ddbj_search_converter.cache_db.bs_date import get_dates as get_bs_dates
from ddbj_search_converter.cache_db.bs_date import \
    get_session as get_bs_date_session
from ddbj_search_converter.cache_db.fusion_getter import get_xrefs
from ddbj_search_converter.config import (BS_JSONL_DIR_NAME, LOGGER, TODAY,
                                          Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.schema import (Attribute, BioSample, Distribution,
                                          Model, Organism, Package, Xref)

DEFAULT_BATCH_SIZE = 2000
DDBJ_JSONL_FILE_NAME = "ddbj_biosample.jsonl"
COMMON_JSONL_FILE_NAME = "biosample_{n}.jsonl"


def xml_to_jsonl(
    config: Config,
    xml_file: Path,
    is_ddbj: bool,
    output_dir: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    context = etree.iterparse(xml_file, tag="BioSample", recover=True)
    docs: List[BioSample] = []
    batch_count = 0
    file_count = 1
    with get_bs_date_session(config) as session:
        for _events, element in context:
            if element.tag == "BioSample":
                xml_str = etree.tostring(element)
                metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
                sample = metadata["BioSample"]
                accession = _parse_accession(sample, is_ddbj)

                model = _parse_and_update_model(accession, sample)
                package = _parse_and_update_package(accession, sample, model, is_ddbj)

                if is_ddbj:
                    date_created, date_modified, date_published = get_bs_dates(session, accession)
                else:
                    date_created, date_modified, date_published = _parse_date(sample)

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
                    dbXref=get_xrefs(config, accession, "biosample"),
                    sameAs=_parse_same_as(accession, sample),
                    status="public",
                    visibility="unrestricted-access",
                    dateCreated=date_created,
                    dateModified=date_modified,
                    datePublished=date_published,
                )

                # properties の中の object に対して整形を行う
                _update_owner(sample)

                docs.append(bs_instance)

                batch_count += 1
                if batch_count >= batch_size:
                    output_file = output_dir.joinpath(
                        DDBJ_JSONL_FILE_NAME if is_ddbj else COMMON_JSONL_FILE_NAME.format(n=file_count)
                    )
                    write_jsonl(output_file, docs, is_append=is_ddbj)
                    batch_count = 0
                    file_count += 1
                    docs = []

            # メモリリークを防ぐために要素をクリアする
            clear_element(element)

    if len(docs) > 0:
        # 余りの docs の書き込み
        output_file = output_dir.joinpath(
            DDBJ_JSONL_FILE_NAME if is_ddbj else COMMON_JSONL_FILE_NAME.format(n=file_count)
        )
        write_jsonl(output_file, docs, is_append=is_ddbj)


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
        LOGGER.error("Failed to parse accession: %s", e)

    if isinstance(accession, str):
        return accession
    else:
        LOGGER.error("Failed to parse accession: %s", accession)
        raise ValueError(f"Failed to parse accession: {accession}")


def _parse_organism(accession: str, sample: Dict[str, Any], is_ddbj: bool) -> Optional[Organism]:
    try:
        organism_obj = sample["Description"]["Organism"]
        if is_ddbj:
            name = organism_obj["OrganismName"]
        else:
            name = organism_obj["taxonomy_name"]
        return Organism(
            identifier=str(organism_obj["taxonomy_id"]),
            name=name,
        )
    except Exception as e:
        LOGGER.debug("Failed to parse organism with accession %s: %s", accession, e)
        return None


def _parse_title(accession: str, sample: Dict[str, Any]) -> Optional[str]:
    try:
        title = sample["Description"]["Title"]
        return str(title)
    except Exception as e:
        LOGGER.debug("Failed to parse title with accession %s: %s", accession, e)
        return None


def _parse_description(accession: str, sample: Dict[str, Any]) -> Optional[str]:
    try:
        description = sample["Description"]["Comment"]["Paragraph"]
        if isinstance(description, str):
            return description
        elif isinstance(description, list):
            return ",".join(description)
        return None
    except Exception as e:
        LOGGER.debug("Failed to parse description with accession %s: %s", accession, e)
        return None


def _parse_attributes(accession: str, sample: Dict[str, Any]) -> List[Attribute]:
    try:
        return [Attribute(
            attribute_name=attribute_obj.get("attribute_name", None),
            display_name=attribute_obj.get("display_name", None),
            harmonized_name=attribute_obj.get("harmonized_name", None),
            content=attribute_obj.get("content", None),
        ) for attribute_obj in sample["Attributes"]["Attribute"]]
    except Exception as e:
        LOGGER.debug("Failed to parse attribute with accession %s: %s", accession, e)
        return []


def _parse_and_update_model(accession: str, sample: Dict[str, Any]) -> List[Model]:
    try:
        model_obj = deepcopy(sample["Models"]["Model"])
        if isinstance(model_obj, dict):
            return [Model(name=model_obj["content"])]
        elif isinstance(model_obj, list):
            new_model_obj = [
                {"content": item}
                if isinstance(item, str)
                else {"content": item["content"], "version": item["version"]}
                for item in model_obj
            ]
            sample["Models"]["Model"] = new_model_obj
            return [Model(name=item["content"]) for item in new_model_obj]
        elif isinstance(model_obj, str):
            new_model_obj = [{"content": model_obj}]
            sample["Models"]["Model"] = new_model_obj
            return [Model(name=model_obj)]
        return []
    except Exception as e:
        LOGGER.debug("Failed to parse model with accession %s: %s", accession, e)
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
        LOGGER.debug("Failed to parse package with accession %s: %s", accession, e)
        return None


def _parse_same_as(accession: str, sample: Dict[str, Any]) -> List[Xref]:
    try:
        same_as = []
        for sample_obj in sample["Ids"]["Id"]:
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
        LOGGER.debug("Failed to parse sameAs with accession %s: %s", accession, e)
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
        owner_name = sample["Owner"]["Name"]
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
        LOGGER.debug("Failed to update owner: %s", e)
        return


def write_jsonl(output_file: Path, docs: List[BioSample], is_append: bool = False) -> None:
    """\
    - memory のほうが多いと見越して、一気に書き込む
    """
    mode = "a" if is_append else "w"
    with output_file.open(mode=mode, encoding="utf-8") as f:
        if is_append:
            f.write("\n")
        f.write("\n".join(doc.model_dump_json() for doc in docs))


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


# === CLI implementation ===


class Args(BaseModel):
    xml_file: Path
    is_ddbj: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE


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
    )


def main() -> None:
    LOGGER.info("Start converting BioSample XML to JSON-Lines")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    output_dir = config.work_dir.joinpath(BS_JSONL_DIR_NAME).joinpath(TODAY)
    LOGGER.info("Output directory: %s", output_dir)
    xml_to_jsonl(
        config=config,
        xml_file=args.xml_file,
        is_ddbj=args.is_ddbj,
        output_dir=output_dir,
        batch_size=args.batch_size,
    )

    LOGGER.info("Finished converting BioSample XML to JSON-Lines")


if __name__ == "__main__":
    main()
