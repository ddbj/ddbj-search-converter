"""\
- SRA Accession tab file などから、JSON-Lines 形式のファイルを生成する
- 生成される JSON-Lines は 1 line が 1 accession に対応する
"""
import argparse
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from functools import lru_cache
from itertools import islice
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import xmltodict
from pydantic import BaseModel

from ddbj_search_converter.cache_db.dra_date import get_dates_bulk
from ddbj_search_converter.cache_db.dra_relation_ids import \
    get_relation_ids_bulk
from ddbj_search_converter.cache_db.sra_accessions import (
    download_sra_accessions_tab_file, find_latest_sra_accessions_tab_file)
from ddbj_search_converter.config import (DRA_JSONL_DIR_NAME, LOGGER, TODAY,
                                          Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.dra.utils import (ACCESSION_TYPE, SraMetadata,
                                             generate_experiment_dir_path,
                                             generate_sra_file_path,
                                             generate_xml_file_path,
                                             iterate_sra_metadata)
from ddbj_search_converter.schema import (SRA, Distribution, DownloadUrl,
                                          Organism)
from ddbj_search_converter.utils import to_xref

DEFAULT_BATCH_SIZE = 2000
DEFAULT_PARALLEL_NUM = 32


def generate_dra_jsonl(
    config: Config,
    output_dir: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> None:
    exist_xml_files: Set[Path] = set()
    not_exist_xml_files: Set[Path] = set()

    futures = []
    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        for accession_type in ACCESSION_TYPE:
            sra_iterator = iterate_sra_metadata(config, accession_type, exist_xml_files, not_exist_xml_files)
            for i, sra_metadata_list in enumerate(_chunked_iterator(sra_iterator, batch_size)):
                jsonl_file = output_dir.joinpath(f"sra-{accession_type.lower()}_{i + 1}.jsonl")
                future = executor.submit(jsonl_worker, config, sra_metadata_list, jsonl_file)
                futures.append(future)

        for future in futures:
            try:
                future.result()
            except Exception as e:
                LOGGER.error("Failed to generate JSON-Lines: %s", e)


def _chunked_iterator(iterator: Iterator[SraMetadata], batch_size: int) -> Iterator[List[SraMetadata]]:
    while True:
        chunk = list(islice(iterator, batch_size))
        if not chunk:
            break
        yield chunk


def jsonl_worker(config: Config, sra_metadata_list: List[SraMetadata], jsonl_file: Path) -> None:
    LOGGER.info("Converting to JSON-Lines: %s", jsonl_file.name)
    submission_ids = [sra_metadata.submission for sra_metadata in sra_metadata_list]
    date_map = get_dates_bulk(config, submission_ids)
    relation_ids_map = get_relation_ids_bulk(config, submission_ids)

    docs: List[SRA] = []
    for sra_metadata in sra_metadata_list:
        dra_instance = sra_metadata_to_dra_instance(config, sra_metadata)
        if dra_instance is None:
            continue
        dra_instance.dbXref = [
            to_xref(id_) for id_ in relation_ids_map.get(sra_metadata.submission, [])
            if id_ != sra_metadata.accession
        ]
        dra_instance.dateCreated = date_map.get(sra_metadata.submission, None)
        docs.append(dra_instance)

    write_jsonl(jsonl_file, docs)


def sra_metadata_to_dra_instance(
    config: Config,
    sra_metadata: SraMetadata,
) -> Optional[SRA]:
    xml_file_path = config.dra_base_path.joinpath(generate_xml_file_path(sra_metadata))
    xml_metadata = _load_xml_file(xml_file_path)
    properties = deepcopy(_parse_properties(sra_metadata, xml_metadata))
    if properties is None:
        # LOGGER.warning("No properties found for %s, submission %s", sra_metadata.accession, sra_metadata.submission)
        return None

    dra_instance = SRA(
        identifier=sra_metadata.accession,
        properties=properties,
        distribution=_parse_distribution(sra_metadata),
        isPartOf="sra",
        type=f"sra-{sra_metadata.type.lower()}",  # type: ignore
        name=_parse_name(sra_metadata),
        url=_parse_url(sra_metadata),
        organism=_parse_organism(sra_metadata, properties),
        title=_parse_title(sra_metadata, properties),
        description=_parse_description(sra_metadata, properties),
        dbXref=[],  # Update after by bulk
        sameAs=[],
        downloadUrl=_parse_download_url(config, sra_metadata, properties),
        status="public",
        visibility="unrestricted-access",
        dateCreated=None,  # Update after by bulk
        dateModified=sra_metadata.updated,
        datePublished=sra_metadata.published,
    )

    return dra_instance


@lru_cache(maxsize=10)
def _load_xml_file(xml_file_path: Path) -> Dict[str, Any]:
    with xml_file_path.open("rb") as f:
        xml_bytes = f.read()
    xml_metadata = xmltodict.parse(xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False)

    return xml_metadata  # type: ignore


def _parse_properties(sra_metadata: SraMetadata, xml_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if sra_metadata.type == "SUBMISSION":
        item = xml_metadata["SUBMISSION"]
        if sra_metadata.accession != item["accession"]:
            return None
        return item  # type: ignore
    else:
        items = xml_metadata[f"{sra_metadata.type}_SET"][f"{sra_metadata.type}"]
        if isinstance(items, list):
            items = [item for item in items if item["accession"] == sra_metadata.accession]
            if len(items) == 0:
                return None
            return items[0]  # type: ignore
        if sra_metadata.accession != items["accession"]:
            return None
        return items  # type: ignore


def _parse_url(sra_metadata: SraMetadata) -> str:
    index = f"sra-{sra_metadata.type.lower()}"
    return f"https://ddbj.nig.ac.jp/search/entry/{index}/{sra_metadata.accession}"


def _parse_distribution(sra_metadata: SraMetadata) -> List[Distribution]:
    return [
        Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl=f"{_parse_url(sra_metadata)}.json",
        ),
        Distribution(
            type="DataDownload",
            encodingFormat="JSON-LD",
            contentUrl=f"{_parse_url(sra_metadata)}.jsonld",
        ),
    ]


def _parse_name(sra_metadata: SraMetadata) -> str:
    return sra_metadata.accession


def _parse_organism(sra_metadata: SraMetadata, properties: Dict[str, Any]) -> Optional[Organism]:
    if sra_metadata.type == "SAMPLE":
        sample_name = properties.get("SAMPLE_NAME", {})
        identifier = sample_name.get("TAXON_ID", None)
        name = sample_name.get("content", None)
        if name is None:
            name = sample_name.get("SCIENTIFIC_NAME", None)

        return Organism(
            identifier=identifier,
            name=name,
        )

    return None


def _parse_title(sra_metadata: SraMetadata, properties: Dict[str, Any]) -> Optional[str]:
    try:
        if sra_metadata.type == "STUDY":
            return properties.get("DESCRIPTOR", {}).get("STUDY_TITLE", sra_metadata.accession)  # type: ignore
        else:
            return properties.get("TITLE", sra_metadata.accession)  # type: ignore
    except Exception as e:
        LOGGER.warning("Failed to parse title with accession %s: %s", sra_metadata.accession, e)
        LOGGER.warning("Properties: %s", properties)
        LOGGER.warning("Traceback: %s", traceback.format_exc())
        return sra_metadata.accession


def _parse_description(sra_metadata: SraMetadata, properties: Dict[str, Any]) -> Optional[str]:
    try:
        if sra_metadata.type == "STUDY":
            descriptor = properties.get("DESCRIPTOR", {})
            description = descriptor.get("STUDY_DESCRIPTION", None)
            if description is None:
                description = descriptor.get("STUDY_ABSTRACT", None)
            return description  # type: ignore
        elif sra_metadata.type == "EXPERIMENT":
            return properties.get("DESIGN", {}).get("DESIGN_DESCRIPTION", None)  # type: ignore
        else:
            return properties.get("DESCRIPTION", None)  # type: ignore
    except Exception as e:
        LOGGER.warning("Failed to parse description with accession %s: %s", sra_metadata.accession, e)
        LOGGER.warning("Properties: %s", properties)
        LOGGER.warning("Traceback: %s", traceback.format_exc())
        return None


def _parse_download_url(config: Config, sra_metadata: SraMetadata, properties: Dict[str, Any]) -> List[DownloadUrl]:
    urls = []

    xml_file_path = Path(generate_xml_file_path(sra_metadata))
    urls.append(
        DownloadUrl(
            type="meta",
            name=f"{sra_metadata.submission}.{sra_metadata.type.lower()}.xml",
            url=f"https://ddbj.nig.ac.jp/public/ddbj_database/dra/{str(xml_file_path)}",
            ftpUrl=f"ftp://ddbj.nig.ac.jp/public/ddbj_database/dra/{str(xml_file_path)}",
        )
    )

    if sra_metadata.type == "RUN":
        experiment = properties.get("EXPERIMENT_REF", {}).get("accession", None)
        if experiment is not None:
            experiment_dir_path_str = generate_experiment_dir_path(sra_metadata, experiment)
            experiment_dir_path = config.dra_base_path.joinpath(experiment_dir_path_str)
            if experiment_dir_path.exists():
                urls.append(
                    DownloadUrl(
                        type="fastq",
                        name=f"{sra_metadata.accession}'s fastq",
                        url=f"https://ddbj.nig.ac.jp/public/ddbj_database/dra/{experiment_dir_path_str}/",
                        ftpUrl=f"ftp://ddbj.nig.ac.jp/ddbj_database/dra/{experiment_dir_path_str}/",
                    )
                )

            sra_file_path_str = generate_sra_file_path(sra_metadata, experiment)
            sra_file_path = config.dra_base_path.joinpath(sra_file_path_str)
            if sra_file_path.exists():
                urls.append(
                    DownloadUrl(
                        type="sra",
                        name=f"{sra_metadata.accession}'s SRA",
                        url=f"https://ddbj.nig.ac.jp/public/ddbj_database/dra/{sra_file_path_str}",
                        ftpUrl=f"ftp://ddbj.nig.ac.jp/public/ddbj_database/dra/{sra_file_path_str}",
                    )
                )

    if sra_metadata.type == "ANALYSIS":
        data_block = properties.get("DATA_BLOCK", [])
        if isinstance(data_block, dict):
            data_block = [data_block]
        file_items = []
        for block in data_block:
            files = block.get("FILES", {}).get("FILE", [])
            if isinstance(files, dict):
                files = [files]
            file_items.extend(files)

        base_dir = config.dra_base_path.joinpath("fastq", sra_metadata.submission[:6], sra_metadata.submission, sra_metadata.accession)
        existing_files: Set[str] = set()
        if base_dir.exists():
            for glob_file in base_dir.rglob("*"):
                if glob_file.is_file():
                    rel_path = glob_file.relative_to(config.dra_base_path)
                    existing_files.add(str(rel_path))

        for file in file_items:
            file_name = file["filename"]
            file_type = file["filetype"]
            file_path_str = f"fastq/{sra_metadata.submission[:6]}/{sra_metadata.submission}/{sra_metadata.accession}/{file_name}"
            if file_path_str not in existing_files:
                file_path_str = f"fastq/{sra_metadata.submission[:6]}/{sra_metadata.submission}/{sra_metadata.accession}/provisional/{file_name}"
                if file_path_str not in existing_files:
                    continue

            urls.append(
                DownloadUrl(
                    type=file_type,
                    name=file_name,
                    url=f"https://ddbj.nig.ac.jp/public/ddbj_database/dra/{file_path_str}",
                    ftpUrl=f"ftp://ddbj.nig.ac.jp/public/ddbj_database/dra/{file_path_str}",
                )
            )

    return urls


def write_jsonl(output_file: Path, docs: List[SRA], is_append: bool = False) -> None:
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
    download: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE
    parallel_num: int = DEFAULT_PARALLEL_NUM


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Generate DRA JSONL files from SRA Accession tab file.
        """
    )

    parser.add_argument(
        "--sra-accessions-tab-file",
        help="""\
            The path to the SRA_Accessions.tab file.
            If not specified, the file will be found in the DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_BASE_PATH directory.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--work-dir",
        help=f"""\
            The base directory where the script outputs are stored.
            By default, it is set to $PWD/ddbj_search_converter_results.
            The resulting JSON-lines file will be stored in {{work_dir}}/{DRA_JSONL_DIR_NAME}/{{date}}.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--download",
        help="""\
            Download the SRA_Accessions.tab file from the NCBI FTP server.
            Download to the work directory and use it.
        """,
        action="store_true",
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

    # ここの logic は、この file の docstring に記載されている通り
    if parsed_args.sra_accessions_tab_file is not None:
        sra_accessions_tab_file = Path(parsed_args.sra_accessions_tab_file)
        if not sra_accessions_tab_file.exists():
            raise FileNotFoundError(f"SRA_Accessions.tab file not found: {sra_accessions_tab_file}")
        config.sra_accessions_tab_file_path = sra_accessions_tab_file
    else:
        if parsed_args.download:
            # Download the SRA_Accessions.tab file later
            pass
        else:
            if config.sra_accessions_tab_base_path is not None:
                config.sra_accessions_tab_file_path = find_latest_sra_accessions_tab_file(config)
            else:
                raise ValueError("SRA_Accessions.tab file path is not specified.")

    if parsed_args.debug:
        config.debug = True

    return config, Args(
        download=parsed_args.download,
        batch_size=parsed_args.batch_size,
        parallel_num=parsed_args.parallel_num,
    )


def main() -> None:
    LOGGER.info("Start generating DRA JSONL files")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))
    if args.download:
        LOGGER.info("Downloading SRA_Accessions.tab file")
        config.sra_accessions_tab_file_path = download_sra_accessions_tab_file(config)
    LOGGER.info("Using SRA_Accessions.tab file: %s", config.sra_accessions_tab_file_path)

    output_dir = config.work_dir.joinpath(DRA_JSONL_DIR_NAME).joinpath(TODAY)
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Output directory: %s", output_dir)
    generate_dra_jsonl(
        config=config,
        output_dir=output_dir,
        batch_size=args.batch_size,
        parallel_num=args.parallel_num,
    )

    LOGGER.info("Finished generating DRA JSONL files")


if __name__ == "__main__":
    main()
