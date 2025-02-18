"""\
- dblink と SRA_Accessions.tab から BioProject ID とその他の ID との relation 情報 を作成し、es に bulk update する
- どの accessions_tab_file を使うかの logic
    - まず、config.py における DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_BASE_PATH がある
        - 日時 batch では、これを元に find して、使用されると思われる
    - 引数で --sra-accessions-tab-file が指定されている場合は、それを使用する
        - 主に debug 用途
    - file が指定されず、--download が指定されている場合は、download してきて、それを使用する
"""
import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple

from pydantic import BaseModel

from ddbj_search_converter.cache_db.ddbj_dblink import load_dblink_files
from ddbj_search_converter.cache_db.sra_accessions import (
    download_sra_accessions_tab_file, find_latest_sra_accessions_tab_file,
    load_sra_accessions_tab)
from ddbj_search_converter.cache_db.to_xref import to_xref
from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from elasticsearch import Elasticsearch, helpers

AccessionType = Literal["bioproject", "biosample"]


def is_document_mission_exception(error: Any) -> bool:
    """\
    e.g.,: {'update': {'_index': 'bioproject', '_id': 'PRJNA', 'status': 404, 'error': {'type': 'document_missing_exception', 'reason': '[PRJNA]: document missing', 'index_uuid': 'ujbdYuPJQdSaEZ954FJaXQ', 'shard': '0', 'index': 'bioproject'}}}
    """
    return (
        isinstance(error, dict)
        and "update" in error
        and error["update"]["status"] == 404
        and error["update"]["error"]["type"] == "document_missing_exception"
    )


def bulk_update_to_es(config: Config, sra_accessions_tab_file: Path, accession_type: AccessionType) -> None:
    LOGGER.info("Loading DBLink files to cache.")
    dblink_cache = load_dblink_files(config, accession_type)
    LOGGER.info("Loading SRA Accessions Tab file to cache.")
    sra_accessions_cache = load_sra_accessions_tab(sra_accessions_tab_file, accession_type)

    LOGGER.info("Merging DBLink and SRA Accessions cache.")
    merged_cache = defaultdict(set)
    for bp_id, dblink_ids in dblink_cache.items():
        merged_cache[bp_id].update(dblink_ids)
    for bp_id, sra_accessions in sra_accessions_cache.items():
        merged_cache[bp_id].update(sra_accessions)

    LOGGER.info("Bulk updating to Elasticsearch.")
    LOGGER.info("Updating %d documents.", len(merged_cache))
    count = 0

    es_client = Elasticsearch(config.es_url)
    failed_docs: List[Dict[str, Any]] = []
    actions = []
    for accession_id, relation_ids in merged_cache.items():
        actions.append({
            "_op_type": "update",
            "_index": accession_type,
            "_id": accession_id,
            "_source": {
                "doc": {
                    "dbXref": [to_xref(id_).model_dump(by_alias=True) for id_ in relation_ids]
                },
            },
        })
        count += 1
        if len(actions) >= 2000:
            LOGGER.info("Updating %d/%d documents.", count, len(merged_cache))
            _success, failed = helpers.bulk(
                es_client,
                actions,
                stats_only=False,
                raise_on_error=False
            )
            if isinstance(failed, list):
                for error in failed:
                    if not is_document_mission_exception(error):
                        failed_docs.append(error)
            actions.clear()

    if len(actions) > 0:
        LOGGER.info("Updating %d/%d documents.", count, len(merged_cache))
        _success, failed = helpers.bulk(
            es_client,
            actions,
            stats_only=False,
            raise_on_error=False
        )
        if isinstance(failed, list):
            for error in failed:
                if not is_document_mission_exception(error):
                    failed_docs.append(error)

    if failed_docs:
        LOGGER.error("Failed to update some docs: \n%s", failed_docs)


# === CLI implementation ===

class Args(BaseModel):
    download: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Bulk update documents in Elasticsearch with relation IDs from DBLink and SRA Accessions.
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
        "--download",
        help="""\
            Download the SRA_Accessions.tab file from the NCBI FTP server.
            Download to the work directory and use it.
        """,
        action="store_true",
    )
    parser.add_argument(
        "--es-url",
        help="The URL of the Elasticsearch server to update the data into.",
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
    if parsed_args.es_url is not None:
        config.es_url = parsed_args.es_url

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

    return config, Args(download=parsed_args.download)


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)

    LOGGER.info("Bulk updating BioProject documents in Elasticsearch.")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))
    if args.download:
        LOGGER.info("Downloading SRA_Accessions.tab file")
        config.sra_accessions_tab_file_path = download_sra_accessions_tab_file(config)
    LOGGER.info("Using SRA_Accessions.tab file: %s", config.sra_accessions_tab_file_path)

    bulk_update_to_es(config, config.sra_accessions_tab_file_path, "bioproject")

    LOGGER.info("Finished updating BioProject documents in Elasticsearch.")


if __name__ == "__main__":
    main()
