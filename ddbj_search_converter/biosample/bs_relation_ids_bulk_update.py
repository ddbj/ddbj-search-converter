"""\
- dblink と SRA_Accessions.tab から BioSample ID とその他の ID との relation 情報 を作成し、es に bulk update する
- どの accessions_tab_file を使うかの logic
    - まず、config.py における DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_BASE_PATH がある
        - 日時 batch では、これを元に find して、使用されると思われる
    - 引数で --sra-accessions-tab-file が指定されている場合は、それを使用する
        - 主に debug 用途
    - file が指定されず、--download が指定されている場合は、download してきて、それを使用する
"""
import sys

from ddbj_search_converter.bioproject.bp_relation_ids_bulk_update import (
    bulk_update_to_es, parse_args)
from ddbj_search_converter.cache_db.sra_accessions import \
    download_sra_accessions_tab_file
from ddbj_search_converter.config import LOGGER, set_logging_level


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)

    LOGGER.info("Bulk updating BioSample documents in Elasticsearch.")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))
    if args.download:
        LOGGER.info("Downloading SRA_Accessions.tab file")
        config.sra_accessions_tab_file_path = download_sra_accessions_tab_file(config)
    LOGGER.info("Using SRA_Accessions.tab file: %s", config.sra_accessions_tab_file_path)

    bulk_update_to_es(config, config.sra_accessions_tab_file_path, "biosample")

    LOGGER.info("Finished updating BioSample documents in Elasticsearch.")


if __name__ == "__main__":
    main()
