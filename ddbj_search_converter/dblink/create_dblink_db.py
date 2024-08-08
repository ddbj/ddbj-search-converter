"""\
create_dblink_db の実装

- ddbj/dblink にある BioSample と BioProject の関連データをダウンロードし、SQLite データベースを作成する
    - 実際には、遺伝研スパコン内のデータを利用する
"""

import argparse
import sys
from pathlib import Path
from typing import List

from sqlalchemy import Column, Index, Table, Text, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import MetaData

from ddbj_search_converter.config import (LOGGER, Config, default_config,
                                          get_config, set_logging_level)
from ddbj_search_converter.dblink.id_relation_db import get_session


def parse_args(args: List[str]) -> Config:
    parser = argparse.ArgumentParser(description="Create SQLite database from DDBJ/DBLink files")

    parser.add_argument(
        "dblink_files_base_path",
        nargs="?",
        default=None,
        help=f"Base path of DDBJ/DBLink files (default: {default_config.dblink_files_base_path})",
    )
    parser.add_argument(
        "dblink_db_path",
        nargs="?",
        default=None,
        help="Path to the created SQLite database(default: ./converter_results/dblink.sqlite)",
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

    if parsed_args.dblink_files_base_path is not None:
        config.dblink_files_base_path = Path(parsed_args.dblink_files_base_path)
    if parsed_args.dblink_db_path is not None:
        config.dblink_db_path = Path(parsed_args.dblink_db_path)

    return config


# e.g., {DBLINK_FILES_BASE_PATH}/assembly_genome-bp/assembly_genome2bp.tsv
# DBLINK_FILES_BASE_PATH = "/lustre9/open/shared_data/dblink"
DBLINK_FILES = [
    "assembly_genome-bp/assembly_genome2bp.tsv",
    "assembly_genome-bs/assembly_genome2bs.tsv",
    "bioproject-biosample/bioproject2biosample.tsv",
    "bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv",
    "biosample-bioproject/biosample2bioproject.tsv",
    "gea-bioproject/gea2bioproject.tsv",
    "gea-biosample/gea2biosample.tsv",
    "insdc-bioproject/insdc2bioproject.tsv",
    "insdc-biosample/insdc2biosample.tsv",
    "insdc_master-bioproject/insdc_master2bioproject.tsv",
    "insdc_master-biosample/insdc_master2biosample.tsv",
    "mtb2bp/mtb_id_bioproject.tsv",
    "mtb2bs/mtb_id_biosample.tsv",
    "ncbi_biosample_bioproject/ncbi_biosample_bioproject.tsv",
    "taxonomy_biosample/trace_biosample_taxon2bs.tsv",
]


def dblink_files(config: Config) -> List[Path]:
    files = [config.dblink_files_base_path.joinpath(file) for file in DBLINK_FILES]
    for file in files:
        if not file.exists():
            LOGGER.error("DDBJ/DBLink file not found: %s", file)
            sys.exit(1)

    return files


def create_db_engine(config: Config) -> Engine:
    return create_engine(
        f"sqlite:///{config.dblink_db_path}",
    )


def create_database(engine: Engine, file: Path) -> None:
    table_name = file.name.split(".")[0]

    # テーブルを作成
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("field1", Text),
        Column("field2", Text),
    )
    metadata.create_all(engine)

    # TSVファイルを読み込み、データベースに挿入
    data = []
    for row in file.read_text().splitlines():
        if row == "":
            continue
        field1, field2 = row.strip().split("\t")
        data.append({"field1": field1, "field2": field2})

    with get_session(engine=engine) as session:
        session.execute(table.insert(), data)
        session.commit()

        Index(f"{table_name}_field1", table.c.field1).create(bind=engine)
        Index(f"{table_name}_field2", table.c.field2).create(bind=engine)


def main() -> None:
    config = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.info("Start creating SQLite database from DDBJ/DBLink files")
    LOGGER.info("Config: %s", config.model_dump())

    config.dblink_files_base_path.mkdir(parents=True, exist_ok=True)
    if config.dblink_files_base_path.exists():
        config.dblink_files_base_path.unlink()
    engine = create_db_engine(config)

    files = dblink_files(config)
    # 関係データのファイル毎にデータベースを作成
    for file in files:
        LOGGER.info("Create database from %s", file)
        create_database(engine, file)

    LOGGER.info("Finish creating SQLite database")


if __name__ == "__main__":
    main()
