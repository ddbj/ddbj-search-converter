"""\
create_dblink_db の実装

- ddbj/dblink にある BioSample と BioProject の関連データをダウンロードし、SQLite データベースを作成する
    - 実際には、遺伝研スパコン内のデータを利用する
"""

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional

from sqlalchemy import Column, Index, Table, Text, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData

from ddbj_search_converter.config import Config, default_config, get_config


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

    parsed_args = parser.parse_args(args)

    config = get_config()
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
            print(f"DDBJ/DBLink file not found: {file}")
            sys.exit(1)

    return files


def create_db_engine(config: Config) -> Engine:
    return create_engine(
        f"sqlite:///{config.dblink_db_path}",
    )


@contextmanager
def get_session(config: Optional[Config] = None, engine: Optional[Engine] = None) -> Generator[Session, None, None]:
    if engine is None:
        if config is None:
            raise ValueError("config or engine must be specified")
        engine = create_db_engine(config)
    session = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )()

    try:
        yield session
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


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
    print("Create SQLite database from DDBJ/DBLink files")
    config = parse_args(sys.argv[1:])
    print(f"Config: {config.model_dump()}")

    config.dblink_files_base_path.mkdir(parents=True, exist_ok=True)
    if config.dblink_files_base_path.exists():
        config.dblink_files_base_path.unlink()
    engine = create_db_engine(config)

    files = dblink_files(config)
    # 関係データのファイル毎にデータベースを作成
    for file in files:
        print(f"Create database from {file}")
        create_database(engine, file)

    print("Create SQLite database completed")


if __name__ == "__main__":
    main()
