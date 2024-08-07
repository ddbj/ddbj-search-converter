"""\
create_accession_db の実装

- SRA_Accession を id->Study, id->Experiment, id->Sample のように分解し、SQLite データベースに保存する

- 入力: 2,000,000 行ごとに分割された SRA_Accessions.tab ファイル群をまとめたディレクトリ
    - ディレクトリのパスは、通常、20240801 などの日付である
    - SRA_Accessions.tab は、ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab から download される
    - SRA_Accessions.tab は、scripts/split_sra_accessions.pl で分割される
- 出力: SQLite データベース
"""

import argparse
import csv
import glob
import os
import sys
from multiprocessing import Manager, Pool
from multiprocessing.managers import ListProxy
from pathlib import Path
from typing import Any, Dict, List, Tuple

from sqlalchemy.engine.base import Engine

from ddbj_search_converter.config import (LOGGER, Config, default_config,
                                          get_config, set_logging_level)
from ddbj_search_converter.dblink.id_relation_db import (TableNames,
                                                         bulk_insert,
                                                         create_db_engine,
                                                         init_db)

CHUNK_SIZE = 100000
Relation = Tuple[str, str, TableNames]
SharedData = Dict[TableNames, ListProxy[Any]]


def parse_args(args: List[str]) -> Config:
    parser = argparse.ArgumentParser(description="Create SQLite database from SRA_Accessions.tab files")

    parser.add_argument(
        "accessions_dir_path",
        nargs="?",
        default=None,
        help="Directory containing split SRA_Accessions.tab files (default: ./converter_results/sra_accessions/20240801)",
    )
    parser.add_argument(
        "accessions_db_path",
        nargs="?",
        default=None,
        help="Path to the created SQLite database (default: ./converter_results/sra_accessions.sqlite)",
    )
    parser.add_argument(
        "process_pool_size",
        default=default_config.process_pool_size,
        type=int,
        help=f"Number of processes to use (default: {default_config.process_pool_size})",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    config = get_config()
    if parsed_args.accessions_dir_path is not None:
        config.accessions_dir = parsed_args.accessions_dir_path
        if config.accessions_dir.exists() is False:
            LOGGER.error("Directory not found: %s", config.accessions_dir)
            sys.exit(1)
    if parsed_args.accessions_db_path is not None:
        config.accessions_db_path = parsed_args.accessions_db_path
    config.debug = parsed_args.debug

    return config


def store_relation_data(config: Config, path: Path, shared_data: SharedData) -> None:
    """
    SRA_Accession を id->Study, id->Experiment, id->Sample のように分解し（自分の該当する type は含まない）し、shared_data の各リストに保存する
    各リストが一定の長さになったら sqlite のテーブルに insert し、リストを初期化する

    各 process で呼ばれるため、engine と session は各 process で生成する
    """
    reader = csv.reader(path.open(), delimiter="\t", quoting=csv.QUOTE_NONE)
    next(reader)
    # 行の Type（STUDY, EXPERIMENT, SAMPLE, RUN, ANALYSIS, SUBMISSION）ごとテーブルを生成し、
    # 各 Type+BioProject, BioSample を追加したターゲットの値が null でなければ ID とのセットを作成しテーブルに保存する
    # relation は詳細表示に利用することを想定し、直接の検索では無いため status が live 以外は store しない

    engine = create_db_engine(config)
    for row in reader:
        # SRA_Accessions の行ごと処理を行う
        # status が liveであった場合
        # 各行の Type を取得し、処理を分岐し実行する
        try:
            if row[2] == "live":
                acc_type = row[6]
                data_relation: List[Relation]
                if acc_type == "SUBMISSION":
                    data_relation = []
                elif acc_type == "STUDY":
                    data_relation = [
                        (row[0], row[1], "study_submission"),
                        (row[0], row[18], "study_bioproject"),
                        (row[0], row[10], "study_experiment"),
                    ]
                elif acc_type == "EXPERIMENT":
                    data_relation = [
                        (row[0], row[10], "sample_experiment"),
                        (row[0], row[17], "sample_biosample"),
                    ]
                elif acc_type == "SAMPLE":
                    data_relation = [
                        (row[0], row[10], "sample_experiment"),
                        (row[0], row[17], "sample_biosample"),
                    ]
                elif acc_type == "RUN":
                    data_relation = [
                        (row[0], row[10], "run_experiment"),
                        (row[0], row[11], "run_sample"),
                        (row[0], row[17], "run_biosample"),
                    ]
                elif acc_type == "ANALYSIS":
                    data_relation = [
                        (row[0], row[1], "analysis_submission"),
                    ]
                else:
                    LOGGER.debug("Unknown accession type in %s: %s", row, acc_type)
                    continue
                for relation in data_relation:
                    insert_data(engine, shared_data, relation)
        except Exception as e:
            LOGGER.debug("Unexpected error in %s: %s", row, e)


def insert_data(engine: Engine, shared_data: SharedData, relation: Relation) -> None:
    table_name = relation[2]
    shared_list = shared_data[table_name]
    shared_list.append((relation[0], relation[1]))
    if len(shared_list) > CHUNK_SIZE:
        insert_rows = list(shared_list)
        bulk_insert(engine, insert_rows, table_name)
        shared_list.clear()


def main() -> None:
    config = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.info("Create SQLite database from SRA_Accessions.tab files")
    LOGGER.info("Config: %s", config.model_dump())

    init_db(config)
    file_list = glob.glob(os.path.join(config.accessions_dir, "*.txt"))

    error_flag = False
    with Manager() as manager:
        shared_data: SharedData = {
            "study_submission": manager.list(),
            "study_bioproject": manager.list(),
            "study_experiment": manager.list(),
            "experiment_bioproject": manager.list(),
            "experiment_study": manager.list(),
            "experiment_sample": manager.list(),
            "experiment_biosample": manager.list(),
            "sample_experiment": manager.list(),
            "sample_biosample": manager.list(),
            "run_experiment": manager.list(),
            "run_sample": manager.list(),
            "run_biosample": manager.list(),
            "analysis_submission": manager.list(),
        }

        with Pool(config.process_pool_size) as p:
            try:
                p.starmap(store_relation_data, [(config, Path(f), shared_data) for f in file_list])
            except Exception as e:
                LOGGER.error("Failed to store relation data: %s", e)
                error_flag = True

        # 最後に残ったデータを insert する
        engine = create_db_engine(config)
        for table_name, shared_list in shared_data.items():
            if len(shared_list) > 0:
                insert_rows = list(shared_list)
                bulk_insert(engine, insert_rows, table_name)

    if error_flag:
        LOGGER.error("Failed to create SQLite database, please check the log")
        sys.exit(1)

    LOGGER.info("Create SQLite database completed")


if __name__ == "__main__":
    main()
