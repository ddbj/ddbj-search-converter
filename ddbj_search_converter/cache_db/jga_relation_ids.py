import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Set, Tuple

from pydantic import BaseModel
from sqlalchemy import (Column, MetaData, String, Table, create_engine, insert,
                        select)
from sqlalchemy.orm import Session

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)

DB_FILE_NAME = "jga_relation_ids.sqlite"

TABLE_LIST = [
    "dataset_policy_relation",
    "dataset_dac_relation",
    "dataset_study_relation",
    "study_policy_relation",
    "study_dac_relation",
    "study_dataset_relation",
    "policy_dac_relation",
    "policy_dataset_relation",
    "policy_study_relation",
    "dac_dataset_relation",
    "dac_study_relation",
    "dac_policy_relation",
]

# === Models ===


metadata = MetaData()

TABLES = {
    name: Table(
        name,
        metadata,
        Column("accession", String, primary_key=True),
        Column("relation_ids", String),
    )
    for name in TABLE_LIST
}


# === Abstracted functions ===


def init_db(config: Config, overwrite: bool = True) -> None:
    db_file_path = config.work_dir.joinpath(DB_FILE_NAME)
    if db_file_path.exists() and overwrite:
        db_file_path.unlink()

    engine = create_engine(f"sqlite:///{db_file_path}")
    metadata.create_all(engine)
    engine.dispose()


@contextmanager
def get_session(config: Config) -> Generator[Session, None, None]:
    engine = create_engine(f"sqlite:///{config.work_dir.joinpath(DB_FILE_NAME)}")
    with Session(engine) as session:
        yield session
    engine.dispose()


# === CRUD, etc. ===


Relation = Set[Tuple[str, str]]


def _read_relation_file(config: Config, from_rel_name: str, to_rel_name: str) -> Relation:
    file_path = config.jga_base_path.joinpath(f"{from_rel_name}-{to_rel_name}-relation.csv")
    if not file_path.exists():
        raise FileNotFoundError(f"Relation file {file_path} does not exist.")

    relation: Relation = set()
    with file_path.open("r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if len(row) < 3:
                LOGGER.warning("Skipping invalid row in %s: %s", file_path.name, row)
                continue
            relation.add((row[1], row[2]))

    return relation


def _join(ab: Relation, bc: Relation) -> Relation:
    """\
    (a, b) and (b, c) -> (a, c)
    """
    b_to_c: Dict[str, Set[str]] = defaultdict(set)
    for b, c in bc:
        b_to_c[b].add(c)

    return {(a, c) for a, b in ab for c in b_to_c.get(b, ())}


def _reverse(relation: Relation) -> Relation:
    """\
    (a, b) set -> (b, a) set
    """
    return {(b, a) for a, b in relation}


def _concat(relations: Iterable[Relation]) -> Relation:
    """\
    Concatenate multiple relations into one.
    """
    result: Relation = set()
    for relation in relations:
        result.update(relation)
    return result


def _to_index(relation: Relation) -> Dict[str, Set[str]]:
    """\
    (a, b) set -> {a: {b,...}}
    """
    index: Dict[str, Set[str]] = defaultdict(set)
    for a, b in relation:
        index[a].add(b)
    return index


def build_relations(config: Config) -> Dict[str, Relation]:
    """\
    - csv ファイルから:
        - analysis_data
        - analysis_sample
        - analysis_study
        - data_experiment
        - dataset_analysis
        - dataset_data
        - dataset_policy
        - experiment_sample
        - experiment_study
        - policy_dac
    - 下の relation を作成する:
        - dataset_policy
            - そのまま csv ファイルから読み込む
        - dataset_dac
            - dataset_policy と policy_dac を join
        - dataset_study
            - dataset_analysis と analysis_study を join
            - dataset_data, data_experiment, experiment_study を join
        - study_dac
            - reverse(dataset_study) と dataset_dac を join
        - study_policy
            - reverse(dataset_study) と dataset_policy を join
        - policy_dac
            - そのまま csv ファイルから読み込む
    """

    relations: Dict[str, Relation] = {
        "dataset_policy": _read_relation_file(config, "dataset", "policy"),
        "policy_dac": _read_relation_file(config, "policy", "dac"),
        "dataset_analysis": _read_relation_file(config, "dataset", "analysis"),
        "analysis_study": _read_relation_file(config, "analysis", "study"),
        "dataset_data": _read_relation_file(config, "dataset", "data"),
        "data_experiment": _read_relation_file(config, "data", "experiment"),
        "experiment_study": _read_relation_file(config, "experiment", "study"),
    }

    relations["dataset_dac"] = _join(
        relations["dataset_policy"],
        relations["policy_dac"]
    )
    relations["dataset_study"] = _concat([
        _join(relations["dataset_analysis"], relations["analysis_study"]),
        _join(_join(relations["dataset_data"], relations["data_experiment"]), relations["experiment_study"]),
    ])
    relations["study_dataset"] = _reverse(relations["dataset_study"])
    relations["study_dac"] = _join(
        relations["study_dataset"],
        relations["dataset_dac"]
    )
    relations["study_policy"] = _join(
        _reverse(relations["dataset_study"]),
        relations["dataset_policy"]
    )

    # Reverse relations
    relations["policy_dataset"] = _reverse(relations["dataset_policy"])
    relations["policy_study"] = _reverse(relations["study_policy"])
    relations["dac_dataset"] = _reverse(relations["dataset_dac"])
    relations["dac_study"] = _reverse(relations["study_dac"])
    relations["dac_policy"] = _reverse(relations["policy_dac"])

    for table_name in TABLE_LIST:
        rel_name = re.sub(r"_relation$", "", table_name)
        if rel_name not in relations:
            raise ValueError(f"Relation {rel_name} is not found in the relations dictionary.")

    return relations


def store_data(config: Config, relations: Dict[str, Relation]) -> None:
    LOGGER.info("Storing data to SQLite.")

    with get_session(config) as session:
        for table_name in TABLE_LIST:
            rel_name = re.sub(r"_relation$", "", table_name)
            relation = relations[rel_name]
            if not relation:
                LOGGER.warning("No data to store for %s", table_name)
                continue

            table = TABLES[table_name]
            try:
                session.execute(
                    insert(table),
                    [{
                        "accession": accession,
                        "relation_ids": json.dumps(sorted(relation_ids)),
                    } for accession, relation_ids in _to_index(relation).items()]
                )
                session.commit()
            except Exception as e:
                LOGGER.error("Failed to store data to SQLite for %s: %s", table_name, e)
                session.rollback()
                raise


def get_relation_ids(config: Config, rel_name: str, accession: str) -> List[str]:
    table_name = f"{rel_name}_relation"
    if table_name not in TABLE_LIST:
        raise ValueError(f"Invalid relation name: {rel_name}")

    table = TABLES[table_name]

    try:
        with get_session(config) as session:
            record = session.execute(
                select(table).where(table.c.accession == accession)
            ).scalar_one_or_none()

            if record is None:
                return []

            return json.loads(record.relation_ids)  # type: ignore
    except Exception as e:
        raise Exception(f"Failed to get relation IDs from SQLite for {rel_name}: {e}") from e


def get_relation_ids_bulk(config: Config, rel_name: str, accessions: Iterable[str]) -> Dict[str, List[str]]:
    table_name = f"{rel_name}_relation"
    if table_name not in TABLE_LIST:
        raise ValueError(f"Invalid relation name: {rel_name}")

    table = TABLES[table_name]

    try:
        with get_session(config) as session:
            records = session.execute(
                select(table.c.accession, table.c.relation_ids)
                .where(table.c.accession.in_(accessions))
            ).all()

            return {
                acc: json.loads(rel_ids)
                for acc, rel_ids in records
            }
    except Exception as e:
        raise Exception(f"Failed to get relation IDs from SQLite for {rel_name}: {e}") from e


# === CLI implementation ===


class Args(BaseModel):
    pass


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create SQLite DB for JGA relation IDs.
        """
    )

    parser.add_argument(
        "--work-dir",
        help=f"""\
            The base directory where the script outputs are stored.
            By default, it is set to $PWD/ddbj_search_converter_results.
            The resulting SQLite file will be stored in {{work_dir}}/{DB_FILE_NAME}.
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
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)

    LOGGER.info("Creating SQLite DB for JGA relation IDs.")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    relations = build_relations(config)
    init_db(config)
    store_data(config, relations)

    LOGGER.info("SQLite DB for JGA relation IDs created successfully.")


if __name__ == "__main__":
    main()
