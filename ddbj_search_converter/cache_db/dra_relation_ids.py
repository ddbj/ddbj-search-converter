"""\
- DRA Submission ID とその他の ID との relation 情報を管理するための DB 周り
    - model 定義、初期化関数、getter 関数などを提供する
- 元データは、SRA_Accessions.tab と Submission の XML となる
    - SQLite: ${config.work_dir}/${DB_FILE_NAME (dra_relation_ids.sqlite)} に保存する
"""
import argparse
import json
import sys
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Tuple

from pydantic import BaseModel
from sqlalchemy import String, create_engine, insert, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.cache_db.sra_accessions import (
    download_sra_accessions_tab_file, find_latest_sra_accessions_tab_file)
from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.dra.utils import iterate_sra_metadata

DB_FILE_NAME = "dra_relation_ids.sqlite"
TABLE_NAME = "dra_relation_ids"


# === Models ===


class Base(DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = TABLE_NAME

    accession: Mapped[str] = mapped_column(String, primary_key=True)
    relation_ids: Mapped[str] = mapped_column(String)


# === Abstracted functions ===


def init_db(config: Config, overwrite: bool = True) -> None:
    db_file_path = config.work_dir.joinpath(DB_FILE_NAME)
    if db_file_path.exists() and overwrite:
        db_file_path.unlink()

    engine = create_engine(f"sqlite:///{db_file_path}")
    Base.metadata.create_all(engine)
    engine.dispose()


@contextmanager
def get_session(config: Config) -> Generator[Session, None, None]:
    engine = create_engine(f"sqlite:///{config.work_dir.joinpath(DB_FILE_NAME)}")
    with Session(engine) as session:
        yield session
    engine.dispose()


# === CRUD, etc. ===


def store_data(config: Config) -> None:
    records = defaultdict(set)
    for sra_metadata in iterate_sra_metadata(config):
        if sra_metadata.submission is None:
            continue
        if sra_metadata.accession is not None:
            records[sra_metadata.submission].add(sra_metadata.accession)
        if sra_metadata.bioproject is not None:
            records[sra_metadata.submission].add(sra_metadata.bioproject)
        if sra_metadata.biosample is not None:
            records[sra_metadata.submission].add(sra_metadata.biosample)

    LOGGER.info("Storing data to SQLite.")
    with get_session(config) as session:
        try:
            session.execute(
                insert(Record),
                [{
                    "accession": submission_id,
                    "relation_ids": json.dumps(sorted(relation_ids)),
                } for submission_id, relation_ids in records.items()]
            )
            session.commit()
        except Exception as e:
            LOGGER.error("Failed to store data to SQLite: %s", e)
            session.rollback()
            raise


def get_relation_ids(config: Config, accession: str) -> List[str]:
    """\
    accession: DRA submission ID
    """
    try:
        with get_session(config) as session:
            record = session.execute(
                select(Record).where(Record.accession == accession)
            ).scalar_one_or_none()

            if record is None:
                return []

            return json.loads(record.relation_ids)  # type: ignore
    except Exception as e:
        raise Exception(f"Failed to get relation IDs from SQLite: {e}") from e


def get_relation_ids_bulk(config: Config, accessions: Iterable[str]) -> Dict[str, List[str]]:
    try:
        with get_session(config) as session:
            records = session.execute(
                select(Record).where(Record.accession.in_(accessions))
            ).scalars().all()

            return {
                record.accession: json.loads(record.relation_ids)
                for record in records
            }
    except Exception as e:
        raise Exception(f"Failed to get relation IDs from SQLite: {e}") from e


# === CLI implementation ===


class Args(BaseModel):
    download: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create SQLite DB for relation IDs.
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

    LOGGER.info("Creating SQLite DB for DRA relation IDs.")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))
    if args.download:
        LOGGER.info("Downloading SRA_Accessions.tab file")
        config.sra_accessions_tab_file_path = download_sra_accessions_tab_file(config)
    LOGGER.info("Using SRA_Accessions.tab file: %s", config.sra_accessions_tab_file_path)

    init_db(config)
    store_data(config)

    LOGGER.info("Completed. Saved to %s", config.work_dir.joinpath(DB_FILE_NAME))


if __name__ == "__main__":
    main()
