"""\
- DRA の date 情報を管理するための DB 周り
    - model 定義、初期化関数、getter 関数などを提供する
- 元データは、SRA_Accessions.tab と Submission の XML となる
    - SQLite: ${config.work_dir}/${DB_FILE_NAME (dra_date.sqlite)} に保存する
"""
import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple

from pydantic import BaseModel
from sqlalchemy import String, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.cache_db.sra_accessions import (
    download_sra_accessions_tab_file, find_latest_sra_accessions_tab_file)
from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.dra.utils import (TarXmlStore, iterate_sra_metadata,
                                             load_xml_metadata_from_tar)

DB_FILE_NAME = "dra_date.sqlite"
TABLE_NAME = "dra_date"


# === Models ===


class Base(DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = TABLE_NAME

    accession: Mapped[str] = mapped_column(String, primary_key=True)
    date_created: Mapped[Optional[str]] = mapped_column(String, nullable=True)


# === Abstracted functions ===


def init_sqlite_db(config: Config, overwrite: bool = True) -> None:
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


def store_data_to_sqlite(config: Config) -> None:
    tar_store = TarXmlStore(config.dra_xml_tar_file_path)
    records: List[Tuple[str, Optional[str]]] = []
    submission_accessions: Set[str] = set()
    try:
        for sra_metadata in iterate_sra_metadata(config, "SUBMISSION", tar_store=tar_store):
            if sra_metadata.accession is None:
                continue
            if sra_metadata.accession in submission_accessions:
                LOGGER.debug("Duplicate submission accession found: %s", sra_metadata.accession)
                continue

            xml_metadata = load_xml_metadata_from_tar(tar_store, sra_metadata)
            created_date: Optional[str] = xml_metadata.get("SUBMISSION", {}).get("submission_date", None)

            records.append((sra_metadata.accession, created_date))
            submission_accessions.add(sra_metadata.accession)
    finally:
        tar_store.close()

    with get_session(config) as session:
        try:
            session.execute(
                insert(Record),
                [{
                    "accession": accession,
                    "date_created": created_date,
                } for [accession, created_date] in records]
            )
            session.commit()
        except Exception as e:
            LOGGER.error("Failed to store data to SQLite: %s", e)
            session.rollback()
            raise


def count_records(config: Config) -> int:
    with get_session(config) as session:
        result = session.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME};"))
        count = result.fetchone()[0]  # type: ignore
        if isinstance(count, int):
            return count
        else:
            return -1


def get_date(config: Config, accession: str) -> Optional[str]:
    """\
    accession: DRA submission ID
    Return dateCreated
    """
    try:
        with get_session(config) as session:
            record = session.execute(
                select(Record).where(Record.accession == accession)
            ).scalar_one_or_none()

            if record is None:
                return None

            return record.date_created
    except Exception as e:
        LOGGER.debug("Failed to get date from SQLite: %s", e)
        return None


def get_dates_bulk(config: Config, accessions: Iterable[str]) -> Dict[str, Optional[str]]:
    """\
    Return dateCreated
    """
    try:
        with get_session(config) as session:
            records = session.execute(
                select(Record).where(Record.accession.in_(accessions))
            ).scalars().all()

            return {
                record.accession: record.date_created
                for record in records
            }
    except Exception as e:
        LOGGER.debug("Failed to get dates from SQLite: %s", e)
        return {}


# === CLI implementation ===


class Args(BaseModel):
    download: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create SQLite DB for DRA date information.
            The actual date information is retrieved from accession tab file and stored in an SQLite database.
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
            The resulting SQLite file will be stored in {{work_dir}}/{DB_FILE_NAME}.
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

    LOGGER.info("Creating SQLite DB for DRA date information")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))
    if args.download:
        LOGGER.info("Downloading SRA_Accessions.tab file")
        config.sra_accessions_tab_file_path = download_sra_accessions_tab_file(config)
    LOGGER.info("Using SRA_Accessions.tab file: %s", config.sra_accessions_tab_file_path)

    init_sqlite_db(config)
    store_data_to_sqlite(config)
    count = count_records(config)

    LOGGER.info("Completed. Saved to %s", config.work_dir.joinpath(DB_FILE_NAME))
    LOGGER.info("Number of records: %d", count)


if __name__ == "__main__":
    main()
