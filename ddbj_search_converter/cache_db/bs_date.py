"""\
- BioSample の date 情報を管理するための DB 周り
    - model 定義、初期化 (postgres -> sqlite) 関数、getter 関数などを提供する
- 元データは、PostgreSQL から取得し、SQLite に保存する
    - PostgreSQL: at098:54301 に存在する (config.postgres_url で指定)
    - SQLite: ${config.work_dir}/${DB_FILE_NAME (bs_date.sqlite)} に保存する
"""
import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

from pydantic import BaseModel
from sqlalchemy import String, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.utils import format_date

DB_FILE_NAME = "bs_date.sqlite"
TABLE_NAME = "bs_date"
POSTGRES_DB_NAME = "biosample"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DEFAULT_CHUNK_SIZE = 10000

# === Models ===


class Base(DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = TABLE_NAME

    accession: Mapped[str] = mapped_column(String, primary_key=True)
    date_created: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    date_modified: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    date_published: Mapped[Optional[str]] = mapped_column(String, nullable=True)


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

# > \d mass.biosample_summary
# +---------------+---------+-----------+
# | Column        | Type    | Modifiers |
# |---------------+---------+-----------|
# | submitter_id  | text    |           |
# | submission_id | text    |           |
# | sample_name   | text    |           |
# | acc_type      | text    |           |
# | accession_id  | text    |           |
# | entity_status | integer |           |
# | title         | text    |           |
# | smp_id        | text    |           |
# +---------------+---------+-----------+

# > \d mass.sample
# +---------------+--------------------------+------------------------------------------------------------->
# | Column        | Type                     | Modifiers                                                   >
# |---------------+--------------------------+------------------------------------------------------------->
# | submission_id | text                     |  not null                                                   >
# | sample_name   | text                     |  not null                                                   >
# | release_type  | integer                  |                                                             >
# | release_date  | timestamp with time zone |                                                             >
# | core_package  | integer                  |                                                             >
# | pathogen      | integer                  |                                                             >
# | mixs          | integer                  |                                                             >
# | env_pkg       | integer                  |                                                             >
# | status_id     | integer                  |                                                             >
# | create_date   | timestamp with time zone |  not null default now()                                     >
# | modified_date | timestamp with time zone |  not null default now()                                     >
# | dist_date     | timestamp with time zone |                                                             >
# | smp_id        | bigint                   |  not null default nextval('mass.sample_smp_id_seq'::regclass>
# | package_group | text                     |                                                             >
# | package       | text                     |                                                             >
# | env_package   | text                     |                                                             >
# +---------------+--------------------------+------------------------------------------------------------->
# Indexes:
#     "sample_pkey" PRIMARY KEY, btree (smp_id)

DateTuple = Tuple[Optional[str], Optional[str], Optional[str]]
RowTuple = Tuple[str, Optional[str], Optional[str], Optional[str]]


def fetch_data_from_postgres(config: Config, chunk_size: int = 10000) -> Generator[List[RowTuple], None, None]:
    engine = create_engine(f"{config.postgres_url}/{POSTGRES_DB_NAME}")

    # key: submission_id, value: (date_created, date_modified, date_published)
    submission_date: Dict[str, DateTuple] = {}

    try:
        with engine.connect() as conn:
            offset_1 = 0
            while True:
                query = text("""
                SELECT DISTINCT ON (submission_id)
                    submission_id, create_date, modified_date, release_date
                FROM mass.sample
                ORDER BY submission_id
                LIMIT :chunk_size OFFSET :offset;
                """)
                try:
                    res = conn.execute(query, {"chunk_size": chunk_size, "offset": offset_1}).fetchall()
                    if not res:
                        break
                    for row in res:
                        submission_date[row.submission_id] = (
                            row.create_date, row.modified_date, row.release_date
                        )
                    offset_1 += chunk_size
                except Exception as e:
                    LOGGER.error("Failed to fetch data from PostgreSQL: %s", e)
                    break

            offset_2 = 0
            while True:
                # accession_id ごとに、submission_id は一つしか存在しないため、DISTINCT は不要
                query = text("""
                SELECT accession_id, submission_id
                FROM mass.biosample_summary
                WHERE accession_id IS NOT NULL
                ORDER BY accession_id
                LIMIT :chunk_size OFFSET :offset;
                """)
                res = conn.execute(query, {"chunk_size": chunk_size, "offset": offset_2}).fetchall()
                if not res:
                    break

                yield [
                    (row.accession_id, *submission_date.get(row.submission_id, (None, None, None)))
                    for row in res
                ]

                offset_2 += chunk_size

    finally:
        engine.dispose()


def store_data_to_sqlite(config: Config, records: List[RowTuple]) -> None:
    with get_session(config) as session:
        try:
            session.execute(
                insert(Record),
                [{
                    "accession": record[0],
                    "date_created": format_date(record[1]),
                    "date_modified": format_date(record[2]),
                    "date_published": format_date(record[3]),
                } for record in records]
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


def get_dates(config: Config, accession: str, session: Optional[Session] = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """\
    Return (dateCreated, dateModified, datePublished)
    """
    try:
        query = select(Record).where(Record.accession == accession)
        if session is None:
            with get_session(config) as session:
                res = session.execute(query).scalar_one_or_none()
        else:
            res = session.execute(query).scalar_one_or_none()

        if res is None:
            return None, None, None

        return (
            getattr(res, "date_created", None),
            getattr(res, "date_modified", None),
            getattr(res, "date_published", None),
        )
    except Exception as e:
        LOGGER.debug("Failed to get dates from SQLite: %s", e)
        return None, None, None


# === CLI implementation ===


class Args(BaseModel):
    chunk_size: int = DEFAULT_CHUNK_SIZE


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create SQLite DB for BioSample date information.
            The actual date information is retrieved from PostgreSQL and stored in an SQLite database.
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
        "--postgres-url",
        help="""\
            The PostgreSQL connection URL used to retrieve BioProject date information.
            The format is 'postgresql://{{username}}:{{password}}@{{host}}:{{port}}'.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--chunk-size",
        help="The number of records to fetch from PostgreSQL at a time. Default is {DEFAULT_CHUNK_SIZE}.",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
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
    if parsed_args.postgres_url is not None:
        config.postgres_url = parsed_args.postgres_url
    if parsed_args.debug:
        config.debug = True

    return config, Args(chunk_size=parsed_args.chunk_size)


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)

    LOGGER.info("Creating SQLite DB for BioSample date information")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    init_sqlite_db(config)
    for records in fetch_data_from_postgres(config, chunk_size=args.chunk_size):
        store_data_to_sqlite(config, records)
    count = count_records(config)

    LOGGER.info("Completed. Saved to %s", config.work_dir.joinpath(DB_FILE_NAME))
    LOGGER.info("Number of records: %d", count)


if __name__ == "__main__":
    main()
