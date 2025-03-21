"""\
- BioProject の date 情報を管理するための DB 周り
    - model 定義、初期化 (postgres -> sqlite) 関数、getter 関数などを提供する
- 元データは、PostgreSQL から取得し、SQLite に保存する
    - PostgreSQL: at098:54301 に存在する (config.postgres_url で指定)
    - SQLite: ${config.work_dir}/${DB_FILE_NAME (bp_date.sqlite)} に保存する
"""
import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import (Any, Dict, Generator, Iterable, List, Optional, Sequence,
                    Tuple)

from pydantic import BaseModel
from sqlalchemy import Row, String, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.utils import format_date

DB_FILE_NAME = "bp_date.sqlite"
TABLE_NAME = "bp_date"
POSTGRES_DB_NAME = "bioproject"
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

# > \d mass.bioproject_summary
# +---------------+---------+-----------+
# | Column        | Type    | Modifiers |
# |---------------+---------+-----------|
# | submitter_id  | text    |           |
# | submission_id | text    |           |
# | acc_type      | text    |           |
# | accession     | text    |           |
# | entity_status | integer |           |
# | title         | text    |           |
# +---------------+---------+-----------+

# > \d mass.project
# +--------------------+--------------------------+-------------------------------------------------------->
# | Column             | Type                     | Modifiers                                              >
# |--------------------+--------------------------+-------------------------------------------------------->
# | submission_id      | text                     |  not null                                              >
# | project_id_prefix  | text                     |  default 'PRJDB'::text                                 >
# | project_id_counter | integer                  |  default nextval('mass.project_project_id_counter_seq':>
# | create_date        | timestamp with time zone |  not null default now()                                >
# | modified_date      | timestamp with time zone |  not null default now()                                >
# | issued_date        | timestamp with time zone |                                                        >
# | status_id          | integer                  |                                                        >
# | project_type       | text                     |  not null                                              >
# | release_date       | timestamp with time zone |                                                        >
# | dist_date          | timestamp with time zone |                                                        >
# | comment            | text                     |                                                        >
# +--------------------+--------------------------+-------------------------------------------------------->
# Indexes:
#     "project_pkey2" PRIMARY KEY, btree (submission_id)
#     "project_project_id_prefix_key1" UNIQUE CONSTRAINT, btree (project_id_prefix, project_id_counter)

# Note:
# - accession と submission_id の関係は 1 対 1
#   - -> DISTINCT ON は不要

def fetch_data_from_postgres(config: Config, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Generator[Sequence[Row[Any]], None, None]:
    engine = create_engine(f"{config.postgres_url}/{POSTGRES_DB_NAME}")

    offset = 0
    try:
        with engine.connect() as conn:
            while True:
                query = text("""
                SELECT
                    s.accession AS accession,
                    p.create_date AS date_created,
                    p.modified_date AS date_modified,
                    p.release_date AS date_published
                FROM mass.bioproject_summary s
                INNER JOIN mass.project p
                ON s.submission_id = p.submission_id
                WHERE s.accession IS NOT NULL
                ORDER BY s.accession
                LIMIT :chunk_size OFFSET :offset;
                """)
                try:
                    res = conn.execute(query, {"chunk_size": chunk_size, "offset": offset}).fetchall()
                    if not res:
                        break
                    yield res
                    offset += chunk_size
                except Exception as e:
                    LOGGER.error("Failed to fetch data from PostgreSQL: %s", e)
                    break
    finally:
        engine.dispose()


def store_data_to_sqlite(config: Config, records: Sequence[Row[Any]]) -> None:
    with get_session(config) as session:
        try:
            session.execute(
                insert(Record),
                [{
                    "accession": record.accession,
                    "date_created": format_date(record.date_created),
                    "date_modified": format_date(record.date_modified),
                    "date_published": format_date(record.date_published),
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


def get_dates(config: Config, accession: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """\
    Return (dateCreated, dateModified, datePublished)
    """
    try:
        with get_session(config) as session:
            record = session.execute(
                select(Record).where(Record.accession == accession)
            ).scalar_one_or_none()

            if record is None:
                return None, None, None

            return record.date_created, record.date_modified, record.date_published
    except Exception as e:
        LOGGER.debug("Failed to get dates from SQLite: %s", e)
        return None, None, None


def get_dates_bulk(config: Config, accessions: Iterable[str]) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
    """\
    Return (dateCreated, dateModified, datePublished)
    """
    try:
        with get_session(config) as session:
            records = session.execute(
                select(Record).where(Record.accession.in_(accessions))
            ).scalars().all()

            return {
                record.accession: (record.date_created, record.date_modified, record.date_published)
                for record in records
            }
    except Exception as e:
        LOGGER.debug("Failed to get dates from SQLite: %s", e)
        return {}


# === CLI implementation ===


class Args(BaseModel):
    chunk_size: int = DEFAULT_CHUNK_SIZE


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create SQLite DB for BioProject date information.
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

    LOGGER.info("Creating SQLite DB for BioProject date information")
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    init_sqlite_db(config)
    for records in fetch_data_from_postgres(config, args.chunk_size):
        store_data_to_sqlite(config, records)
    count = count_records(config)

    LOGGER.info("Completed. Saved to %s", config.work_dir.joinpath(DB_FILE_NAME))
    LOGGER.info("Number of records: %d", count)


if __name__ == "__main__":
    main()
