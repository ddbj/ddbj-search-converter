"""\
- BioSample の date 情報を管理するための DB 周り
    - model 定義、初期化 (postgres -> sqlite) 関数、getter 関数などを提供する
- 元データは、PostgreSQL から取得し、SQLite に保存する
    - PostgreSQL: at098:54301 に存在する (config.postgres_url で指定)
    - SQLite: ${config.work_dir}/${DB_FILE_NAME (bs_date.sqlite)} に保存する
"""
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Generator, List, Optional, Tuple, Union

from sqlalchemy import String, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import LOGGER, Config

DB_FILE_NAME = "bs_date.sqlite"
TABLE_NAME = "bs_date"
POSTGRES_DB_NAME = "biosample"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


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

    engine = create_engine(
        f"sqlite:///{db_file_path}",
        echo=config.debug,
    )
    Base.metadata.create_all(engine)
    engine.dispose()


@contextmanager
def get_session(config: Config) -> Generator[Session, None, None]:
    engine = create_engine(
        f"sqlite:///{config.work_dir.joinpath(DB_FILE_NAME)}",
        echo=config.debug,
    )
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
    engine = create_engine(
        f"{config.postgres_url}/{POSTGRES_DB_NAME}",
        echo=config.debug,
    )

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


def _format_date(value: Optional[Union[str, datetime]]) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.strftime(DATE_FORMAT)
        elif isinstance(value, str):
            return datetime.fromisoformat(value).strftime(DATE_FORMAT)
        else:
            raise ValueError(f"Invalid date format: {value}")
    except Exception as e:
        LOGGER.debug("Failed to format postgreSQL date to SQLite format: %s", e)

    return None


def store_data_to_sqlite(config: Config) -> None:
    with get_session(config) as session:
        try:
            for records in fetch_data_from_postgres(config):
                session.execute(
                    insert(Record),
                    [{
                        "accession": record[0],
                        "date_created": _format_date(record[1]),
                        "date_modified": _format_date(record[2]),
                        "date_published": _format_date(record[3]),
                    } for record in records]
                )
                session.commit()
        except Exception as e:
            LOGGER.error("Failed to store data to SQLite: %s", e)
            session.rollback()
            raise


def get_dates(session: Session, accession: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """\
    Return (dateCreated, dateModified, datePublished)
    """
    try:
        query = select(Record).where(Record.accession == accession)
        res = session.execute(query).fetchone()
        if res is None:
            return None, None, None
        return res.date_created, res.date_modified, res.date_published
    except Exception as e:
        LOGGER.debug("Failed to get dates from SQLite: %s", e)
        return None, None, None
