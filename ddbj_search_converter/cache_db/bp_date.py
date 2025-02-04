"""\
- BioProject の date 情報を管理するための DB 周り
    - model 定義、初期化 (postgres -> sqlite) 関数、getter 関数などを提供する
- 元データは、PostgreSQL から取得し、SQLite に保存する
    - PostgreSQL: at098:54301 に存在する (config.postgres_url で指定)
    - SQLite: ${config.work_dir}/${DB_FILE_NAME (bp_date.sqlite)} に保存する
"""
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator, Optional, Sequence, Tuple, Union

from sqlalchemy import Row, String, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import LOGGER, Config

DB_FILE_NAME = "bp_date.sqlite"
TABLE_NAME = "bp_date"
POSTGRES_DB_NAME = "bioproject"
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

def fetch_data_from_postgres(config: Config, chunk_size: int = 10000) -> Generator[Sequence[Row[Any]], None, None]:
    engine = create_engine(
        f"{config.postgres_url}/{POSTGRES_DB_NAME}",
        echo=config.debug,
    )

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
                        "accession": record.accession,
                        "date_created": _format_date(record.date_created),
                        "date_modified": _format_date(record.date_modified),
                        "date_published": _format_date(record.date_published),
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
