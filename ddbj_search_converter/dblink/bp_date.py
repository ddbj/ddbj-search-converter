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


# === CRUD, etc. ===


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


def format_date(value: Optional[Union[str, datetime]]) -> Optional[str]:
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
                        "date_created": format_date(record.date_created),
                        "date_modified": format_date(record.date_modified),
                        "date_published": format_date(record.date_published),
                    } for record in records]
                )
                session.commit()
        except Exception as e:
            session.rollback()
            LOGGER.error("Failed to store data to SQLite: %s", e)
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
