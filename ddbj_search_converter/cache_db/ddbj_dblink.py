"""\
- dblink 以下の id relation 情報を管理するための DB 周り
    - /lustre9/open/shared_data/dblink 以下のファイルのこと
    - それぞれ、tsv で、各行が id to id であるとする
"""
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from typing import Deque, Generator, Literal, Tuple

from sqlalchemy import String, create_engine, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import LOGGER, Config

DB_FILE_NAME = "ddbj_dblink.sqlite"

# Config.dblink_base_path として、以下の path を想定している
# BASE_PATH = /lustre9/open/shared_data/dblink
SOURCE_FILES = [
    "assembly_genome-bp/assembly_genome2bp.tsv",
    "assembly_genome-bs/assembly_genome2bs.tsv",
    "bioproject-biosample/bioproject2biosample.tsv",
    "bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv",
    "biosample-bioproject/biosample2bioproject.tsv",
    "gea-bioproject/gea2bioproject.tsv",
    "gea-biosample/gea2biosample.tsv",
    "mtb2bp/mtb_id_bioproject.tsv",
    "mtb2bs/mtb_id_biosample.tsv",
    "ncbi_biosample_bioproject/ncbi_biosample_bioproject.tsv",
    "taxonomy_biosample/trace_biosample_taxon2bs.tsv",
]


# === Models ===


TableNames = Literal[
    "assembly_genome2bp",
    "assembly_genome2bs",
    "bioproject2biosample",
    "bioproject_umbrella2bioproject",
    "biosample2bioproject",
    "gea2bioproject",
    "gea2biosample",
    "mtb_id_bioproject",
    "mtb_id_biosample",
    "ncbi_biosample_bioproject",
    "trace_biosample_taxon2bs",
]


class Base(DeclarativeBase):
    pass


class AssemblyGenome2Bp(Base):
    __tablename__ = "assembly_genome2bp"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class AssemblyGenome2Bs(Base):
    __tablename__ = "assembly_genome2bs"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class Bioproject2Biosample(Base):
    __tablename__ = "bioproject2biosample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class BioprojectUmbrella2Bioproject(Base):
    __tablename__ = "bioproject_umbrella2bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class Biosample2Bioproject(Base):
    __tablename__ = "biosample2bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class Gea2Bioproject(Base):
    __tablename__ = "gea2bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class Gea2Biosample(Base):
    __tablename__ = "gea2biosample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class MtbIdBioproject(Base):
    __tablename__ = "mtb_id_bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class MtbIdBiosample(Base):
    __tablename__ = "mtb_id_biosample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class NcbiBiosampleBioproject(Base):
    __tablename__ = "ncbi_biosample_bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class TraceBiosampleTaxon2Bs(Base):
    __tablename__ = "trace_biosample_taxon2bs"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)

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


def _insert_data(config: Config, table_name: TableNames, data: Deque[Tuple[str, str]]) -> None:
    with get_session(config) as session:
        try:
            table = Base.metadata.tables[table_name]
            session.execute(
                insert(table),
                [{"id0": id0, "id1": id1} for id0, id1 in data],
            )
            session.commit()
        except Exception as e:
            LOGGER.error("Failed to insert data to %s: %s", table_name, e)
            session.rollback()
            raise


def store_data(config: Config, source_file: Path, table_name: TableNames, chunk_size: int = 10000) -> None:
    queue: Deque[Tuple[str, str]] = deque()

    with source_file.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                id0, id1 = line.strip().split("\t")
                queue.append((id0, id1))
                if len(queue) >= chunk_size:
                    _insert_data(config, table_name, queue)
                    queue.clear()
            except Exception as e:
                LOGGER.error("Failed to parse line %s: %s", line, e)
                raise

    if queue:
        _insert_data(config, table_name, queue)
