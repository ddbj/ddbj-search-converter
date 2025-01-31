from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator, Optional, Sequence, Tuple, Union

from sqlalchemy import Row, String, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import LOGGER, Config

DB_FILE_NAME = "id_relation.sqlite"


# === Models ===

class Base(DeclarativeBase):
    pass


class StudySubmission(Base):
    __tablename__ = "study_submission"

    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class StudyBioProject(Base):
    __tablename__ = "study_bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class StudyExperiment(Base):
    __tablename__ = "study_experiment"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class ExperimentStudy(Base):
    __tablename__ = "experiment_study"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class ExperimentBioProject(Base):
    __tablename__ = "experiment_bioproject"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class ExperimentSample(Base):
    __tablename__ = "experiment_sample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class ExperimentBioSample(Base):
    __tablename__ = "experiment_biosample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class SampleExperiment(Base):
    __tablename__ = "sample_experiment"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class SampleBioSample(Base):
    __tablename__ = "sample_biosample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class AnalysisSubmission(Base):
    __tablename__ = "analysis_submission"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class RunExperiment(Base):
    __tablename__ = "run_experiment"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class RunSample(Base):
    __tablename__ = "run_sample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


class RunBioSample(Base):
    __tablename__ = "run_biosample"
    id0: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)
    id1: Mapped[str] = mapped_column(String(16), primary_key=True, index=True)


# === CRUD, etc. ===


def init_db(config: Config, overwrite: bool = True) -> None:
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


def store_date() -> None:
    pass
