from contextlib import contextmanager
from typing import Generator, List, Literal, Optional, Tuple

from sqlalchemy import Column, String, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.orm.session import Session

from ddbj_search_converter.config import Config


def create_db_engine(config: Config) -> Engine:
    return create_engine(
        f"sqlite:///{config.accessions_db_path}",
    )


@contextmanager
def get_session(config: Optional[Config] = None, engine: Optional[Engine] = None) -> Generator[Session, None, None]:
    if engine is None:
        if config is None:
            raise ValueError("config or engine must be specified")
        engine = create_db_engine(config)
    session = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )()

    try:
        yield session
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


# === models ===


class Base(DeclarativeBase):
    pass


class StudySubmission(Base):
    __tablename__ = "study_submission"
    # MySQL の場合 VARCHAR 型の長さに当たる (16) の部分が無いとエラーになる
    # SQLite は必要が無い
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class StudyBioProject(Base):
    __tablename__ = "study_bioproject"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class StudyExperiment(Base):
    __tablename__ = "study_experiment"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class ExperimentStudy(Base):
    __tablename__ = "experiment_study"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class ExperimentBioProject(Base):
    __tablename__ = "experiment_bioproject"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class ExperimentSample(Base):
    __tablename__ = "experiment_sample"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class ExperimentBioSample(Base):
    __tablename__ = "experiment_biosample"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class SampleExperiment(Base):
    __tablename__ = "sample_experiment"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class SampleBioSample(Base):
    __tablename__ = "sample_biosample"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class AnalysisSubmission(Base):
    __tablename__ = "analysis_submission"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class RunExperiment(Base):
    __tablename__ = "run_experiment"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class RunSample(Base):
    __tablename__ = "run_sample"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


class RunBioSample(Base):
    __tablename__ = "run_biosample"
    id0 = Column(String(16), primary_key=True, index=True)
    id1 = Column(String(16), primary_key=True, index=True)


# === CRUD func, etc. ===


TableNames = Literal[
    "study_submission",
    "study_bioproject",
    "study_experiment",
    "experiment_bioproject",
    "experiment_study",
    "experiment_sample",
    "experiment_biosample",
    "sample_experiment",
    "sample_biosample",
    "run_experiment",
    "run_sample",
    "run_biosample",
    "analysis_submission",
]


def init_db(config: Config) -> None:
    engine = create_db_engine(config)
    if config.accessions_db_path.exists():
        config.accessions_db_path.unlink()
    config.accessions_db_path.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    engine.dispose()


def bulk_insert(
    engine: Engine,
    rows: List[Tuple[str, str]],
    table_name: str,
) -> None:
    table = Base.metadata.tables[table_name]
    insert_data = [{"id0": id0, "id1": id1} for id0, id1 in rows]
    with get_session(engine=engine) as session:
        session.execute(table.insert(), insert_data)
        session.commit()
