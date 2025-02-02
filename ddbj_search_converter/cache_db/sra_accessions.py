"""\
- SRA_Accessions.tab の id relation を管理するための DB 周り
    - このファイルは、tsv ファイルで、各行にある accession とその他の id との関係が記載されている
    - ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab から download される
- model 定義、初期化関数、getter 関数などを提供する
- SQLite: ${config.work_dir}/${DB_FILE_NAME (sra_accessions.sqlite)} に保存する
- 多分、SQLite の write 処理がボトルネックになる
    - queue を使って、ある程度固めて insert する
    - relation を作る部分は、並列化しても多分、律速になると思われる
    - それより、メンテナンス性などを上げておく
"""
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from typing import Deque, Dict, Generator, List, Literal, Tuple

import httpx
from sqlalchemy import String, create_engine, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import LOGGER, Config

# SRA_ACCESSIONS_FILE_URL = "ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
SRA_ACCESSIONS_FILE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
SRA_ACCESSIONS_FILE_NAME = "SRA_Accessions.tab"
DB_FILE_NAME = "sra_accessions.sqlite"


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


TableNames = Literal[
    "study_submission",
    "study_bioproject",
    "study_experiment",
    "experiment_study",
    "experiment_bioproject",
    "experiment_sample",
    "experiment_biosample",
    "sample_experiment",
    "sample_biosample",
    "analysis_submission",
    "run_experiment",
    "run_sample",
    "run_biosample",
]


# === Abstracted functions ===


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


# === CRUD, etc. ===


def download_sra_accessions_tab_file(config: Config) -> Path:
    try:
        with httpx.stream("GET", SRA_ACCESSIONS_FILE_URL, timeout=30) as response:
            response.raise_for_status()
            with config.work_dir.joinpath(SRA_ACCESSIONS_FILE_NAME).open("wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
    except Exception as e:
        LOGGER.error("Failed to download SRA_Accessions.tab file: %s", e)
        raise

    return config.work_dir.joinpath(SRA_ACCESSIONS_FILE_NAME)


def _insert_data(config: Config, table_name: TableNames, data: Deque[Tuple[str, str]]) -> None:
    """\
    - 大きめの data を insert するため、メモリリークを考慮し、session は、都度生成することにする
    """
    if not data:
        return

    with get_session(config) as session:
        try:
            table = Base.metadata.tables[table_name]
            session.execute(
                insert(table),
                [{"id0": id0, "id1": id1} for id0, id1 in data]
            )
            session.commit()
        except Exception as e:
            LOGGER.error("Failed to insert data into %s: %s", table_name, e)
            session.rollback()
            raise


def store_data(config: Config, sra_accessions_tab_file: Path, chunk_size: int = 10000) -> None:
    """\
    - 行の Type（STUDY, EXPERIMENT, SAMPLE, RUN, ANALYSIS, SUBMISSION）ごとテーブルを生成する
    - relation は詳細表示に利用することを想定し、直接の検索では無いため status が live 以外は store しない
    """

    queues: Dict[TableNames, Deque[Tuple[str, str]]] = {
        "study_submission": deque(),
        "study_bioproject": deque(),
        "study_experiment": deque(),
        "experiment_study": deque(),
        "experiment_bioproject": deque(),
        "experiment_sample": deque(),
        "experiment_biosample": deque(),
        "sample_experiment": deque(),
        "sample_biosample": deque(),
        "analysis_submission": deque(),
        "run_experiment": deque(),
        "run_sample": deque(),
        "run_biosample": deque(),
    }

    with sra_accessions_tab_file.open("f", encoding="utf-8") as f:
        next(f)  # skip header
        for i, line in enumerate(f):
            row = line.strip().split("\t")
            try:
                if row[2] != "live":
                    continue

                accession_type = row[6]
                if accession_type == "SUBMISSION":
                    pass  # do nothing
                elif accession_type == "STUDY":
                    queues["study_submission"].append((row[0], row[1]))
                    queues["study_bioproject"].append((row[0], row[18]))
                    queues["study_experiment"].append((row[0], row[10]))
                elif accession_type == "EXPERIMENT":
                    queues["sample_experiment"].append((row[0], row[10]))
                    queues["sample_biosample"].append((row[0], row[17]))
                elif accession_type == "SAMPLE":
                    queues["sample_experiment"].append((row[0], row[10]))
                    queues["sample_biosample"].append((row[0], row[17]))
                elif accession_type == "RUN":
                    queues["run_experiment"].append((row[0], row[10]))
                    queues["run_sample"].append((row[0], row[11]))
                    queues["run_biosample"].append((row[0], row[17]))
                elif accession_type == "ANALYSIS":
                    queues["analysis_submission"].append((row[0], row[1]))
                else:
                    LOGGER.debug("Unknown accession type in %s: %s", row, accession_type)

                # queue の length check は、chunk_size // 10 ごとに行う
                if (i + 1) % (chunk_size // 10) == 0:
                    for table_name, queue in queues.items():
                        if len(queue) >= chunk_size:
                            _insert_data(config, table_name, queue)
                            queue.clear()

            except Exception as e:
                LOGGER.error("Failed to parse SRA_Accessions.tab row: %s", e)
                # TODO: とりあえず、raise しない

    # 最後に残った queue を insert する
    for table_name, queue in queues.items():
        if queue:
            _insert_data(config, table_name, queue)
