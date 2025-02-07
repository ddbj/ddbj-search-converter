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
- どの accessions_tab_file を使うかの logic
    - まず、config.py における DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_BASE_PATH がある
        - 日時 batch では、これを元に find して、使用されると思われる
    - 引数で --sra-accessions-tab-file が指定されている場合は、それを使用する
        - 主に debug 用途
    - file が指定されず、--download が指定されている場合は、download してきて、それを使用する
"""
import argparse
import datetime
import sys
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from typing import Deque, Dict, Generator, List, Literal, Tuple

import httpx
from pydantic import BaseModel
from sqlalchemy import String, create_engine, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ddbj_search_converter.config import (LOGGER, Config, get_config,
                                          set_logging_level)

# SRA_ACCESSIONS_FILE_URL = "ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
SRA_ACCESSIONS_FILE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
SRA_ACCESSIONS_FILE_NAME = "SRA_Accessions.tab"
DB_FILE_NAME = "sra_accessions.sqlite"
DEFAULT_CHUNK_SIZE = 10000


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
        # echo=config.debug,
    )
    Base.metadata.create_all(engine)
    engine.dispose()


@contextmanager
def get_session(config: Config) -> Generator[Session, None, None]:
    engine = create_engine(
        f"sqlite:///{config.work_dir.joinpath(DB_FILE_NAME)}",
        # echo=config.debug,
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


def store_data(config: Config, sra_accessions_tab_file: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
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


def find_latest_sra_accessions_tab_file(config: Config) -> Path:
    """\
    - スパコン上の SRA_Accessions.tab file の位置として、/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions 以下に存在する
    - `{year}/{month}/SRA_Accessions.tab.{yyyymmdd}` という path で保存されている
    - Today から遡って、最初に見つかったファイルを返す
    """
    today = datetime.date.today()
    for days in range(90):  # Search for the last 90 days
        check_date = today - datetime.timedelta(days=days)
        year, month, yyyymmdd = check_date.strftime("%Y"), check_date.strftime("%m"), check_date.strftime("%Y%m%d")
        sra_accessions_tab_file_path = config.dblink_base_path.joinpath(f"{year}/{month}/SRA_Accessions.tab.{yyyymmdd}")
        if sra_accessions_tab_file_path.exists():
            return sra_accessions_tab_file_path

    raise FileNotFoundError("SRA_Accessions.tab file not found in the last 90 days")

# === CLI implementation ===


class Args(BaseModel):
    chunk_size: int = DEFAULT_CHUNK_SIZE
    download: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Create SQLite DB for SRA Accessions information.
            The SRA_Accessions.tab file contains id relations between accessions.
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
        "--sra-accessions-tab-file",
        help="""\
            The path to the SRA_Accessions.tab file.
            If not specified, the file will be found in the DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_BASE_PATH directory.
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
        "--chunk-size",
        help=f"The number of records to store in a single transaction. Default is {DEFAULT_CHUNK_SIZE}.",
        nargs="?",
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

    # ここの logic は、この file の docstring に記載されている通り
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

    return config, Args(chunk_size=parsed_args.chunk_size, download=parsed_args.download)


def main() -> None:
    LOGGER.info("Creating SRA Accessions SQLite DB")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))
    if args.download:
        LOGGER.info("Downloading SRA_Accessions.tab file")
        config.sra_accessions_tab_file_path = download_sra_accessions_tab_file(config)

    init_db(config)
    store_data(config, config.sra_accessions_tab_file_path, args.chunk_size)

    LOGGER.info("Completed. Saved to %s", config.work_dir.joinpath(DB_FILE_NAME))


if __name__ == "__main__":
    main()
