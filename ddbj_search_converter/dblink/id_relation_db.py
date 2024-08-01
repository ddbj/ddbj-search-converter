from sqlalchemy import Column, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

# TODO:環境に合わせ書き換える・環境変数に記述するように
engine = create_engine("sqlite:///sra_accessions.sqlite")
session = scoped_session(sessionmaker(autocommit=False,
                                      bind=engine))
Base = declarative_base()


class StudySubmission(Base):
    __tablename__ = "study_submission"
    # MySQLの場合VARCHAR型の長さに当たる(10)の部分が無いとエラーになる
    # SQLiteは必要が無い
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


# Base.metadata.create_all(bind=engine)
