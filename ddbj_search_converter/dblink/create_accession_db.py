import csv

from ddbj_search_converter.dblink.id_relation_db import *

chunk_size = 10000
# TODO:環境に合わせ書き換える・環境変数に記述するように
LOCAL_ACCESSION_PATH = "/mnt/sra/SRA_Accessions.tab"
# ACCESSIONS_URL = ""  # 不要


class ChunkedList(list):
    def push(self, id1, id2, t):
        if id1 and id2:
            self.append((id1, id2))
        if len(self) > chunk_size:
            d = [base[t](id0=d[0], id1=d[1]) for d in self]
            session.bulk_save_objects(d, return_defaults=True)
            session.commit()
            self.clear()

    def store_rest(self, t):
        """
        最後に残ったリストの要素をsqliteに追加する
        Todo:最後にchunkedlistに残った全てデータをdbに保存するためこのメソッドを呼ぶイベント
        :return:
        """
        d = [base[t](id0=d[0], id1=d[1]) for d in self]
        session.bulk_save_objects(d, return_defaults=True)
        session.commit()


# 指定したサイズを超えたらDBにデータを保存しリフレッシュするリストを
# 保存したいエッジの数分用意する
study_submission_set = ChunkedList()
study_bioproject_set = ChunkedList()
study_experiment_set = ChunkedList()
experiment_study_set = ChunkedList()
experiment_bioproject_set = ChunkedList()
experiment_biosample_set = ChunkedList()
experiment_sample_set = ChunkedList()
sample_experiment_set = ChunkedList()
sample_biosample_set = ChunkedList()
analysis_submission_set = ChunkedList()
run_experiment_set = ChunkedList()
run_sample_set = ChunkedList()
run_biosample_set = ChunkedList()


def main():
    # accessions.tabを取得
    # SRA_Accessions.tabがミラーされているためこの操作は不要
    # get_accession_data()
    # インサート前にテーブルを空にする
    # Todo: tableの更新用、検索用のローテーションのフロー検討
    drop_all_tables()
    store_relation_data()
    # ChunkedListに保存されず残ったデータを保存する
    close_chunked_list()


def store_relation_data():
    """
    SRA_Accessionをid->Study, id->Experiment, id->Sampleのように分解し（自分の該当するtypeは含まない）しList[set]に保存
    各リストが一定の長さになったらsqliteのテーブルに保存し、リストを初期化する（処理が終了する際にも最後に残ったリストをsqliteに保存）
    :return:
    """
    reader = csv.reader(open(LOCAL_ACCESSION_PATH), delimiter="\t", quoting=csv.QUOTE_NONE)
    next(reader)
    # 行のType（STUDY, EXPERIMENT, SAMPLE, RUN, ANALYSIS, SUBMISSION ）ごとテーブルを生成し、
    # 各Type+BioProject, BioSampleを追加したターゲットの値がnullでなければIDとのセットを作成しテーブルに保存する
    # relationは詳細表示に利用することを想定し、直接の検索では無いためstatusがlive以外はstoreしない

    for r in reader:
        # SRA_Accessionsの行ごと処理を行う
        # statusがliveであった場合
        # 各行のTypeを取得し、処理を分岐し実行する
        if r[2] == "live":
            acc_type = r[6]
            convert_row[acc_type](r)


def close_chunked_list():
    experiment_bioproject_set.store_rest("experiment_bioproject")
    experiment_study_set.store_rest("experiment_study")
    experiment_sample_set.store_rest("experiment_sample")
    experiment_biosample_set.store_rest("experiment_biosample")
    sample_experiment_set.store_rest("sample_experiment")
    sample_biosample_set.store_rest("sample_biosample")
    run_experiment_set.store_rest("run_experiment")
    run_sample_set.store_rest("run_sample")
    run_biosample_set.store_rest("run_biosample")
    analysis_submission_set.store_rest("analysis_submission")


def drop_all_tables():
    """
    sqlalchemyから全テーブル削除する
    :return:
    """
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def store_study_set(r: list):
    """
    AccessionがStudyの行の処理
    study->Submission, Study->BioProject,Study->Experimentをchunkedlistに追加する
    :param r:
    :return:
    """
    study_submission_set.push(r[0], r[1], "study_submission")
    study_bioproject_set.push(r[0], r[18], "study_bioproject")
    study_experiment_set.push(r[0], r[10], "study_experiment")


def store_experiment_set(r: list):
    """
    AccessionがExperimentの行の処理
    :param r:
    :return:
    """
    experiment_bioproject_set.push(r[0], r[18], "experiment_bioproject")
    experiment_study_set.push(r[0], r[12], "experiment_study")
    experiment_sample_set.push(r[0], r[11], "experiment_sample")
    experiment_biosample_set.push(r[0], r[17], "experiment_biosample")


def store_sample_set(r: list):
    sample_experiment_set.push(r[0], r[10], "sample_experiment")
    sample_biosample_set.push(r[0], r[17], "sample_biosample")


def store_run_set(r: list):
    run_experiment_set.push(r[0], r[10], "run_experiment")
    run_sample_set.push(r[0], r[11], "run_sample")
    run_biosample_set.push(r[0], r[17], "run_biosample")


def store_analysis_set(r: list):
    analysis_submission_set.push(r[0], r[1], "analysis_submission")


def store_submission_set(r):
    pass


# typeに応じて処理を分岐する。処理はChunkedListにセットを追加する
convert_row = {
    "SUBMISSION": store_submission_set,
    "STUDY": store_study_set,
    "EXPERIMENT": store_experiment_set,
    "SAMPLE": store_sample_set,
    "RUN": store_run_set,
    "ANALYSIS": store_analysis_set
}

base = {
    "study_submission": StudySubmission,
    "study_bioproject": StudyBioProject,
    "study_experiment": StudyExperiment,
    "experiment_bioproject": ExperimentBioProject,
    "experiment_study": ExperimentStudy,
    "experiment_sample": ExperimentSample,
    "experiment_biosample": ExperimentBioSample,
    "sample_experiment": SampleExperiment,
    "sample_biosample": SampleBioSample,
    "run_experiment": RunExperiment,
    "run_sample": RunSample,
    "run_biosample": RunBioSample,
    "analysis_submission": AnalysisSubmission
}


def test_orm():
    q = (session.query(StudyBioProject, ExperimentBioProject, RunExperiment, ExperimentBioSample)
         .join(ExperimentBioProject, ExperimentBioProject.id1 == StudyBioProject.id1)
         .join(RunExperiment)
         .join(ExperimentBioSample)
         .filter())


if __name__ == "__main__":
    main()
