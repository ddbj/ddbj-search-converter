# encoding:utf-8
import csv
import requests
import os
import glob
import argparse
from multiprocessing import Pool
from typing import NewType, List
# dep.
from id_relation_db import *
from create_id_relation_table import initialize_table

parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
# 分割済みファイルのディレクトリ指定
parser.add_argument("input")
# sqliteデータベースのpath指定
# TODO: 現状sqlalchemyのモデルに依存してレポジトリのdblink/にdbが作られる・sqlalchemyを廃止し速度を改善する
parser.add_argument("db")
args = parser.parse_args()

FilePath = NewType('FilePath', str)
chunk_size = 100000


class ChunkedList(list):
    def __init__(self):
        self.ds = set()
        
    def push(self, id1, id2, t):
        if id1 and id2 and id2 != "-": 
            self.ds.add((id1, id2))
        if len(self.ds) > chunk_size:
            rows = list(set([base[t](id0=d[0], id1=d[1]) for d in self.ds]))
            try:
                session.bulk_save_objects(rows)
                #session.add_all(rows)
                session.commit()
            except Exception as e:
                print("insert error: ", e)
            finally:
                self.ds.clear()
                session.close()

    # pushを読んだのと同じインスタンスから呼ぶ必要がある
    def store_rest(self, t):
        """
        登録し残したself.dsが最後に残るのでこれをsqliteに保存する
        Todo:最後にchunkedlistに残った全データをdbに保存するためこのメソッドを呼ぶイベント
        :return:
        """
        rows = [base[t](id0=d[0], id1=d[1]) for d in self.ds if d[1] != "-"]
        # 以下sqlqlchemyの条件
        # session.bulk_save_objects(d, return_defaults=True)　# 同じデータがreplaceされない
        # session.add_all(d) # 同じデータがreplaceされない
        #session.merge(d, load=True) # 同じデータがreplaceされない
        # TODO
        # sqlite3でbulk insertする処理に書き直す
        try:
            session.bulk_save_objects(rows)
            #session.add_all(d)
            session.commit()
        except Exception as e:
            print("insert error: ", e)
        finally:
            session.close()

# 保存したい関係の変換クラスをインスタンス化する
# TODO: 要検討・インスタンスは各プロセスで呼ばないと効率的で無いのでは << 一旦グローバルなインスタンスで処理してみる
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


def create_db(path: FilePath):
    # インサート前にテーブルを空にする
    store_relation_data(path)
    # ChunkedListに保存されず残ったデータを保存する
    close_chunked_list()


def store_relation_data(path:FilePath):
    """
    SRA_Accessionをid->Study, id->Experiment, id->Sampleのように分解し（自分の該当するtypeは含まない）し一時リスト List[set]に保存
    各リストが一定の長さになったらsqliteのテーブルに保存し、一時リストを初期化する（処理が終了する際にも最後に残ったリストをsqliteに保存）
    :return:
    """
    reader = csv.reader(open(path), delimiter="\t", quoting=csv.QUOTE_NONE)
    next(reader)
    # 行のType（STUDY, EXPERIMENT, SAMPLE, RUN, ANALYSIS, SUBMISSION ）ごとテーブルを生成し、
    # 各Type+BioProject, BioSampleを追加したターゲットの値がnullでなければIDとのセットを作成しテーブルに保存する
    # relationは詳細表示に利用することを想定し、直接の検索では無いためstatusがlive以外はstoreしない

    for r in reader:
        # SRA_Accessionsの行ごと処理を行う
        # statusがliveであった場合
        # 各行のTypeを取得し、処理を分岐し実行する
        try:
            if r[2] == "live":
                try:
                    # store_{type}_set.store_rest(row)を呼ぶ
                    acc_type = r[6]

                    # store_*_set(row)関数を呼ぶ
                    convert_row[acc_type](r)
                except KeyError as E:
                    print(E)
                    pass
                except IndexError as E:
                    print(E)
                    pass
        except:
            pass


# store_restには各chunkedListインスタンスに残ったid Set[tuple](=self.ds)を登録する処理を書く
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
    depricated: sqlalchemy廃止のためcreate_id_relation_tableモジュールで直接sqlを操作する

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
    sample_biosample_set.push(r[0],r[17], "sample_biosample")


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

# depricated: sqlalchemyは今後つかわない
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

def main():

    # TODO: table初期化モジュールを呼ぶ
    db = args.db
    initialize_table(db)
    
    # cpu_count()次第で分割数は変える
    p = Pool(32)
    try:
        target_dir = args.input
        target_pattern = "*.txt"
        file_list = glob.glob(os.path.join(target_dir, target_pattern))
        # ファイルパスを引数として渡す TODO: ファイルパスを受け取るように以降の処理を書き換える
        p.map(create_db, file_list)
    except Exception as e:
        print("main: ", e)


if __name__ == "__main__":
    main()