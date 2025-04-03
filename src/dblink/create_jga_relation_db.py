import csv
import argparse
import sqlite3
from typing import NewType, List, Tuple
from create_jga_relation_table import initialize_table, create_indexes

# TODO:環境変数に記述するように
CHUNK_SIZE = 10000
LOCAL_FILE_PATH = "/lustre9/open/shared_data/jga/metadata-history/metadata/"
FILE_LIST = [
    "analysis-study-relation",
    "dataset-analysis-relation",
    "dataset-policy-relation",
    "policy-dac-relation",
]
# pythonのsqlite3からの操作で"-"を含むtable nameを受け付けなかったため変更
TABLE_LIST = [
    "dataset_policy_relation",
    "dataset_dac_relation",
    "dataset_study_relation",
    "study_dac_relation",
    "study_policy_relation",
    "policy_dac_relation"
]
DB_PATH = "~/tasks/sra/resources/jga_link.sqlite"
parser = argparse.ArgumentParser(description="jga relation file to sqlite")
parser.add_argument("-d", default=DB_PATH)
args = parser.parse_args()


def load_id_set(file_name:str) -> List[tuple]:
    """
    csvファイルからrelationファイルを取得しidペアのLIST[set]を生成する
    """
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # 先頭行をスキップ
        return [(row[1], row[2]) for row in reader]


def store_data(db, tbl, relation_list):
    """
    id relationのリストをsqliteに保存する
    """
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    sql = f"INSERT INTO {tbl} VALUES (?, ?)"
    cur.executemany(sql, relation_list)
    conn.commit()


def create_dataset_relation(relations:dict) -> Tuple[List[tuple]]:
    """
    dataset-policy,dataset-dac,dataset-studyペアを生成しsqliteに保存する
    また続く処理に利用するため次のペアのリストを返す
    Returns:
        Tuple[List[tuple]]: dataset-study, dataset-dac
    """
    # dataset-dac-relation生成
    dataset_dac = []
    for d_p in relations["dataset_policy_relation"]:
        dataset_dac.extend([(d_p[0],p_d[1]) for p_d in relations["policy_dac_relation"] if p_d[0] == d_p[1]])

    dataset_dac = list(set(dataset_dac))

    # dataset-study-relation生成
    dataset_study = []
    for d_a in relations["dataset_analysis_relation"]:
        dataset_study.extend([(d_a[0], a_s[1]) for a_s in relations["analysis_study_relation"] if a_s[0] == d_a[1] ])
    dataset_study = list(set(dataset_study))

    # dataset-*-relationをsqliteに保存
    for r in [("dataset_dac_relation", dataset_dac), ("dataset_study_relation", dataset_study), ("dataset_policy_relation", relations["dataset_policy_relation"])]:
        store_data(DB_PATH, r[0], r[1])

    print("len dataset-dac, dataset-study: ", len(dataset_dac), len(dataset_study))
    return dataset_dac, dataset_study


def create_study_relation(dataset_dac,dataset_study, relations):
    """
    study-dac-relation,study-policy-relationの関係データを生成しsqliteに保存する

    Args:
        relations (dict): 
        dataset_study (list): create_dataset_relation()で生成したlist
        dataset_dac (list): create_dataset_relation()で生成したlist
    """
    # study-dac-relation生成
    study_dac = []
    for d_s in dataset_study:
        study_dac.extend([(d_s[1], d_d[1]) for d_d in dataset_dac if d_s[0] == d_d[0]])
    study_dac = list(set(study_dac))
    
    # study-policy-relation生成
    study_policy = []
    for d_s in dataset_study:
        study_policy.extend([(d_s[1], d_p[1]) for d_p in  relations["dataset_policy_relation"] if d_s[0] == d_p[0]])
    study_policy = list(set(study_policy))

    # 生成したrelationを保存
    for r in [("study_dac_relation", study_dac), ("study_policy_relation",study_policy)]:
        store_data(DB_PATH, r[0], r[1])

    print("len study-dac, study-policy: ", len(study_dac), len(study_policy))


def create_policy_relation(relations):
    """
    policy-dac-relationをsqliteに保存する
    """
    store_data(DB_PATH, "policy_dac_relation", relations["policy_dac_relation"])


def create_jga_relation():
    initialize_table(args.d)
    # csvをList[tuple]に変換
    relations = {}
    for f in FILE_LIST:
        file_name = f"{LOCAL_FILE_PATH}{f}.csv"
        new_f = f.replace("-", "_")
        relations[new_f] = load_id_set(file_name)

    dataset_dac, dataset_study = create_dataset_relation(relations)
    create_study_relation(dataset_dac, dataset_study, relations)
    create_policy_relation(relations)
    create_indexes(args.d)

if __name__ == "__main__":
    create_jga_relation()

