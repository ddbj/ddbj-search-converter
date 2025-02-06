import csv
import argparse
import sqlite3
from multiprocessing import Pool
from typing import NewType, List
from create_id_relation_table import initialize_table, create_indexes

parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
# sqliteデータベースのpath指定
# TODO: 環境変数に記述するように
parser.add_argument("db", default="tasks/sra/resources/jga_link.sqlite")
args = parser.parse_args()
# TODO:環境変数に記述するように
CHUNK_SIZE = 10000
LOCAL_FILE_PATH = "/lustre9/open/shared_data/jga/metadata-history/metadata/"
TABLE_LIST = [
    "analysis-data-relation",
    "analysis-sample-relation",
    "analysis-study-relation",
    "data-experiment-relation",
    "dataset-analysis-relation",
    "dataset-data-relation",
    "dataset-policy-relation",
    "experiment-sample-relation",
    "experiment-study-relation",
    "policy-dac-relation"
]

FilePath = NewType('FilePath', str)


# TODO: create_accession_db_m.pyと同じ処理なので処理を共有できるか検討
class ChunkedList(list):
    def __init__(self):
        self.ds = set()
        
    def push(self, id1, id2, t):
        # TODO: sqlite3のbulk save処理に書き換える（sqlalchemyのコードが残っている）
        conn = sqlite3.connect(args.db)
        if id1 and id2 and id2 != "-": 
            # ペアの値があればイテレーション用のsetにidの対を追加する
            self.ds.add((id1, id2))
        # idペアのリストがchunk_sizeを超えたらdbに追加する
        if len(self.ds) > CHUNK_SIZE:
            # uniqueなidペアに変換する
            rows = list(set([(d[0],d[1]) for d in self.ds]))
            try:
                bulk_insert(conn, rows, t)
                #session.bulk_save_objects(rows)
                #session.add_all(rows)
                #session.commit()
            except Exception as e:
                print("insert error: ", e)
            finally:
                self.ds.clear()
                conn.commit()
    
    def store_rest(self, t):
        """
        登録し残したself.dsが最後に残るのでこれをsqliteに保存する
        """
        conn = sqlite3.connect(args.db)
        rows = list(set([(d[0],d[1]) for d in self.ds]))
        # sqlite3でbulk insertする処理に書き直す
        try:
            bulk_insert(conn, rows, t)
            conn.commit()
        except Exception as e:
            print("insert error: ", e)
        finally:
            conn.close()


def create_db(table_name: str):
    """
    typeに応じてChunkdListを呼び出す
    Args:
        path (FilePath): _description_
    """
    file = f"{LOCAL_FILE_PATH}{table_name}.csv"
    reader = csv.reader(open(file), delimiter=",", quoting=csv.QUOTE_NONE)
    next(reader)
    chunked_list = ChunkedList()
    for r in reader:
        chunked_list.push(r[1], r[2], table_name)
    # chunked_listのchunk size以下のリストが残るのでDBに保存する
    chunked_list.store_rest(table_name)


def store_relation_data(path:FilePath):
    """
    SRA_Accessionをid->Study, id->Experiment, id->Sampleのように分解し（自分の該当するtypeは含まない）し一時リスト List[set]に保存
    各リストが一定の長さになったらsqliteのテーブルに保存し、一時リストを初期化する（処理が終了する際にも最後に残ったリストをsqliteに保存）
    :return:
    """
    reader = csv.reader(open(path), delimiter="\t", quoting=csv.QUOTE_NONE)
    next(reader)

    with open(file, 'r') as f:
        for line in f:
            field1, field2 = line.strip().split('\t')
            c.execute(f'INSERT INTO {table_name} VALUES (?, ?)', (field1, field2))


def bulk_insert(conn, lst:List[set], t:str):
    # bulk挿入
    cur = conn.cursor()
    sql = f"INSERT INTO {t} VALUES (?, ?)"
    cur.executemany(sql, lst)


def main():
    db = args.db
    initialize_table(db)
    # 関係テーブル数とcpu_count()次第で分割数は調整する
    p = Pool(10)
    try:
        p.map(create_db, TABLE_LIST)
    except Exception as e:
        print("main: ", e)

    # create_index
    create_indexes(db)
    

if __name__ == "__main__":
    main()