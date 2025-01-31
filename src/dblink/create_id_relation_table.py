# encoding:utf-8
import sqlite3


table_list = [
    "study_bioproject",
    "study_experiment",
    "study_submission",
    "experiment_study",
    "experiment_bioproject",
    "experiment_sample",
    "experiment_biosample",
    "sample_experiment",
    "sample_biosample",
    "analysis_submission",
    "run_experiment",
    "run_sample",
    "run_biosample"
]

def create_table_sql(table_name):
    
    table_sql = f"""
    CREATE TABLE {table_name} (
        id0 VARCHAR(16),
        id1 VARCHAR(16)
    );
    """
    #print("table: ", table_sql)
    return table_sql


def  create_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(create_table_sql(table_name))
    conn.commit()


def drop_table(conn, table_name):
    cur = conn.cursor()
    q = f"DROP TABLE IF EXISTs {table_name}"
    cur.execute(q)
    conn.commit()


def initialize_table(db):
    """
    table listを元にtableの
    drop_all,create_tableを呼び
    sraの関係テーブルを初期化する
    """
    conn = sqlite3.connect(db)
    for t in table_list:
        drop_table(conn, t)
        create_table(conn, t)


def create_indexes(db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    for t in table_list:
        sql0 = f"CREATE INDEX index_{t}_0 ON {t}(id0)"
        sql1 = f"CREATE INDEX index_{t}_1 ON {t}(id1)"
        cur.execute(sql0)
        cur.execute(sql1)
        conn.commit()

    
if __name__ == "__main__":
    initialize_table()