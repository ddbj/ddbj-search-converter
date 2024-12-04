# encoding:utf-8
import sqlite3

def create_table_sql(table_name):
    
    table_sql = f"""
    CREATE TABLE {table_name} (
        id0 VARCHAR(16) PRIMARY KEY,
        id1 VARCHAR(16) PRIMARY KEY
    );
    """
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
    table_list = [
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
        "run_biosample"
    ]
    conn = sqlite3.connect(db)
    for t in table_list:
        drop_table(conn, t)
        create_table(conn, create_table_sql(t))

    
if __name__ == "__main__":
    initialize_table()