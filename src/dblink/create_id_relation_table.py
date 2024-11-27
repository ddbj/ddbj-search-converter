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


def  create_StudyBioProject():
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()



def drop_all():
    """
    すべてのtableを破棄する
    """
    pass


def create_index(input_path, db_path):
    """
    - このモジュールはcreate_accession_db_m.pyから呼ばれる
    - id relation tableを生成する
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
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for tbl in table_list:
        cur.execute(drop_all(tbl))
        cur.execute(create_table_sql(tbl))


    conn.commit()
    cursor.close()
    pass


def main():
    


def main():
    pass