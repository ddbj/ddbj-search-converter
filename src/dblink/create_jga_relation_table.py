import sqlite3

TABLE_LIST = [
    "dataset_policy_relation",
    "dataset_dac_relation",
    "dataset_study_relation",
    "study_dac_relation",
    "study_policy_relation",
    "policy_dac_relation"
]

# TODO: create_id_relation_tableと同じ処理なため
# table_listをまとめ一つのモジュールでid relationを作成する
# もしくはtable_listを引数にし呼び出し先でtable_listを定義するようにする

def create_table_sql(table_name):
    
    table_sql = f"""
    CREATE TABLE {table_name} (
        id0 VARCHAR(16),
        id1 VARCHAR(16)
    );
    """
    #print("table: ", table_sql)
    return table_sql


def create_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(create_table_sql(table_name))
    conn.commit()


def drop_table(conn, table_name):
    cur = conn.cursor()
    q = f"DROP TABLE IF EXISTS {table_name};"
    cur.execute(q)
    conn.commit()


def initialize_table(db):
    """
    drop_all,create_tableを呼び
    table_listのtableを初期化する
    """
    conn = sqlite3.connect(db)
    for t in TABLE_LIST:
        drop_table(conn, t)
        create_table(conn, t)


def create_indexes(db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    for t in TABLE_LIST:
        sql0 = f"CREATE INDEX index_{t}_0 ON {t}(id0)"
        sql1 = f"CREATE INDEX index_{t}_1 ON {t}(id1)"
        cur.execute(sql0)
        cur.execute(sql1)
        conn.commit()


#if __name__ == "__main__":
#    initialize_table(db)
