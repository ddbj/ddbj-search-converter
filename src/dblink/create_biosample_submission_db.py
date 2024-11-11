import pg8000.native
import sqlite3
import datetime


def submission_records(conn, chunksize):
    """
    chunk_sizeごとbiosampleのdate情報を取得し
    sqliteにaccession, dateCreated, datePublishec, dateModifiedを保存する
    Args:
        conn (_type_): _description_
        table_name (_type_): _description_
        chunk_size (_type_, optional): _description_. Defaults to chunk_size.
    """

    offset = 0

    # TODO: create_dateが取得できればOKなためsubmission
    while True:

        # sumibission_idの inner joinを1レコードに制限する
        q = f"SELECT DISTINCT ON (submission_id) * \
        FROM mass.sample \
        ORDER BY submission_id, some_column \
        LIMIT {chunksize} OFFSET {offset} ;"
        res = conn.run(q)
        while True:
            if not res:
                break
            yield res
            offset += chunksize


def count_submission_id(conn):
    q = f"SELECT COUNT(*) \
            FROM ( 
            SELECT DISTINCT ON (submission_id) *
            FROM mass.sample
            ORDER BY submission_id
        ) AS subquery;"
    res = conn.run(q)
    return res


def cast_dt(dt):
    if dt is None:
        return None
    else:
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
    

def drop_table(conn, cursor, table_name):
    """
    レコードを取り込む前にtableをdropする
    """
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    result = cursor.fetchone()
    if result:
        cursor.execute(f"DROP TABLE {table_name};")
    conn.commit()


def create_date_table(db, table_name):
    """
    accessionとdateを紐付けるテーブルを生成する
    既存のtableがあった場合一旦dropする
    """
    # データベースファイル名
    
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    drop_table(conn, cursor, table_name)
    
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        accession TEXT,
        date_created TEXT,
        date_published TEXT,
        date_modified TEXT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()


def store_records(table_name, db, records):
    """
    sqliteにdateデータを保存する
    """
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    q = f"INSERT ITNO {table_name} VALUES (?, ?, ?, ?);"
    t = [(r[0], cast_dt(r[1]), cast_dt(r[2]),cast_dt(r[3])) for r in records]
    cur.executemany(q, t)
    conn.commit()


def count_records(db, table_name):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    res = cursor.fetchall()
    return res


if __name__ == "__main__":
    # sqliteのdbを設定
    table_name = "date_biosample"
    db_file = '/home/w3ddbjld/tasks/sra/resources/date.db'
    create_date_table(db_file, table_name)

    # postgresqlからsqliteにレコードをコピーして保存する
    chunk_size = 100000
    conn_ps = pg8000.native.Connection(
        host='at098',
        port='54301',
        user='const',
        password='',
        database='biosample'
    )

    print(count_submission_id(conn_ps))

    '''
    for records in submission_records(conn_ps, chunk_size):
        print(records)
        store_records(table_name, db_file, records)
    '''