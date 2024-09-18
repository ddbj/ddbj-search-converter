import pg8000.native
import sqlite3
import datetime


def date_records(conn, chunksize):
    """
    chunk_sizeごとbiosampleのdate情報を取得し
    sqliteにaccession, dateCreated, datePublishec, dateModifiedを保存する
    Args:
        conn (_type_): _description_
        table_name (_type_): _description_
        chunk_size (_type_, optional): _description_. Defaults to chunk_size.
    """

    offset = 0

    # TODO: distinctを取らないと膨大なサイズのデータになる
    while True:
        q = f"SELECT DISTINCT s.accession_id AS accession, p.create_date AS date_created, \
        p.release_date AS date_published, p.modified_date AS date_modified  \
        FROM mass.biosample_summary s INNER JOIN mass.sample p ON s.submission_id = p.submission_id \
        LIMIT {chunksize} OFFSET {offset} ;"
        res = conn.run(q)
        while True:
            if not res:
                break
            yield res
            offset += chunksize


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


def bs_date_query_sample(conn, ):
    conn = pg8000.native.Connection(
        host='at098',
        port='54301',
        user='const',
        password='',
        database='biosample'
    )
    q = 'SELECT s.accession_id AS accession, p.create_date AS date_created, \
    p.release_date AS date_published, p.modified_date AS date_modified  \
    FROM mass.biosample_summary s INNER JOIN mass.sample p ON s.submission_id = p.submission_id ;'

    date_table = []
    for row in conn.run(q):
        date_table.append(row)
    table = [[r[0], cast_dt(r[1]), cast_dt(r[2]),cast_dt(r[3])] for r in date_table]   
    return date_table


def create_index(db, table_name):
    """
    sqliteのindexを生成する。
    """
    index_name = f"idx_{table_name}_accession"
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute(f"CREATE INDEX {index_name} ON {table_name} (accession)")
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
    chunk_size = 10000
    conn_ps = pg8000.native.Connection(
        host='at098',
        port='54301',
        user='const',
        password='',
        database='biosample'
    )

    for records in date_records(conn_ps, chunk_size):
        store_records(table_name, db_file, records)

    create_index(db_file, table_name)
    print("date_biosample: ", count_records(db_file, table_name))
