import pg8000.native
import sqlite3
import datetime


chunk_size = 100000
sqlite_db = "date.db"
sqlite_table = "date_biosample"

def chunks(conn, size):
    """
    chunk_sizeごとbiosampleのdate情報を取得し
    sqliteにaccession, dateCreated, datePublishec, dateModifiedを保存する
    Args:
        conn (_type_): _description_
        table_name (_type_): _description_
        chunk_size (_type_, optional): _description_. Defaults to chunk_size.
    """
    offset = 0
    q = f"SELECT s.accession_id AS accession, p.create_date AS date_created, \
    p.release_date AS date_published, p.modified_date AS date_modified  \
    FROM mass.biosample_summary s INNER JOIN mass.sample p ON s.submission_id = p.submission_id OFFSET {offset} LIMIT {size};"
    while True:
        rows = conn.run(q)
        if not rows:
            break
        yield rows
        offset += chunk_size

def db2db():
    conn = pg8000.native.Connection(
        host='at098',
        port='54301',
        user='const',
        password='const',
        database='biosample'
    )

    for chunk in chunks(conn, chunk_size):
        for row in chunk:
            store_records(row)


def store_records(d):
    """
    sqliteにrecordsを保存
    Args:
        d (_type_): _description_
    """
    c = sqlite3.connect(sqlite_db)
    cur = c.cursor()
    # tableが無ければtableを生成し、accession, date_created, date_publilshed, date_modifiedの4つのフィールドに値を保存する
    q = ""
    cur.execute(q)
    c.commit()


def bs_date_query_sample(conn, ):
    conn = pg8000.native.Connection(
        host='at098',
        port='54301',
        user='const',
        password='const',
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

def cast_dt(dt):
    if dt is None:
        return None
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f%+09')
    

def create_table(name):
    """
    テーブルを作成する
    """
    conn = sqlite3.connect(db_name)
    q = f"CREATE TABLE IF NOT EXISTS {name} (accession TEXT,date_created TEXT,date_published TEXT,date_modified TEXT);"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    conn.close()



def clear_table(db_name,table_name):
    """
    レコードを取り込む前にtableをdropする
    """
    conn = sqlite3.connect(db_name)
    q = f"DROP TABLE {table_name}"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    conn.close()


def create_index():
    """
    sqliteのindexを生成する。
    生成する前に以前のindexが存在していたら削除する
    """
    pass


if __name__ == "__main__":
    clear_table(sqlite_db, sqlite_table)