import pg8000.native
import datetime

def create_date_table():
    conn = pg8000.native.Connection(
        host='at098',
        port='54301',
        user='const',
        password='const',
        database='bioproject'
    )

    # mass.bioproject_summaryのsubmission_idとmass.projectのsubmission_idでINNER JOINし
    # bioproject_summaryのaccessionとprojectのcreate_date, date_published, date_modifiedを取得
    q = 'SELECT s.accession AS accession, p.create_date AS date_created, \
    p.release_date AS date_published, p.modified_date AS date_modified  \
    FROM mass.bioproject_summary s INNER JOIN mass.project p ON s.submission_id = p.submission_id ;'

    date_table = []
    for row in conn.run(q):
        date_table.append(row)

    return date_table

def cast_dt(dt):
    if dt is None:
        return None
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f%+09')
    
def store_db():
    """
    sqliteに保存する
    """
    pass


def clear_db():
    """
    レコードを取り込む前にtableをdropする
    """
    pass



if __name__ == "__main__":
    t = create_date_table()
    # datetimeってprintすると文字列にstrtimeされてcastされる？？

    t = [[r[0], cast_dt(r[1]), cast_dt(r[2]),cast_dt(r[3])] for r in t]   
    print(t[0:2])