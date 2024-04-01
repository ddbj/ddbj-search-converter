import sqlite3
import csv
import pandas as pd

db = ""
accessions = ""
table = "sra_accessions"

def add_to_db(row, con):
    # Function that make insert to your DB, make your own.
    row.to_sql(table, con, if_exists='append', index=False, method='multi')
    # row.to_sql('sra_accessions', con, if_exists='append', index_label='bioproject', method='multi')


def process_chunk(chunk):
    # Handles one chunk of rows from pandas reader.
    con = sqlite3.connect(db)
    #cur.execute('''DROP TABLE IF EXISTS SA''')
    #for row in chunk:
    add_to_db(chunk, con)
    con.commit()


def create_index():
    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute('CREATE INDEX indexbp ON sra_accessions(BioProject)')
    con.commit()
    con.close()


def drop_table(table):
    con = sqlite3.connect(db)
    cur = con.cursor()
    con.execute('DROP TABLE IF EXISTS {table}')
    con.commit
    con.close()


if __name__ == "__main__":
    """
    SRA_Accessions.tabをsqliteにインポートする
    """
    accessions = sys.argv[1]
    db = sys.argv[2]
    # 古いtableがあった場合削除する
    drop_table(table)
    reader = pd.read_csv(accessions, delimiter='\t', quoting=csv.QUOTE_NONE, on_bad_lines='skip', chunksize=10000)
    for r in reader:
        process_chunk(r)
    create_index()

