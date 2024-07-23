import sqlite3
import csv
import subprocess


file_path = [
"https://ddbj.nig.ac.jp/public/dblink/assembly_genome-bp/assembly_genome2bp.tsv",
"https://ddbj.nig.ac.jp/public/dblink/assembly_genome-bs/assembly_genome2bs.tsv",
"https://ddbj.nig.ac.jp/public/dblink/bioproject-biosample/bioproject2biosample.tsv",
"https://ddbj.nig.ac.jp/public/dblink/bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/biosample-bioproject/biosample2bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/gea-bioproject/gea2bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/gea-biosample/gea2biosample.tsv",
"https://ddbj.nig.ac.jp/public/dblink/insdc-bioproject/insdc2bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/insdc-biosample/insdc2biosample.tsv",
"https://ddbj.nig.ac.jp/public/dblink/insdc_master-bioproject/insdc_master2bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/insdc_master-biosample/insdc_master2biosample.tsv",
"https://ddbj.nig.ac.jp/public/dblink/mtb2bp/mtb_id_bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/mtb2bs/mtb_id_biosample.tsv",
"https://ddbj.nig.ac.jp/public/dblink/ncbi_biosample_bioproject/ncbi_biosample_bioproject.tsv",
"https://ddbj.nig.ac.jp/public/dblink/taxonomy_biosample/trace_biosample_taxon2bs.tsv",
]

def create_database(ftp_path, ftp_filename,table_name,sqlite_db_path):
  """
  FTPサイトからTSVファイルをダウンロードし、SQLiteデータベースを構築する

  引数:
    ftp_host: FTPサーバーのホスト名
    ftp_user: FTPサーバーのユーザー名
    ftp_password: FTPサーバーのパスワード
    ftp_filename: ダウンロードするTSVファイル名
    sqlite_db_path: 作成するSQLiteデータベースのパス

  戻り値:
    なし
  """
  # オプションを指定してwgetを実行
  subprocess.call(['wget', '-O', f'./data/{ftp_filename}', ftp_path])

  # SQLiteデータベースを作成
  conn = sqlite3.connect(sqlite_db_path)
  c = conn.cursor()

  # テーブルを作成
  c.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
      field1 TEXT,
      field2 TEXT
    );
  ''')

  # TSVファイルを読み込み、データベースに挿入
  with open(f'./data/{ftp_filename}', 'r') as f:
    for line in f:
      field1, field2 = line.strip().split('\t')
      c.execute(f'INSERT INTO {table_name} VALUES (?, ?)', (field1, field2))

  # インデックスを作成
  c.execute(f'CREATE INDEX {table_name}_field1 ON {table_name} (field1)')
  c.execute(f'CREATE INDEX {table_name}_field2 ON {table_name} (field2)')

  # コミットしてクローズ
  conn.commit()
  conn.close()

if __name__ == '__main__':
  """
  ddbj/dblinkにあるBioSampleとBioProjectの関連データをダウンロードし、SQLiteデータベースを作成する
  """
  # SQLiteデータベースのパス
  sqlite_db_path = 'ddbj_dblink.db'

  # 関係データのファイル毎にデータベースを作成
  for i in range(len(file_path)):
    # FTPサーバーの情報
    ftp_path = file_path[i]
    ftp_filename = file_path[i].split("/")[-1]
    table_name = ftp_filename.split(".")[0]
    #ftp_host = file_path[i].split("/")[2]
    # データベースを作成
    create_database(ftp_path, ftp_filename, table_name, sqlite_db_path)

  print('データベース作成完了')
