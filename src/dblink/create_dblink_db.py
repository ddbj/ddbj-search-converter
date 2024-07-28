import sqlite3
import csv
import subprocess

# TODO:スパコン内部のパスに変更
file_path = [
"/lustre9/open/shared_data/dblink/assembly_genome-bp/assembly_genome2bp.tsv",
"/lustre9/open/shared_data/dblink/assembly_genome-bs/assembly_genome2bs.tsv",
"/lustre9/open/shared_data/dblink/bioproject-biosample/bioproject2biosample.tsv",
"/lustre9/open/shared_data/dblink/bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv",
"/lustre9/open/shared_data/dblink/biosample-bioproject/biosample2bioproject.tsv",
"/lustre9/open/shared_data/dblink/gea-bioproject/gea2bioproject.tsv",
"/lustre9/open/shared_data/dblink/gea-biosample/gea2biosample.tsv",
"/lustre9/open/shared_data/dblink/insdc-bioproject/insdc2bioproject.tsv",
"/lustre9/open/shared_data/dblink/insdc-biosample/insdc2biosample.tsv",
"/lustre9/open/shared_data/dblink/insdc_master-bioproject/insdc_master2bioproject.tsv",
"/lustre9/open/shared_data/dblink/insdc_master-biosample/insdc_master2biosample.tsv",
"/lustre9/open/shared_data/dblink/mtb2bp/mtb_id_bioproject.tsv",
"/lustre9/open/shared_data/dblink/mtb2bs/mtb_id_biosample.tsv",
"/lustre9/open/shared_data/dblink/ncbi_biosample_bioproject/ncbi_biosample_bioproject.tsv",
"/lustre9/open/shared_data/dblink/taxonomy_biosample/trace_biosample_taxon2bs.tsv",
]

def create_database(file,file_name,table_name,sqlite_db_path):
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
  # deplicated
  # subprocess.call(['wget', '-O', f'./data/{ftp_filename}', ftp_path])

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
  with open(file, 'r') as f:
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
    file = file_path[i]
    filename = file.split("/")[-1]
    table_name = filename.split(".")[0]
    #ftp_host = file_path[i].split("/")[2]
    # データベースを作成
    create_database(file, filename, table_name, sqlite_db_path)

  print('データベース作成完了')
