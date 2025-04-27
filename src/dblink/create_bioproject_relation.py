import csv
import sqlite3
from lxml import etree
from typing import Set


BIOPROJECT_FILE = "/usr/local/resources/bioproject/bioproject.xml"
BIOPROJECT_RELATION_DB = "/home/w3ddbjld/tasks/relations/bioproject_relation.sqlite"
OUTPUT_FILE_PATH = "/home/w3ddbjld/tasks/relations/bioproject_umbrella2bioproject.csv"
BIOPROJECT_ACCESSION_TABLE = "bioproject_accessions"
TABLE_LIST = [
    "bioproject_umbrella2bioproject"
]


class BioProjectUmbrellaPair:
    def __init__(self):
        self.store_bioproject_accessions()

    def primary_umbrella_pair(self, xml_path):
        context = etree.iterparse(xml_path, events=("start", "end"))
        current_link = {}
        inside_link = False

        cnt_member_id = 0
        cnt_project_id = 0
        for event, elem in context:
            if event == "start" and elem.tag == "Link":
                inside_link = True
                current_link = {}
            elif inside_link and event == "start":
                if elem.tag == "Hierarchical":
                    current_link['hierarchical_type'] = elem.attrib.get('type')
                elif elem.tag == "ProjectIDRef":
                    cnt_project_id += 1
                    current_link['project_id'] = elem.attrib.get('accession')
                elif elem.tag == "MemberID":
                    cnt_member_id += 1
                    current_link['member_id'] = elem.attrib.get('accession')

            elif event == "end" and elem.tag == "Link":
                # TopSingleは除外しTopAdminのみを取得する
                if current_link.get("hierarchical_type") == "TopAdmin":
                    yield {
                        "type": current_link["hierarchical_type"],
                        # primary id
                        "project_id": current_link.get("project_id"),
                        # umbrella id
                        "member_id": current_link.get("member_id")
                    }
                inside_link = False
                current_link.clear()
                elem.clear()

    def get_primary_umbrella_pair(self):
        """
        - self.primary_umbrella_pair()が
        [{'type': 'TopSingle', 'project_id': 'PRJNA3', 'member_id': 'PRJNA19847'},,]
        を返すので順次sqliteに保存する。

        - sqliteに保存する際にself.all_bioproject_accessionにaccessionが存在するか確認する.
        存在しない場合はprivateであるとし保存は行わない
        """
        conn = sqlite3.connect(BIOPROJECT_RELATION_DB)
        cur = conn.cursor()
        # for devlepment
        for umbrella2bioproject in self.primary_umbrella_pair(BIOPROJECT_FILE):
            # primaryがbioproject_accessions:sqliteに存在する場合のみ処理を行う
            if check_bioproject_accession(cur, umbrella2bioproject["project_id"]):
                # (primay, umbrella)のセットを渡す
                cur.execute(
                    f"INSERT OR IGNORE INTO {TABLE_LIST[0]} (child, parent) VALUES (?, ?)",
                    (umbrella2bioproject["project_id"], umbrella2bioproject["member_id"]),
                )
                # sqlite3に保存するのと同時にcsvに書き出す
                #write_csv(umbrella2bioproject["project_id"], umbrella2bioproject["member_id"])

        conn.commit()

    def store_bioproject_accessions(self) -> set:
        """
        bioproject.xmlからbioprojectのidを取得し
        sqliteに保存する。
        """
        tree = etree.parse(BIOPROJECT_FILE)
        root = tree.getroot()
        accessions = root.xpath('//ArchiveID/@accession')
        create_bioproject_accession_db(set(accessions))
        

def write_csv(child: str, parent: str):
    with open(OUTPUT_FILE_PATH , "a") as f:
        writer = csv.writer(f)
        writer.writerow(
            [child, parent]
            )


# ここからsqliteに関する処理
# テーブル作成のsql
def create_table_sql(table_name):
    # TODO:有効のrelationであるためカラム名を意味のあるchild,parentとした
    # id0, id1のような名称の方が良いかは検討

    table_sql = f"""
    CREATE TABLE {table_name} (
        child VARCHAR(16),
        parent VARCHAR(16),
        UNIQUE (child, parent)
    );
    """
    return table_sql

# テーブル作成
def create_table(table_name):
    conn = sqlite3.connect(BIOPROJECT_RELATION_DB)
    cur = conn.cursor()
    cur.execute(create_table_sql(table_name))
    conn.commit()

# 初期化のためにテーブル削除のsqlと実行
def drop_table(table_name):
    conn = sqlite3.connect(BIOPROJECT_RELATION_DB)
    cur = conn.cursor()
    q = f"DROP TABLE IF EXISTS {table_name};"
    cur.execute(q)
    conn.commit()

# テーブル初期化
def initialize_table():
    """
    drop_all,create_tableを呼び
    table_listのtableを初期化する
    """
    for t in TABLE_LIST:
        drop_table(t)
        create_table(t)
    
    # ACCESSION_TABLEの初期化
    drop_table(BIOPROJECT_ACCESSION_TABLE)

# all_bioproject_accessionを保存するdbを作成
def create_bioproject_accession_db(accessions: Set[str]):
    conn = sqlite3.connect(BIOPROJECT_RELATION_DB)
    cur = conn.cursor()
    
    cur.execute(f'''
    CREATE TABLE IF NOT EXISTS {BIOPROJECT_ACCESSION_TABLE} (
        accession TEXT UNIQUE
    );
    ''')
    conn.commit()

    data_to_sql = [(acc, ) for acc in accessions]
    cur.executemany(f"INSERT OR IGNORE INTO {BIOPROJECT_ACCESSION_TABLE} (accession) VALUES (?)", data_to_sql)
    conn.commit()

# bioprojectのaccessionの存在を確認する
def check_bioproject_accession(cur, acc: str) -> bool:
    cur.execute(f'SELECT COUNT(*) FROM {BIOPROJECT_ACCESSION_TABLE} WHERE accession = ?', (acc,))
    result = cur.fetchone()
    return result[0] > 0

# インデックス作成
def create_indexes():
    conn = sqlite3.connect(BIOPROJECT_RELATION_DB)
    cur = conn.cursor()

    for t in TABLE_LIST:
        sql0 = f"CREATE INDEX index_{t}_0 ON {t}(child)"
        sql1 = f"CREATE INDEX index_{t}_1 ON {t}(parent)"
        cur.execute(sql0)
        cur.execute(sql1)
        conn.commit()

# sqliteからcsvに書き出す
def export_to_csv(db, table_name):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    with open(OUTPUT_FILE_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        cur.execute(f"SELECT child, parent FROM {table_name}")
        for row in cur.fetchall():
            writer.writerow(row)
# ここまでsqliteに関する処理ブロック



if __name__ == "__main__":
    initialize_table()
    # bioproject.xmlからprimary-umbrellaを取得しsqliteに書き出す
    bppair = BioProjectUmbrellaPair()
    bppair.get_primary_umbrella_pair()
    create_indexes()
    # sqliteからcsvに書き出す
    export_to_csv(BIOPROJECT_RELATION_DB, TABLE_LIST[0])
    print("Done")