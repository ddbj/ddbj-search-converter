import csv
import sqlite3
from lxml import etree
from typing import NewType, List, Tuple


BIOPROJECT_FILE = "/usr/local/resources/bioproject/bioproject"
BIOPROJECT_RELATION_DB = "/home/w3ddbjld/tasks/relations/bioproject_relation.sqlite"
OUTPUT_FILE_PATH = "/home/w3ddbjld/tasks/relations/bioproject_umbrella2bioproject.csv"
TABLE_LIST = [
    "bioproject_umbrella2bioproject"
]


def primary_umbrella_pair(xml_path):
    context = etree.iterparse(xml_path, events=("start", "end"))
    current_link = {}
    inside_link = False

    for event, elem in context:
        if event == "start" and elem.tag == "Link":
            inside_link = True
            current_link = {}
        elif inside_link and event == "start":
            if elem.tag == "Hierarchical":
                current_link['hierarchical_type'] = elem.attrib.get('type')
            elif elem.tag == "ProjectIDRef":
                current_link['project_id'] = elem.attrib.get('accession')
            elif elem.tag == "MemberID":
                current_link['member_id'] = elem.attrib.get('accession')

        elif event == "end" and elem.tag == "Link":
            if current_link.get("hierarchical_type") in {"TopSingle", "TopAdmin"}:
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


def create_table_sql(table_name):
    # TODO:有効のrelationであるためカラム名を意味のあるchild,parentとした
    # id0, id1のような名称の方が良いかは検討

    table_sql = f"""
    CREATE TABLE {table_name} (
        child VARCHAR(16),
        parent VARCHAR(16)
    );
    """
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
        sql0 = f"CREATE INDEX index_{t}_0 ON {t}(child)"
        sql1 = f"CREATE INDEX index_{t}_1 ON {t}(parent)"
        cur.execute(sql0)
        cur.execute(sql1)
        conn.commit()

def write_csv(child: str, parent: str):
    with open(OUTPUT_FILE_PATH , "a") as f:
        writer = csv.writer(f)
        writer.writerow(
            [child, parent]
            )


def get_primary_umbrella_pair():
    """
    primary_umbrella_pairは
    [{'type': 'TopSingle', 'project_id': 'PRJNA3', 'member_id': 'PRJNA19847'},,]
    を返すので順次storeする
    """
    conn = sqlite3.connect(BIOPROJECT_RELATION_DB)
    cur = conn.cursor()
    for umbrella2bioproject in primary_umbrella_pair(BIOPROJECT_FILE):
        # (primay, umbrella)のセットを渡す
        cur.execute(
            f"INSERT INTO {TABLE_LIST[0]} (child, parent) VALUES (?, ?)",
            (umbrella2bioproject["project_id"], umbrella2bioproject["member_id"]),
        )

        # sqlite3に保存するのと同時にcsvに書き出す
        write_csv(umbrella2bioproject["project_id"], umbrella2bioproject["member_id"])
    conn.commit()

if __name__ == "__main__":
    initialize_table(BIOPROJECT_RELATION_DB)
    get_primary_umbrella_pair()
    create_indexes(BIOPROJECT_RELATION_DB)