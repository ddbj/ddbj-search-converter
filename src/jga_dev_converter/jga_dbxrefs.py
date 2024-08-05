import re
import sqlite3
from typing import List


table_names = {
    "jga-study": [
        "jga_study2humID",
        "jga_study2jga_dataset",
        "jga_study2pubmed_id",
        "metabobank2jga_study",
    ],
    "jga-dataset": [
        "jga_study2jga_dataset",
    ]
}


class RelationObject():
    def __init__(self) -> None:
        """
        IDのタイプごとの頭文字を定義する
        """
        self.relation_patterns = {
            "biosample": r"^SAM",
            "bioproject": r"^PRJ",
            "sra-experiment": r"(S|D|E)RX",
            "sra-run": r"(S|D|E)RR",
            "sra-sample": r"(S|D|E)RS",
            "sra-study": r"(S|D|E)RP",
            "gea": r"^E-GEA",
            "assemblies": r"^GCA",
            "mtb": r"^MTB",
            "jga-study": r"^JGAS",
            "jga-dataset": r"^JGAD",
            "jga-policy": r"^JGAP",
            "jga-dac": r"^JGAC",
            "pubmed": r"\d",
            "insdc": r"([A-Z]{1}[0-9]{5}.[0-9]+|[A-Z]{2}[0-9]{6}|[A-Z]{2}[0-9]{6}.[0-9]+|[A-Z]{2}[0-9]{8}|[A-Z]{4}[0-9]{2}S?[0-9]{6,8})|[A-Z]{6}[0-9]{2}S?[0-9]{7,9}",
        }

    def identify_type(self, id:str) -> dict:
        """
        - idの頭文字よりtypeを判別する
        - 判定したtypeを付加したdictを返す
        Args:
            id (str): BioSample, BioProject,,,
        Returns:
            dict: identifier, type
        """
        # relation_mapでtypeに変換する・mapに含まれず値が数字で始まるIDであればtype: taxonomyを返す
        for db_type, pattern in self.relation_patterns.items():
            if re.match(pattern, id):
                return {"identifier": id, "type": db_type}


def get_dbxref(ids:List[str]) -> dict:
    """
    - get_dblinkから関連するIDのリスト（List[str]）を取得する
    - typeを付加しElasticsearchのdbXrefsのスキーマに合わせたオブジェクト（List[dict]）に整形しオブジェクトを返す
    Args:
        id (str): BioSampleまたはBioProject

    Returns:
        List[dict]: [{"identifier": "", "type": ""},,]
    """
    relation = RelationObject()
    dbxref_list = [relation.identify_type(x) for x in ids if x != None]
    return list(filter(None,dbxref_list))


def get_related_ids(id:str, type:str) -> List[dict]:
    """
    全ての関係テーブルから指定したIDに関連するIDを取得する
    モジュールとして get_related_ids(id)
    Args:
        id (str): 捜査対象のID
    return: 関係テーブルの引数のIDをどちらかに含む相補的なIDリストを変換したdbXref形式のobject
    """
    # related_idsにdblinkとsra_accessionsの二系統のid relationをまとめて追加する
    related_ids = []
    # dbXrefが1M要素あるようなドキュメントがあり、Elasticsearchのリクエストが413となるのを回避するため設定
    limit = 10000

    # TODO:環境に合わせて設定・環境変数にする　dblinkのSQLiteデータベースのパス
    sqlite_db_path = '/home/w3ddbjld/tasks/ddbj-search-converter/src/dblink/ddbj_dblink.sqlite'
    conn = sqlite3.connect(sqlite_db_path)
    c = conn.cursor()

    for table_name in table_names[type]:
        c.execute(f'SELECT * FROM {table_name} WHERE field1 = ? OR field2 = ? LIMIT {limit}', (id, id))
        for row in c.fetchall():
            related_ids.append(row[0])
            related_ids.append(row[1])
    conn.close()

    related_ids = [i for i in related_ids if i != id]
    return get_dbxref(list(set(related_ids)))



if __name__ == "__main__":
    infos = [
        {"type": "jga-study","identifier": "JGAS000715"},
        {"type": "jga-dataset","identifier": "JGAD000848"},
        {"type": "jga-study","identifier": "JGAS000722"},
        {"type": "jga-dataset","identifier": "JGAD000855"},
    ]
    for info in infos:
        print(info["type"])
        print(get_related_ids(info["identifier"], info["type"]))
