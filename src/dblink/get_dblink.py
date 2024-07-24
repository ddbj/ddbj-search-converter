import re
import sqlite3
from typing import List

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


def get_related_ids(id:str) -> list:
    """
    全ての関係テーブルから指定したIDに関連するIDを取得する
    モジュールとして get_related_ids(id)
    Args:
        id (str): 捜査対象のID
    return: 関係テーブルの引数のIDをどちらかに含む相補的なIDリスト
    """
    related_ids = []

    # dblinkのSQLiteデータベースのパス
    sqlite_db_path = 'ddbj_dblink.sqlite'
    conn = sqlite3.connect(sqlite_db_path)
    c = conn.cursor()

    # 関係テーブルのリスト
    table_names = [
        "assembly_genome2bp",
        "assembly_genome2bs",
        "bioproject2biosample",
        "bioproject_umbrella2bioproject",
        "biosample2bioproject",
        "gea2bioproject",
        "gea2biosample",
        "insdc2bioproject",
        "insdc2biosample",
        "insdc_master2bioproject",
        "insdc_master2biosample",
        "mtb_id_bioproject",
        "mtb_id_biosample",
        "ncbi_biosample_bioproject",
        "trace_biosample_taxon2bs"
    ]
    
    for table_name in table_names:
        c.execute(f'SELECT * FROM {table_name} WHERE field1 = ? OR field2 = ?', (id, id))
        for row in c.fetchall():
            related_ids.append(row[0])
            related_ids.append(row[1])
    conn.close()

    # TODO: sra_accessions.sqliteの関係データを取得しrelated_idsに追加する
    acc_table_names = [
        "analysis_submission",      
        "experiment_bioproject",
        "experiment_biosample",
        "experiment_sample",
        "experiment_study", 
        "run_biosample",
        "run_experiment",
        "run_sample",
        "study_bioproject", 
        "study_experiment",
        "study_submission",
        "sample_biosample",
        "sample_experiment",
        ]

    acc_db_path = 'sra_accessions.sqlite'
    conn = sqlite3.connect(acc_db_path)
    ac = conn.cursor()
    for acc_table_name in acc_table_names:
        ac.execute(f'SELECT * FROM {acc_table_name} WHERE field1 = ? OR field2 = ?', (id, id))
        for row in ac.fetchall():
            related_ids.append(row[0])
            related_ids.append(row[1])

    related_ids = [i for i in related_ids if i != id]
    return list(set(related_ids))


if __name__ == "__main__":
    # sample ids
    id_lst = [
        "PRJNA3",
        "PRJNA5",
        "SAMN00000002"
    ]
    for id in id_lst:
        related_ids = get_related_ids(id)
        print(get_dbxref(related_ids))


