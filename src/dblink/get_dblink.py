import re
import sqlite3
from typing import List

table_names = {
    "bioproject":[
        "assembly_genome2bp",
        "bioproject2biosample",
        "bioproject_umbrella2bioproject",
        "biosample2bioproject",
        "gea2bioproject",
        # "insdc2bioproject",
        # "insdc_master2bioproject",
        "mtb_id_bioproject",
        "ncbi_biosample_bioproject",
    ],
    "biosample":[
        "assembly_genome2bs",
        "bioproject2biosample",
        "biosample2bioproject",
        "gea2biosample",
        # "insdc2biosample",
        # "insdc_master2biosample",
        "mtb_id_biosample",
        "ncbi_biosample_bioproject",
        "trace_biosample_taxon2bs"
    ]
}

acc_table_names = {
    "bioproject":[
        "experiment_bioproject",
        "study_bioproject", 
    ],
    "biosample":[
        "experiment_biosample",
        "experiment_sample",
        "run_biosample",
        "sample_biosample",
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
            "metabobank": r"^MTB",
            "taxonomy": r"^[0-9]+"
            #"insdc": r"([A-Z]{1}[0-9]{5}.[0-9]+|[A-Z]{2}[0-9]{6}|[A-Z]{2}[0-9]{6}.[0-9]+|[A-Z]{2}[0-9]{8}|[A-Z]{4}[0-9]{2}S?[0-9]{6,8})|[A-Z]{6}[0-9]{2}S?[0-9]{7,9}",
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
                # searchに存在するobjectの場合
                # https://ddbj.nig.ac.jp/search/{type}/{id}
                # assembliesの場合 https://www.ncbi.nlm.nih.gov/datasets/genome/{id}/
                # mtbの場合 https://mb2.ddbj.nig.ac.jp/study/{id}.html
                # geaの場合 https://www.ddbj.nig.ac.jp/gea/index.html AOEが停止しているので直接のリンクが無い
                url = ""
                match db_type:
                    case "biosample":
                        url = f"https://ddbj.nig.ac.jp/resource/{db_type}/{id}"
                    case "bioproject":
                        url = f"https://ddbj.nig.ac.jp/resource/{db_type}/{id}"
                    case "sra-experiment":
                        url = f"https://ddbj.nig.ac.jp/resource/{db_type}/{id}"
                    case "sra-run":
                        url = f"https://ddbj.nig.ac.jp/resource/{db_type}/{id}"
                    case "sra-sample":
                        url = f"https://ddbj.nig.ac.jp/resource/{db_type}/{id}"
                    case "sra-study":
                        url = f"https://ddbj.nig.ac.jp/resource/{db_type}/{id}"
                    case "gea":
                        url = "https://www.ddbj.nig.ac.jp/gea/index.html"
                    case "assemblies":
                        url = f"https://www.ncbi.nlm.nih.gov/datasets/genome/{id}/"
                    case "metabobank":
                        url = f"https://mb2.ddbj.nig.ac.jp/study/{id}.html"
                    case "taxonomy":
                        url = f"https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={id}"

                return {"identifier": id, "type": db_type, "URL":url}


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
    モジュールとしてconverterよりidを指定され呼ばれる get_related_ids(id, type)
    Args:
        id (str): 捜査対象のID
    return: 関係テーブルの引数のIDをどちらかに含む相補的なIDリストを変換したdbXref形式のobject
    """
    # related_idsにdblinkとsra_accessionsの二系統のid relationをまとめて追加する
    related_ids = []
    # dbXrefが1M要素あるようなドキュメントがあり、Elasticsearchのリクエストが413となるのを回避するため設定
    limit = 10000

    # TODO:環境に合わせて設定・環境変数にする　dblinkのSQLiteデータベースのパス
    sqlite_db_path = 'ddbj_dblink.sqlite'
    conn = sqlite3.connect(sqlite_db_path)
    c = conn.cursor()

    for table_name in table_names[type]:
        c.execute(f'SELECT * FROM {table_name} WHERE field1 = ? OR field2 = ? LIMIT {limit}', (id, id))
        for row in c.fetchall():
            related_ids.append(row[0])
            related_ids.append(row[1])
    conn.close()



    # TODO: 環境に合わせて変更する・環境変数に埋め込む
    acc_db_path = 'sra_accessions.sqlite'
    conn = sqlite3.connect(acc_db_path)
    ac = conn.cursor()
    for acc_table_name in acc_table_names[type]:
        ac.execute(f'SELECT * FROM {acc_table_name} WHERE id0 = ? OR id1 = ? LIMIT {limit}', (id, id))
        for row in ac.fetchall():
            related_ids.append(row[0])
            related_ids.append(row[1])

    related_ids = [i for i in related_ids if i != id]
    # 共通項目のschemaに合わせ整形する
    return get_dbxref(list(set(related_ids)))


if __name__ == "__main__":
    # sample ids
    id_lst = [
        "PRJNA3",
        "PRJNA5",
        "SAMN00000002"
    ]
    for id in id_lst:
        # id listはget_dbxrefsによってwrapされたobjectとして返る
        related_ids = get_related_ids(id)


