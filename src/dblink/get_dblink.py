import re
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
            "GEA": r"^E-GEA",
            "MTB": r"^MTB",
            "insdc": r"^CP",
            "insdc_master": r"K",
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
                return db_type


def get_dbxref(ids:List[str]) -> dict:
    """
    get_dblinkから関連するIDのリスト（List[str]）を取得し
    ElasticsearchのdbXrefsのスキーマに合わせたオブジェクト（List[dict]）に整形しオブジェクトを返す

    Args:
        id (str): BioSampleまたはBioProject

    Returns:
        List[dict]: [{"identifier": "", "type": ""},,]
    """
    relation = RelationObject()
    dbxref_list = [relation.identify_type(x) for x in ids]
    return dbxref_list


if __name__ == "__main__":
    id_lst = []
    print(get_dbxref(id_lst))


