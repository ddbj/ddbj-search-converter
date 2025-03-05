"""
- jga-study, jga-dataset, jga-policy, jga-dacのxmlからjsonlファイルへの変換を行う
- それぞれのjsonlのElasticsearchへのbulk insertを行う
- jgaのid関係データベースをあらかじめアップデートしておく（dblink/create_jga_relation_db.py）
- 日付データはcsvより取り込む
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import xmltodict
from lxml import etree
from schma import (Agency, jga, Distribution,
                                          ExternalLink, Grant, Organism,
                                          Organization, Publication, Xref)

DEFAULT_BATCH_SIZE = 2000
JGA_STUDY_XML_FILE = "/lustre9/open/shared_data/jga/metadata-history/metadata/jga-study.xml"
JGA_DATASET_XML_FILE = "/lustre9/open/shared_data/jga/metadata-history/metadata/jga-dataset.xml"
DATASET_DATE_FILE = "/lustre9/open/shared_data/jga/metadata-history/metadata/dataset.date.csv"
STUDY_DATE_FILE = "/lustre9/open/shared_data/jga/metadata-history/metadata/study.date.csv"



def xml_to_jsonl(
        xml_file: Path,
        output_dir: Path,
        is_ddbj: bool,
        batch_size: int = DEFAULT_BATCH_SIZE
) -> None:
    pass
    
# TODO: jga_devのままなので大きく修正必要
def study_to_jsonl(input:FilePath):
    context = etree.iterparse(input, tag="STUDY")
    docs:list[dict] = []
    i = 0
    for events, element in context:
        if element.tag == "STUDY":
            doc = {}
            doc["accession"] = element.attrib["accession"]
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["properties"] = metadata["STUDY"]

            doc["identifier"]= doc["accession"]
            doc["title"] = doc["properties"].get("DESCRIPTOR").get("STUDY_TITLE")
            doc["description"] = doc["properties"].get("DESCRIPTOR").get("STUDY_ABSTRACT")
            doc["name"] = doc["properties"].get("alias")
            doc["type"] = "jga-study"
            doc["url"] = "https://ddbj.nig.ac.jp/resource/jga-study/" + doc["accession"]
            doc["sameAs"] = None
            doc["isPartOf"] = "jga"
            doc["organism"] = {"identifier": 9606, "name": "Homo sapiens"}
            # TODO: dbxreをdblinkモジュールより取得し追加する
            # jga-policy, jga-dac, jga-study: jga-datasetのrelationを埋める
            doc["dbXrefs"] 
            # ?? xmlから取得できる？
            doc["status"] = "public"
            # ?? xmlから取得できる？？
            doc["visibility"] = "unrestricted-access"
            doc["dateCreated"] = ""
            doc["dateModified"] = ""
            doc["datePublished"] = ""

            docs.append(doc)
            i +=1

        clear_element(element)
        if i > batch_size:
            dict2jsonl(docs)
            docs = []




def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


def dict2jsonl(docs: List[dict]):
    jsonl_output = "jga-study_20240808.jsonl"
    with open(jsonl_output, "a") as f:
        for doc in docs:
            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "bioproject", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


# bulk insert

def bulk_insert_to_es():
    pass


# utility


class getDates:
    def __init__(self):
        self.dataset_path = DATASET_DATE_FILE
        self.study_dates = {}
        self.dataset_dates = {}

    def dates(self, type: str, id: str):
        """
        idをキーとしdatecreated,datepublished,datemodifiedを取得
        """
        try:
            # TODO: typeは引数で指定
            with open(STUDY_DATE_FILE, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                header = next(reader)
                dict_list = []
                for row in reader:
                    if len(row) > 0:
                        key = row[0]
                        value = row[1:] if len(row) > 1 else None
                        dict_list.append({key: value})
                return dict_list
        except Exception as e:
            print(f"error occured: {e}")


def main():
    """
    - 変換対象のxmlをjsonlに変換
        - 関係データ(dbXrefsに含める)の取得
        - 日時情報をcsvより取得
        - dictとjsonlに変換
        - ヘッダ行をつけてファイルに書き出す
    - jsonlをElasticsearchにバルクインサートする
    """
    types = [
        {
            "type": "jga-study",
            "tag": "STUDY",
            "file_path": JGA_STUDY_XML_FILE
        },
        {
            "type": "jga-dataset",
            "tag": "DATASET",
            "file_path": JGA_DATASET_XML_FILE
        }
    ]

    # typeごとに変換


    

if __name__ == "__main__":
    main()