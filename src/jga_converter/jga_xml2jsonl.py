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
import os
# for dev 
sys.path.append(os.path.abspath("/mnt/ddbj/ddbj-search-converter/src"))
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET
import urllib.request
import urllib.parse
import xmltodict
import logging
import sqlite3
import base64
from lxml import etree
from schema import (JGA, Organism, Xref)
from dblink.create_jga_relation_db import create_jga_relation


DEFAULT_BATCH_SIZE = 500

JGA_STUDY_XML_FILE = '/lustre9/open/shared_data/jga/metadata-history/metadata/jga-study.xml'
JGA_DATASET_XML_FILE = '/lustre9/open/shared_data/jga/metadata-history/metadata/jga-dataset.xml'
DATASET_DATE_FILE = '/lustre9/open/shared_data/jga/metadata-history/metadata/dataset.date.csv'
STUDY_DATE_FILE = '/lustre9/open/shared_data/jga/metadata-history/metadata/study.date.csv'
ELASTIC_SEARCH_ENDPOINT = 'http://localhost:19200/_bulk'
ELASTIC_PASSWORD: ''
RELATION_DB_PATH = '~/tasks/sra/resources/jga_link.sqlite'

log_file = f"{datetime.date.today()}_bulkinsert_error_log.txt"
logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.DEBUG)
logger = logging.getLogger()


def xml_to_elasticsearch(xml_file: Path, type: str, tag: str):
    context = etree.iterparse(xml_file, tag=tag)
    docs:list[dict] = []
    bulkinsert = BulkInsert(ELASTIC_SEARCH_ENDPOINT)
    for events, element in context:
        if element.tag == tag:
            jga_instance = xml_element_to_jga_instance(etree.tostring(element), type, tag)
            # BaseModelのインスタンスをdictに変換してlistに格納する
            docs.append(jga_instance.dict())
        # TODO: es.helperに投入方法を置き換える
        if len(docs) > DEFAULT_BATCH_SIZE:
            bulkinsert.insert(docs)
            docs = []
    if len(docs) > 0:
        bulkinsert.insert(docs)


def xml_element_to_jga_instance(xml_str, type,  tag) -> JGA:
    metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
    accession = metadata[tag]["accession"]
    dates = get_dates(tag, accession)
    jga_instance = JGA(
        identifier = accession,
        properties = metadata[tag],
        # title = metadata[tag].get("DESCRIPTOR").get("STUDY_TITLE"),
        title = None if metadata[tag].get("DESCRIPTOR") is None else metadata[tag].get("DESCRIPTOR").get("STUDY_TITLE"),
        # description = metadata[tag].get("DESCRIPTOR").get("STUDY_ABSTRACT"),
        description = None if metadata[tag].get("DESCRIPTOR") is None else metadata[tag].get("DESCRIPTOR").get("STUDY_ABSTRACT"),
        name = metadata[tag].get("alias"),
        type = type,
        url = f"https://ddbj.nig.ac.jp/resource/{type}/{accession}",
        sameAs = None,
        isPartOf = "jga",
        organism = parse_oraganism(),
        dbXref = parse_dbxref(accession),
        status = "public",
        visibility = "controlled-access",
        dateCreated = dates[0],
        dateModified = dates[1],
        datePublished = dates[2]
    )
    return jga_instance


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


# bulk insert
class BulkInsert:
    def __init__(self, url):
        self.es_url = url
    def insert(self,docs):
        insert_lst = []
        for doc in docs:
            print(doc, type(doc))
            try:
                insert_lst.append({'index': {'_index': 'genome', '_id': doc['identifier']}})
                insert_lst.append(doc)
            except:
                print("cant open doc: ")
        post_data = "\n".join(json.dumps(d) for d in insert_lst) + "\n"
        auth_str = f"elastic:{ELASTIC_PASSWORD}"
        encoded_auth_str = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
        headers = {"Content-Type": "application/x-ndjson", "Authorization": f"Basic {encoded_auth_str}"}
        req = urllib.request.Request(self.es_url, data=post_data.encode('utf-8'),headers=headers)
        with urllib.request.urlopen(req) as res:
            response_data = json.loads(res.read().decode('utf-8'))
            # print(response_data)
            log_str = json.dumps(response_data)
            word = "exception"
            if word in log_str:
                logger.info(log_str)


def get_dates(tag: str, acc: str):
    """
    accessionをキーとしdatecreated,datepublished,datemodifiedを取得
    return: LIST[{acc: [dateCreated, dateModified, datePublished]}]
    """
    match tag:
        case "DATASET":
            file_path = DATASET_DATE_FILE
        case "STUDY":
            file_path = STUDY_DATE_FILE
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)
            dict_list = {}
            for row in reader:
                if len(row) > 0:
                    key = row[0]
                    value = row[1:] if len(row) > 1 else None
                    # [dateCreated, dateModified, datePublished]がvalueとなる
                    dict_list.update({key: value})
            return dict_list[acc]
    except Exception as e:
        print(f"error occured: {e}")


def parse_oraganism():
    return Organism(
            identifier = "9606",
            name = "Homo sapiens"
        )


def get_relation(accession: str) -> list:
    """
    relation DBよjgaのaccessionのrelation(id0-id1)のリストを取得する
    accessionのリストを返す
    """
    match accession:
        case str() if accession.startswith("JGAD"):
            tables = ["dataset_policy_relation","dataset_dac_relation","dataset_study_relation"]
            return search_relation_table(accession, tables)
        case str() if accession.startswith("JGAC"):
            tables = ["dataset_dac_relation","study_dac_relation","policy_dac_relation"]
            return search_relation_table(accession, tables)
        case str() if accession.startswith("JGAP"):
            tables = ["dataset_policy_relation","study_policy_relation","policy_dac_relation"]
            return search_relation_table(accession, tables)
        case str() if accession.startswith("JGAS"):
            tables = ["dataset_study_relation","study_dac_relation","study_policy_relation"]
            return search_relation_table(accession, tables)


def search_relation_table(accession:str, tables:list) -> list:
    conn = sqlite3.connect(RELATION_DB_PATH)
    cur = conn.cursor()
    results = []
    for t in tables:
        q = f"SELECT id1 AS result FROM {t} WHERE id0 = '{accession}' UNION SELECT id0 FROM {t} WHERE id1 = '{accession}';"
        cur.execute(q)
        # resultsに取得したidのリストを追加する
        results.extend([row[0] for row in cur.fetchall()])
    return results


def parse_dbxref(accession:str):
    """
    get_relation()で関連するaccessionを取得したのち
    urlとtypeを付加したdb_Xrefsの型に整形してxrefsを返す
    """
    xrefs = []
    for acc in get_relation(accession):
        match acc:
            case str() if acc.startswith("JGAD"):
                typ = "jga-dataset"
            case str() if acc.startswith("JGAC"):
                typ = "jga-dac"
            case str() if acc.startswith("JGAP"):
                typ = "jga-policy"
            case str() if acc.startswith("JGAS"):
                typ = "jga-study"
        # xrefs.append(Xref(identifier="", type_ = typ, url = f"https://ddbj.nig.ac.jp/search/resource/{typ}/{acc}"))
        xrefs.append(Xref(identifier="", type = typ, url = f"https://ddbj.nig.ac.jp/search/resource/{typ}/{acc}"))
    return xrefs


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
    create_jga_relation()
    # typeごとに変換しつつbulk insert
    for type in types:
        xml_file = type["file_path"]
        tag = type["tag"]
        type = type["type"]
        xml_to_elasticsearch(xml_file, type, tag)


if __name__ == "__main__":
    main()