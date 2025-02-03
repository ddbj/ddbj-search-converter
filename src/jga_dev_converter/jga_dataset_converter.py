from lxml import etree
import xml.etree.cElementTree as cElementTree
from datetime import datetime
import xmltodict
import requests
import argparse
import os
import json
from typing import NewType, List


FilePath = NewType('FilePath', str)

batch_size = 1


def xml2json(input:FilePath):
    context = etree.iterparse(input, tag="DATASET")
    docs:list[dict] = []
    i = 0
    for events, element in context:
        if element.tag == "DATASET":
            doc = {}
            doc["accession"] = element.attrib["accession"]
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["properties"] = metadata["DATASET"]
            doc["identifier"]= doc["accession"]
            doc["title"] = doc["properties"].get("TITLE")
            doc["description"] = doc["properties"].get("DESCRIPTION")
            doc["name"] = doc["properties"].get("alias")
            doc["type"] = "jga-dataset"
            doc["url"] = "https://ddbj.nig.ac.jp/resource/jga-dataset/" + doc["accession"]
            doc["sameAs"] = None
            doc["isPartOf"] = "jga"
            doc["organism"] = {"identifier": 9606, "name": "Homo sapiens"}
            # TODO: dbxreをdblinkモジュールより取得し追加する
            doc["dbXrefs"] = [
                {
                    "identifier": "JGAP000001",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/resource/jga-policy/JGAP000001"
                },
                {
                    "identifier": "JGAC000001",
                    "type": "jga-dac",
                    "url": "https://ddbj.nig.ac.jp/resource/jga-dac/JGAC000001"
                },
                {
                    "identifier": "JGAS000714",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/resource/jga-study/JGAS000714"
                }
            ]
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            dt_list_str = ["2024-05-30 09:52:53.166475+09","2025-01-30 12:45:53.77827+09","2024-05-30 13:57:36.87+09"]
            doc["dateCreated"] = isoformat_converter(dt_list_str)[0]
            doc["dateModified"] = isoformat_converter(dt_list_str)[2]
            doc["datePublished"] = isoformat_converter(dt_list_str)[1]


            docs.append(doc)
            i +=1

        clear_element(element)
        if i > batch_size:
            dict2jsonl(docs)
            docs = []
    return docs


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


def isoformat_converter(dt_list_str:List[str])-> list:
    """
    Args:
        dt_list_str (List[str]): datecreated, datepublished, datemodified
    Returns:
        list: isoformatに変換したdate情報
    """
    dt = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f+09") for x in dt_list_str]
    dt_iso = [datetime.strftime(x,"%Y-%m-%dT%H:%M:%S+09:00") for x in dt]
    return dt_iso


def dict2jsonl(docs: List[dict]):
    jsonl_output = "jga-dataset_JGAD000847.json"
    with open(jsonl_output, "a") as f:
        for doc in docs:

            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "jga-dataset", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


if __name__ == "__main__":
    input = "/mnt/data/ddbj/jga-adhoc/JGAD000847.xml"
    docs = xml2json(input)
    dict2jsonl(docs)
