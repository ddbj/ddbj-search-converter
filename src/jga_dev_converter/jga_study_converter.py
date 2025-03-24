from lxml import etree
import xml.etree.cElementTree as cElementTree
import datetime
import xmltodict
import requests
import argparse
import os
import json
from datetime import datetime
from typing import NewType, List


FilePath = NewType('FilePath', str)

batch_size = 1


def xml2json(input:FilePath):
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
            # jga-policyと、jga-dac,対となるdatasetを記述
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
                    "identifier": "JGAD000847",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/resource/jga-dataset/JGAD000390"
                }

            ]
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            dt_list_str = ["2021-04-07 16:20:35.449036+09","2025-02-17 10:45:40.365462+09","2021-04-08 15:22:24.865+09"]
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
    jsonl_output = "jga-study_JGAS000284.json"
    with open(jsonl_output, "a") as f:
        for doc in docs:
            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "jga-study", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


if __name__ == "__main__":
    input = "/mnt/data/ddbj/jga-adhoc/JGAS000284.xml"
    docs = xml2json(input)
    dict2jsonl(docs)
