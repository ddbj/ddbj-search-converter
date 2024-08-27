from lxml import etree
import xml.etree.cElementTree as cElementTree
import datetime
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
                    "identifier": "JGAS000731",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/resource/jga-dac/JGAS000731"
                }
            ]
            doc["status"] = "public"
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
    return docs


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


def dict2jsonl(docs: List[dict]):
    jsonl_output = "jga-dataset_JGAD000864.jsonl"
    with open(jsonl_output, "a") as f:
        for doc in docs:

            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "jga-dataset", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


if __name__ == "__main__":
    input = "/mnt/data/ddbj/jga-adhoc/JGAD000864.xml"
    docs = xml2json(input)
    dict2jsonl(docs)
