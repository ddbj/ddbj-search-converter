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


def xml2jsonl(input:FilePath):
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
    jsonl_output = "jga-study_20240731.jsonl"
    with open(jsonl_output, "a") as f:
        for doc in docs:

            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "bioproject", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


if __name__ == "__main__":
    input = "jga-study_add_20240731.xml"
    xml2jsonl(input)
