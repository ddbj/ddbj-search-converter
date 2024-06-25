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
batch_size = 10000
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("output")
args = parser.parse_args()


def xml2jsonl(input:FilePath):
    context = etree.iterparse(input, tag="BioSample")
    i = 0
    docs = []
    for events, element in context:
        if element.tag == "BioSample":
            doc = {}
            doc["accession"] = default_id(element)
            doc["dateCreated"] = element.get("submission_date", None)
            doc["dateModified"] = element.get("last_update", None)
            doc["datePublished"] = element.get("publication_date", None)
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["metadata"] = metadata
            
            docs.append(doc)
            i += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            dict2esjsonl(docs)
            docs = []

    if i > 0:
        dict2esjsonl(docs)

def cxml2jsonl(input:FilePath):
    context = cElementTree.iterparse(input, events=('end',))
    i = 0
    docs = []
    for events, element in context:
        if element.tag == "BioSample":
            doc = {}
            doc["accession"] = default_id(element)
            doc["dateCreated"] = element.get("submission_date", None)
            doc["dateModified"] = element.get("last_update", None)
            doc["datePublished"] = element.get("publication_date", None)
            xml_str = cElementTree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["metadata"] = metadata
            
            docs.append(doc)
            i += 1

        clear_celement(element)
        if i > batch_size:
            i = 0
            dict2esjsonl(docs)
            docs = []

    if i > 0:
        dict2esjsonl(docs)



def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]


def clear_celement(element):
    element.clear()


def default_id(elem):
    try:
        uid = elem.attrib["accession"]
    except:
        uid = elem.find(".//BioSample/Ids/Id[@db='BioSample']").text
    return uid


def dict2esjsonl(docs: List[dict]):
    """
    dictをjsonlファイルに書き出す
    Args:
        docks (List[dict]): _description_
    """
    jsonl_output = args.output
    with open(jsonl_output, "a") as f:
        for doc in docs:
            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "biosample", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


def logs(message: str):
    dir_name = os.path.dirname(args.output)
    log_file = f"{dir_name}/error_log.txt"
    with open(log_file, "a") as f:
        f.write(message + "\n")


def main():
    input_file_path = args.input
    cxml2jsonl(input_file_path)


if __name__ == "__main__":
    main()