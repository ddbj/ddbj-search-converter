from lxml import etree
import json
import xmltodict
from typing import NewType, List
from multiprocessing import Pool
import argparse


FilePath = NewType('FilePath', str)
batch_size = 10000
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("output")
args = parser.parse_args()


def convert(input_num:str):
    input_path = args.input + "/split_bs_" + input_num + ".xml"
    print(input_path)
    xml2dict(input_path, input_num)


def xml2dict(input:FilePath, input_num:str):
    context = etree.iterparse(input, tag="BioSample")
    # 開発用のcnt_maxで変換を終える機能
    #cnt = 0
    #cnt_max = 100000
    print("n: ", input_num)
    i = 0
    docs = []
    for events, element in context:
        if element.tag == "BioSample":
            doc = {}

            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["BioSample"] = metadata["BioSample"]
            doc["accession"] = doc["BioSample"].get("accession")
            doc["dateCreated"] = doc["BioSample"].get("submission_date", None)
            doc["dateModified"] = doc["BioSample"].get("last_update", None)
            doc["datePublished"] = doc["BioSample"].get("publication_date", None)
            
            docs.append(doc)
            i += 1
            #cnt += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            dict2esjsonls(docs, input_num)
            docs = []

        #if cnt > cnt_max:
        #    print(cnt)
        #    i = 0
        #    break

    if i > 0:
        dict2esjsonls(docs, input_num)

def dict2esjsonls(docs: List[dict], n):
    """
    dictをjsonlファイルに書き出す
    Args:
        docks (List[dict]): _description_
    """
    output_path = args.output + "/split_bs_" + n + ".jsonl"
    with open(output_path, "a") as f:
        for doc in docs:
            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "biosample", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]

def main():
    base_path = args.input
    p = Pool(5)
    try:
        p.map(convert, ["1","2","3","4","5"])
    except Exception as e:
        print("main: ", e)



if __name__ == "__main__":
    main()