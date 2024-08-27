# coding: UTF-8
from lxml import etree
import os
import sys
import json
import xmltodict
import glob
from datetime import datetime
from typing import NewType, List
from multiprocessing import Pool
import argparse
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from dblink.get_dblink import get_related_ids


FilePath = NewType('FilePath', str)
batch_size = 10000
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("output")
args = parser.parse_args()
ddbj_biosample_name = "ddbj_biosample"


def convert(input:FilePath):
    # ddbj_coreからの入力のFlag.
    file_name = os.path.basename(input)
    if ddbj_biosample_name in file_name:
        ddbj_biosample = True
        # accessions_data = DdbjCoreData()
    else:
        ddbj_biosample = False

    context = etree.iterparse(input, tag="BioSample")
    #cnt = 0
    #cnt_max = 100000
    i = 0
    docs = []
    for events, element in context:
        if element.tag == "BioSample":
            doc = {}
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["properties"] = metadata["BioSample"]

            # _idの設定
            if ddbj_biosample:
                # ddbj_biosample_set.xmlの場合
                if isinstance(doc["properties"]["Ids"]["Id"], list):
                    # doc["Ids"]["Id"]のnamespace == BioSampleのcontentを取得する
                    bs_id = list(filter(lambda x: x["namespace"] == "BioSample", doc["properties"]["Ids"]["Id"]))
                    doc["accession"] = bs_id[0]["content"]
                else:
                    doc["accession"] = doc["properties"]["Ids"]["Id"]["content"]

            else:
                doc["accession"] = doc["properties"].get("accession")

            # Owner.Nameが文字列が記述されているケースの処理
            try:
                owner_name = doc["properties"]["Owner"]["Name"]
                # owner_nameの型がstrであれば {"abbreviation": val, "content": val}に置き換える
                if isinstance(owner_name, str):
                    doc["properties"]["Owner"]["Name"] = {"abbreviation": owner_name, "content": owner_name}
            except:
                pass
            
            
            # 共通項目

            doc["identifier"] = doc["accession"]
            doc["distribution"] = [
                {
                    "contentUrl": "https://ddbj.nig.ac.jp/resource/biosample/" + doc["accession"],
                    "encodingFormat": "JSON",
                    "type": "DataDownload"
                }
            ]
            doc["isPartOf"] = "BioSample"
            doc["type"] =  "biosample"
            # sameAs: Ids.Id.[].db == SRAの値を取得
            try:
                samples =  doc["properties"]["Ids"]["Id"]
                doc["sameAs"] = [{"identifier": x["content"], 
                                  "type": "sra-sample", 
                                  "url": "https://ddbj.nig.ac.jp/resource/sra-sample/" + x["content"]}
                                  for x in samples if x.get("db") == "SRA" or x.get("namespace") == "SRA"]
            except:
                doc["sameAs"] = None
            doc["name"] = None
            doc["url"] = "https://ddbj.nig.ac.jp/search/entry/biosample/" + doc["accession"],
            # Descriptionから共通項目のtitleとdescription, organismを生成する
            description = doc["properties"].get("Description")
            # organism
            try:
                organism_identifier = description.get("Organism").get("taxonomy_id", "")
                organism_name = description.get("Organism").get("taxonomy_name", "")
                organism_obj = {"identifier": organism_identifier, "name": organism_name}
                doc["organism"] = organism_obj
            except:
                doc["organism"] = None

            doc["title"] = description.get("Title", "")
            try:
                paragaraph = description.get("Comment").get("Paragraph") if type(description.get("Comment").get("Paragraph")) is str else description.get("Comment").get("Paragraph")
                if type(paragaraph) is str:
                    doc["description"] = paragaraph
                elif type(paragaraph) is list:
                    doc["description"] = ",".join(paragaraph)
            except:
                doc["description"] = ""
            # attribute
            try:
                attributes = doc["properties"]["Attributes"]["Attribute"]
                doc["attribute"] = [{"attribute_name": x.get("attribute_name", "") , 
                                    "display_name": x.get("display_name", ""), 
                                    "harmonized_name": x.get("harmonized_name", ""), 
                                    "content": x.get("content", "") } 
                                    for x in attributes]
            except:
                doc["attribute"] = []
            # Models.Modelの正規化と共通項目用の整形
            try:
                models_model = doc["properties"]["Models"]["Model"]
                model_obj = []
                # Models.Modelがオブジェクトの場合そのまま渡す
                if isinstance(models_model, dict):
                    model_obj =[{"name": models_model.get("content")}]
                # Models.Modelがリストの場合
                elif isinstance(models_model, list):
                    # 文字列のリストの場合
                    doc["properties"]["Models"]["Model"] = [{"content": x} for x in models_model]
                    model_obj = [{"name": x} for x in models_model]
                # Models.Modelの値が文字列の場合{"content": value}に変換、共通項目の場合は{"name":value}とする
                elif isinstance(models_model, str):
                    doc["properties"]["Models"]["Model"] = [{"content":models_model}]
                    model_obj = [{"name": models_model}]
                doc["model"] = model_obj
            except:
                doc["model"] = []
            # Package
            try: 
                package = doc["properties"]["Package"]
                package_dct = {}
                package_dct["display_name"] = package["display_name"]
                package_dct["name"] = package["content"]
                doc["Package"] = package_dct
            except:
                doc["Package"] = None
            doc["dbXrefs"] = get_related_ids(doc["accession"], "biosample")
            doc["downloadUrl"] = [
                {
                "name": "biosample_set.xml.gz",
                "ftpUrl": "ftp://ftp.ddbj.nig.ac.jp/ddbj_database/biosample/biosample_set.xml.gz",
                "type": "meta",
                "url": "https://ddbj.nig.ac.jp/public/ddbj_database/biosample/biosample_set.xml.gz"
                }
            ]
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            now = datetime.now()
            iso_format_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            doc["dateCreated"] = doc["properties"].get("submission_date", iso_format_now)
            doc["dateModified"] = doc["properties"].get("last_update", iso_format_now)
            doc["datePublished"] = doc["properties"].get("publication_date", iso_format_now)

            docs.append(doc)
            i += 1
            #cnt += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            dict2jsonls(docs, input)
            docs = []

    if i > 0:
        dict2jsonls(docs, input)


def dict2jsonls(docs: List[dict], input: FilePath):
    """
    dictをjsonlファイルに書き出す
    Args:
        docks (List[dict]): _description_
    """
    # output_pathはinputの拡張子をxmlからjsonlに変更した文字列
    output_path = os.path.splitext(input)[0] + ".jsonl"
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
    # cpu_count()次第で分割数は変える
    p = Pool(32)
    try:
        target_dir = args.input
        target_pattern = "*.xml"
        file_list = glob.glob(os.path.join(target_dir, target_pattern))
        p.map(convert, file_list)
    except Exception as e:
        print("main: ", e)


if __name__ == "__main__":
    main()
