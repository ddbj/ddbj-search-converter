import argparse
import glob
import json
import os
from multiprocessing import Pool
from typing import List, NewType

import xmltodict
from lxml import etree

from ddbj_search_converter.dblink.get_dblink import get_related_ids

FilePath = NewType('FilePath', str)
batch_size = 10000
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("output")
args = parser.parse_args()
ddbj_biosample_name = "ddbj_biosample"


def convert(input: FilePath):
    # ddbj_coreからの入力のFlag.
    file_name = os.path.basename(input)
    if ddbj_biosample_name in file_name:
        ddbj_biosample = True
        # accessions_data = DdbjCoreData()
    else:
        ddbj_biosample = False

    context = etree.iterparse(input, tag="BioSample")
    # cnt = 0
    # cnt_max = 100000
    i = 0
    docs = []
    for events, element in context:
        if element.tag == "BioSample":
            doc = {}
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["properties"] = metadata["BioSample"]

            # Descriptionの子要素をDDBJ共通objectの値に変換する
            description = doc["properties"].get("Description")
            try:
                doc["title"] = description.get("Title", "")
            except:
                doc["title"] = ""
            try:
                doc["description"] = description.get("Comment").get("Paragraph") if type(description.get(
                    "Comment").get("Paragraph")) is str else description.get("Comment").get("Paragraph")[0]
            except:
                doc["description"] = ""
            try:
                organism_identifier = description.get("Organism").get("taxonomy_id", "")
                organism_name = description.get("Organism").get("taxonomy_name", "")
                doc["organism"] = {"identifier": organism_identifier, "name": organism_name}
            except:
                pass

            doc["accession"] = doc["properties"].get("accession")
            doc["dateCreated"] = doc["properties"].get("submission_date", None)
            doc["dateModified"] = doc["properties"].get("last_update", None)
            doc["datePublished"] = doc["properties"].get("publication_date", None)
            doc["identifier"] = doc["accession"]
            doc["type"] = "biosample"
            doc["isPartOf"] = "BioSample"
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            # dbxreをdblinkモジュールより取得
            doc["dbXrefs"] = get_related_ids(doc["accession"], "biosample")

            # _idの設定
            if ddbj_biosample:
                # ddbj_biosample_set.xmlの場合
                if isinstance(doc["properties"]["Ids"]["Id"], list):
                    # doc["Ids"]["Id"]のnamespace == BioSampleのcontentを取得する
                    bs_id = list(filter(lambda x: x["namespace"] == "BioSample", doc["properties"]["Ids"]["Id"]))
                    doc["accession"] = bs_id[0]["content"]
                else:
                    doc["accession"] = doc["properties"]["Ids"]["Id"]["content"]

                # TODO: ddbj_biosampleのtaxonomy_idの入力を確認する

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

            # Models.Modelにobjectが記述されているケースの処理
            try:
                models_model = doc["properties"]["Models"]["Model"]

                # Models.Modelがオブジェクトの場合そのまま渡す
                if isinstance(models_model, dict):
                    doc["properties"]["Models"]["Model"] = models_model.get("content", None)
                # Models.Modelがリストの場合、要素をそれぞれオブジェクトに変換する
                elif isinstance(models_model, list):
                    doc["properties"]["Models"]["Model"] = [{"content": x} for x in models_model]
                # Models.Modelの値が文字列の場合{"content": value}に変換する
                elif isinstance(models_model, str):
                    doc["properties"]["Models"]["Model"] = [{"content": models_model}]

            except:
                pass

            docs.append(doc)
            i += 1
            # cnt += 1

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
