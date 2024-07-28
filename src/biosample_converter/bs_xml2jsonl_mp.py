from lxml import etree
import os
import json
import xmltodict
import glob
from typing import NewType, List
from multiprocessing import Pool
import argparse

from dblink.get_dblink import get_dbxref


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
    # 開発用のcnt_maxで変換を終える機能
    #cnt = 0
    #cnt_max = 100000
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

            # Descriptionの子要素をDDBJ共通objectの値に変換する
            try:
                description = doc["BioSample"].get("Description")
                doc["title"] = description.get("Title", None)
                # 
                doc["description"] = description.get("Comment").get("Paragraph") if type(description.get("Comment").get("Paragraph")) is str else description.get("Comment").get("Paragraph")[0]
                organism_identifier = description.get("Organism").get("taxonomy_id", "")
                organism_name = description.get("Organism").get("taxonomy_name", "")
                doc["organism"] = {"identifier": organism_identifier, "name": organism_name}
            except:
                doc["description"] = ""
                doc["title"] = ""
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            # dbxreをdblinkモジュールより取得
            doc["dbXrefs"] = get_dbxref(doc["accession"])

            # _idの設定
            if ddbj_biosample:
                # ddbj_biosample_set.xmlの場合
                if isinstance(doc["BioSample"]["Ids"]["Id"], list):
                    # doc["Ids"]["Id"]のnamespace == BioSampleのcontentを取得する
                    bs_id = list(filter(lambda x: x["namespace"] == "BioSample", doc["BioSample"]["Ids"]["Id"]))
                    doc["accession"] = bs_id[0]["content"]
                else:
                    doc["accession"] = doc["BioSample"]["Ids"]["Id"]["content"]

                # TODO: ddbj_biosampleのtaxonomy_idの入力を確認する

            else:
                doc["accession"] = doc["BioSample"].get("accession")

            # Owner.Nameが文字列が記述されているケースの処理
            try:
                owner_name = doc["BioSample"]["Owner"]["Name"]
                # owner_nameの型がstrであれば {"abbreviation": val, "content": val}に置き換える
                if isinstance(owner_name, str):
                    doc["BioSample"]["Owner"]["Name"] = {"abbreviation": owner_name, "content": owner_name}
            except:
                pass

            # Models.Modelにobjectが記述されているケースの処理
            try:
                models_model = doc["BioSample"]["Models"]["Model"]
                if isinstance(models_model, dict):
                    doc["BioSample"]["Models"]["Model"] = models_model.get("content", None)
            except:
                pass

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
