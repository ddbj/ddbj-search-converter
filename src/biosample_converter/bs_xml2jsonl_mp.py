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
import sqlite3
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

            # Owner.Name：文字列が記述されているケースやlistにobjectと文字列が混在するケースがあるので整形する
            try:
                owner_name = doc["properties"]["Owner"]["Name"]
                # owner_nameの型がstrであれば {"abbreviation": val, "content": val}に置き換える
                if isinstance(owner_name, str):
                    doc["properties"]["Owner"]["Name"] = {"abbreviation": owner_name, "content": owner_name}
                elif isinstance(owner_name, list):
                    #doc["properties"]["Owner"]["Name"] = {"content": ",".join(owner_name)}
                    doc["properties"]["Owner"]["Name"] = [x if type(x) is dict else {"content": x} for x in owner_name]
            except:
                pass
            
            # 共通項目
            doc["identifier"] = doc["accession"]
            doc["distribution"] = get_distribution(doc["accession"])
            doc["isPartOf"] = "BioSample"
            doc["type"] =  "biosample"
            doc["sameAs"] = get_sameas(doc)
            doc["name"] = ""
            doc["url"] = "https://ddbj.nig.ac.jp/search/entry/biosample/" + doc["accession"]
            # Descriptionから共通項目のtitleとdescription, organismを生成する
            description = doc["properties"].get("Description")
            doc["organism"] = get_organism(description, ddbj_biosample)
            doc["title"] = description.get("Title", "")
            try:
                paragaraph = description.get("Comment").get("Paragraph") if type(description.get("Comment").get("Paragraph")) is str else description.get("Comment").get("Paragraph")
                if type(paragaraph) is str:
                    doc["description"] = paragaraph
                elif type(paragaraph) is list:
                    doc["description"] = ",".join(paragaraph)
            except:
                doc["description"] = ""
            doc["attributes"] = get_attribute(doc)
            # Models.Modelのproperties内の値の正規化と共通項目用の生成
            # ESのmappingは[{"content":str, "version":str}]
            # 共通項目のスキーマは[{"name": str}]
            try:
                models_model = doc["properties"]["Models"]["Model"]
                model_obj = []
                # Models.Modelがオブジェクトの場合そのまま渡す
                if isinstance(models_model, dict):
                    model_obj =[{"name": models_model.get("content")}]
                # Models.Modelがリストの場合
                elif isinstance(models_model, list):
                    # 文字列のリストの場合
                    # models_model.[].contentがversion属性を含むobjectのケースと文字列のケースがあり両者に対応する
                    doc["properties"]["Models"]["Model"] = [{"content": x} if type(x) is str else {"content":x.get("content"), "version": x.get("version")}for x in models_model]
                    model_obj = [{"name": x.get("content")} for x in models_model]

                    # objectのリスト[{version:"", content:""},,]の場合

                # Models.Modelの値が文字列の場合{"content": value}に変換、共通項目の場合は{"name":value}とする
                elif isinstance(models_model, str):
                    doc["properties"]["Models"]["Model"] = [{"content":models_model}]
                    model_obj = [{"name": models_model}]
                doc["model"] = model_obj
            except:
                doc["model"] = []
            # Package
            # TODO: Package をpackageに変更する
            if ddbj_biosample:
                # DDBJのxmlにはPackage属性が無いためmodel.nameの値をpackageに利用する
                try:
                    package_dct = {}
                    package_dct["display_name"] = doc["model"][0].get("name")
                    package_dct["name"] = doc["model"][0].get("name")
                    doc["package"] = package_dct
                except:
                    doc["package"] = None
            else:
                try: 
                    package = doc["properties"]["Package"]
                    package_dct = {}
                    package_dct["display_name"] = package["display_name"]
                    package_dct["name"] = package["content"]
                    doc["package"] = package_dct
                except:
                    doc["package"] = None
            doc["dbXref"] = get_related_ids(doc["accession"], "biosample")
            # depricated
            # doc["downloadUrl"] = get_downloadUrl()
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            now = datetime.now()
            iso_format_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            # DDBJのXMLのdate情報はsubmission_dateを含まないためdbより取得する
            if ddbj_biosample:
                try:
                    submission_date = get_submission_date(doc["accession"])
                    # DEPRECATED: date.dbを修正したため不要
                    # submission_date = convert_datetime_format(submission_date)
                    doc["dateCreated"] = submission_date if submission_date else iso_format_now
                except:
                    doc["dateCreated"] = iso_format_now
                doc["dateModified"] = doc["properties"].get("last_update", iso_format_now)
                doc["datePublished"] = doc["properties"].get("publication_date", iso_format_now) 
            else:
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

##
# 属性の取得と整形に関する関数
##

# submission_dateの値取得と利用
def get_submission_date(accession: str) -> str:
    """
    ddbjのxmlにはsubmission_date情報が一部欠けているためdbから取得した値を用いる
    Args:
        accession (str): _description_
    """
    table_name = "date_biosample"
    # TODO: DBの情報設定に記述
    db = '/home/w3ddbjld/tasks/sra/resources/date.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    q = f"SELECT date_created from {table_name} WHERE accession='{accession}';"
    cur.execute(q)
    res = cur.fetchone()
    return res[0]


def get_distribution(acc: str):
    return [{"type": "DataDownload",
             "encodingFormat": "JSON",
             "contentUrl": f"https://ddbj.nig.ac.jp/search/entry/biosample/{acc}.json"}]


def get_attribute(d:dict) -> list:
    try:
        attributes = d["properties"]["Attributes"]["Attribute"]
        return [{"attribute_name": x.get("attribute_name", "") , 
                            "display_name": x.get("display_name", ""), 
                            "harmonized_name": x.get("harmonized_name", ""), 
                            "content": x.get("content", "") } 
                            for x in attributes]
    except:
        return []


def get_sameas(d:dict) -> list:
    try:
        """
        Ids.Id.db == "SRA"であればcontentをsameAsのidentifierとする
        """
        # TODO：これは引数
        samples =  d["properties"]["Ids"]["Id"]
        return  [{"identifier": x["content"], 
                            "type": "sra-sample", 
                            "url": "https://ddbj.nig.ac.jp/resource/sra-sample/" + x["content"]}
                            for x in samples if x.get("db") == "SRA" or x.get("namespace") == "SRA"]
    except:
        return []
    

def get_downloadUrl():
    return [
                {
                "name": "biosample_set.xml.gz",
                "ftpUrl": "ftp://ftp.ddbj.nig.ac.jp/ddbj_database/biosample/biosample_set.xml.gz",
                "type": "meta",
                "url": "https://ddbj.nig.ac.jp/public/ddbj_database/biosample/biosample_set.xml.gz"
                }
            ]
    

def get_organism(d:dict, is_ddbj:bool) -> dict:
    try:
        organism_identifier = d.get("Organism").get("taxonomy_id", "")
        if is_ddbj:
            organism_name = d.get("Organism").get("OrganismName", "")
        else:
            organism_name = d.get("Organism").get("taxonomy_name", "")
        organism_obj = {"identifier": str(organism_identifier), "name": organism_name}
        return organism_obj
    except:
        return {}
    

def convert_datetime_format(datetime_str):
    """
    日付文字列を指定されたフォーマットに変換する関数
    """
    try:
        dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f%%+09")
        # タイムゾーン情報をUTCに設定し、マイクロ秒を切り捨てる
        dt = dt.replace(tzinfo=datetime.timezone.utc).replace(microsecond=0)
        # 目的のフォーマットに変換
        return dt.isoformat()
    except:
        return datetime_str


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
