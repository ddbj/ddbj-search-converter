from lxml import etree
import xmltodict
import json
import sys
import re
import csv
import os
from datetime import datetime
import argparse
import requests
from typing import NewType, List, Tuple
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from dblink.get_dblink import get_related_ids

FilePath = NewType('FilePath', str)
batch_size = 200

sra_accessions_path = None
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
# ddbj_core_bioproject.xmlの場合はoutputを指定する必要がない
# 正しこの仕様については要検討
parser.add_argument("output")
parser.add_argument("--acc", help="ddbj accessions.tabのファイルパスを入力")
parser.add_argument("-f",  action='store_true', help="Insert all records cases with f option")
# parser.add_argument("sra_accessions") ## 廃止予定
# parser.add_argument("center", nargs="?", default=None) ## 廃止
ddbj_bioproject_name = "ddbj_core"
args = parser.parse_args()


def xml2jsonl(input_file:FilePath) -> dict:
    """
    BioProject XMLをdictに変換・関係データを追加し
    batch_sizeごとにlocalhostのESにbulkインポートする
    """
    # ファイル名が"*ddbj_core*のケースに処理を分岐するフラグを立てる
    # inputに"ddbj_core"が含まれる場合フラグがたつ
    file_name = os.path.basename(input_file)
    is_full_register = args.f

    # ddbj_coreからの入力のFlag.
    if ddbj_bioproject_name in file_name:
        ddbj_core = True
        accessions_data = DdbjCoreData()
    else:
        ddbj_core = False

    context = etree.iterparse(input_file, tag="Package", recover=True)
    i = 0
    docs:list[dict] = []
    for events, element in context:
        if element.tag=="Package":
            """
            Packagetag単位でxmlを変換する
            centerが指定されている場合は指定されたcenterのデータのみ変換する（ddbjのみ対応）
            """
            doc = {}
            accession = element.find(".//Project/Project/ProjectID/ArchiveID").attrib["accession"]
            archive = element.find(".//Project/Project/ProjectID/ArchiveID").attrib["archive"]
            xml_str = etree.tostring(element)
            # metadata = xml2json(xml_str) 
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")

            # DDBJのSchemaに合わせて必要部分を抽出
            doc["accession"] = accession
            doc["properties"] = {}
            project = metadata["Package"]["Project"]
            doc["properties"]["Project"] = project

            # 共通項目のobjectを生成し追加する
            try:
                organism_obj = project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["Organism"]
                name = organism_obj.get("OrganismName")
                identifier = organism_obj.get("taxID")
                organism = {"identifier": identifier, "name": name}
            except:
                organism = None



            try:
                published = project["Project"]["ProjectDescr"]["ProjectReleaseDate"]
            except:
                now = datetime.now()
                published = now.strftime("%Y-%m-%dT00:00:00Z")

            # properties.Project.Project.ProjectDescr.Grant.Agency: 値が文字列の場合の処理
            try:
                grant = project["Project"]["ProjectDescr"]["Grant"]
                if type(grant) is list:
                    for i, item in enumerate(grant):
                        agency = item.get("Agency")
                        if type(agency) is str:
                            doc["properties"]["Project"]["Project"]["ProjectDescr"]["Grant"][i]["Agency"] = {"abbr": agency, "content":agency }
                elif type(grant) is dict:
                    agency = project["Project"]["ProjectDescr"]["Grant"]["Agency"]
                    if  type(agency) == str:
                        doc["properties"]["Project"]["Project"]["ProjectDescr"]["Grant"]["Agency"] = {"abbr": agency, "content":agency }
            except:
                pass

            # properties.Project.Project.ProjectDescr.LocusTagPrefix : 値が文字列の場合の処理
            try:
                prefix = project["Project"]["ProjectDescr"]["LocusTagPrefix"]
                if type(prefix) == list:
                    for i, item in enumerate(prefix):
                        if type(item) == str:
                            doc["properties"]["Project"]["Project"]["ProjectDescr"]["LocusTagPrefix"][i] = {"content":item }
                elif type(prefix) == str:
                    doc["properties"]["Project"]["Project"]["ProjectDescr"]["LocusTagPrefix"] = {"content":prefix }
            except:
                pass

            # properties.Project.Project.ProjectID.LocalID: 値が文字列のケースの処理
            try:
                localid = project["Project"]["ProjectID"]["LocalID"]
                if type(localid) == list:
                    for i, item in enumerate(localid):
                        if type(item) == str:
                            doc["properties"]["Project"]["Project"]["ProjectID"]["LocalID"][i] = {"content": item}
                elif type(localid) == str:
                    doc["properties"]["Project"]["Project"]["ProjectID"]["LocalID"] = {"content": localid}
            except:
                pass

            # 巨大なPublicationを制限する（極稀なケースで存在し、bulk apiに対応しない）
            try:
                publication = project["Project"]["ProjectDescr"]["Publication"]
                if type(publication) == list and len(publication) > 256:
                    doc["properties"]["Project"]["Project"]["ProjectDescr"] = publication[:256]
            except:
                pass


            if ddbj_core is False:
                # ddbj_coreのフラグがない場合の処理を記述
                # ddbj_coreeにはSubmissionの下の属性が無いため以下の処理を行わない
                try:
                    status = project["Submission"]["Description"]["Access"]
                except:
                    status = "public"

                now = datetime.now()
                # submittedが取得できない場合datetime.now()を渡す
                submitted = project["Submission"].get("submitted", None)
                last_update = project["Submission"].get("last_update", None)

                try:
                    description = project["Project"]["ProjectDescr"].get("Description")
                    title = project["Project"]["ProjectDescr"].get("Title")
                except:
                    description = None
                    title = None

                # Organization.Nameの型をobjectに統一する
                # Todo:処理速度を上げるため内包表記にする
                try:
                    organization = project["Submission"]["Description"]["Organization"]
                    if type(organization) == list:
                        for i, item in enumerate(organization):
                            organization_name = item.get("Name")
                            if  type(organization_name) == str:
                                doc["properties"]["Project"]["Submission"]["Description"]["Organization"][i]["Name"] = {"content":organization_name }
                    elif type(organization) == dict:
                        organization_name = organization.get("Name")
                        if  type(organization_name) is str:
                            doc["properties"]["Project"]["Submission"]["Description"]["Organization"]["Name"] = {"content":organization_name }
                except:
                    # 入力されたスキーマが正しくないケースがあるためその場合空のオブジェクトを渡す？
                    pass
            else:
                # DDBJ_coreの場合
                # accessions.tabよりdateを取得
                submitted, published, last_update = accessions_data.ddbj_dates(accession)
                status = "public"
                try:
                    organism_obj = project["Project"]["ProjectType"]["ProjectTypeTopAdmin"]["Organism"]
                    name = organism_obj.get("OrganismName")
                    identifier = organism_obj.get("taxID")
                    organism = {"identifier": identifier, "name": name}
                except:
                    pass

                # 共通項目のTitle, Descriptionを取得する
                try:
                    description = project["Project"]["ProjectDescr"].get("Description", None)
                    title = project["Project"]["ProjectDescr"].get("Title", None)
                except:
                    description = None
                    title = None
            
            doc["organism"] = organism
            doc["description"] = description
            doc["title"] = title
            doc["dateCreated"] = submitted
            doc["dateModified"] = last_update
            doc["datePublished"] = published
            doc["status"] = status
            doc["visibility"] = "unrestricted-access"
            # dbxreをdblinkモジュールより取得
            doc["dbXrefs"] = get_related_ids(accession, "bioproject")

            doc.update(common_object(accession,))
            docs.append(doc)
            i += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            if is_full_register:
                dict2esjsonl(docs)
            else:
                dict2jsonl(docs)
            docs = []

    if i > 0:
        # 処理の終了時にbatch_sizeに満たない場合、未処理のデータを書き出す
        if is_full_register:
            dict2esjsonl(docs)
        else:
            dict2jsonl(docs)


def common_object(accession: str) -> dict:
    """
    BioProjectのmetadataに共通のobjectを生成する
    Todo: nullの場合の処理（index mappingでオブジェクトが指定されている場合からのobjectを与える必要がありそうだが正しいか検証）
    Todo: 共通のobjectの生成方法を検討・実装
    """
    d = {
        "distribution": {"contentUrl":"", "encodingFormat":""},
        "identifier": accession,
        "isPartOf": "bioproject",
        "type": "bioProject",
        "name": None,
        "url": None
    }
    return d


def dict2es(docs: List[dict]):
    """
    requestsでElasticsearchにndjsonをPOSTする
    POSTするndjsonにはindex行を挿入し改行コードで連結する
    Args:
        docs (List[dict]): 
    """
    post_lst = []
    for doc in docs:
        post_lst.append({"index": {"_index": "bioproject", "_id": doc["accession"]}})
        doc.pop("accession")
        post_lst.append(doc)
    post_data = "\n".join(json.dumps(d) for d in post_lst) + "\n"
    headers = {"Content-Type": "application/x-ndjson"}
    res = requests.post("http://localhost:9200/_bulk", data=post_data, headers=headers)

    if res.status_code == 200 or res.status_code == 201:
        pass
    else:
        # エラーメッセージ関係はprintしない。error_logs.txt等に残す
        logs(f"Error: {res.status_code} - {res.text}")

    # POSTするjsonlをprint()しpyの結果をファイルにリダイレクションするとするとjsonlも残すことができる
    # print(post_data)


def dict2jsonl(docs: List[dict]):
    """
    差分更新用にdictをjsonlに変換して出力する

    """
    jsonl_output = args.output
    with open(jsonl_output, "a") as f:
        for doc in docs:
            try:
                # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
                header = {"index": {"_index": "bioproject", "_id": doc["accession"]}}
                doc.pop("accession")
                f.write(json.dumps(header) + "\n")
                json.dump(doc, f)
                f.write("\n")
            except:
                print(doc)


def dict2esjsonl(docs: List[dict]):
    """
    dictをesにbulk insertするのと同時に同じdictをjsonlファイルにに書き出す
    Args:
        docks (List[dict]): _description_
    """
    jsonl_output = args.output
    post_lst = []
    for doc in docs:
        # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
        header = {"index": {"_index": "bioproject", "_id": doc["accession"]}}
        post_lst.append(header)
        doc.pop("accession")
        post_lst.append(doc)
        # ファイル出力q
        with open(jsonl_output, "a") as f:
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")
    post_data = "\n".join(json.dumps(d) for d in post_lst) + "\n"
    headers = {"Content-Type": "application/x-ndjson"}
    res = requests.post("http://localhost:9200/_bulk", data=post_data, headers=headers)
    if res.status_code == 200 or res.status_code == 201:
        pass
    else:
        logs(f"Error: {res.status_code} - {res.text}")


def dict2es(docs: List[dict]):
    """
    documentsをesにbulk insertする
    Args:
        docks (List[dict]): 
    """
    post_lst = []
    for doc in docs:
        # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
        header = {"index": {"_index": "bioproject", "_id": doc["accession"]}}
        post_lst.append(header)
        doc.pop("accession")
        post_lst.append(doc)
    post_data = "\n".join(json.dumps(d) for d in post_lst) + "\n"
    headers = {"Content-Type": "application/x-ndjson"}
    res = requests.post("http://localhost:9200/_bulk", data=post_data, headers=headers)
    if res.status_code == 200 or res.status_code == 201:
        pass
    else:
        logs(f"Error: {res.status_code} - {res.text}")


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


def logs(message: str):
    dir_name = os.path.dirname(args.output)
    log_file = f"{dir_name}/error_log.txt"
    with open(log_file, "a") as f:
        f.write(message + "\n")


def rm_old_file(file_path:FilePath):
    """
    bp_xml2jsonlは追記形式であるため出力ファイルと同じファイル名のファイルがある場合
    同一ファイルに新しい作業日のレコードが追加されることとなるため
    処理開始前に既存のファイルの存在を確認し削除する
    Args:
        file_path (_type_): 出力ファイルのパス
    """
    # ファイルの存在を確認する
    if os.path.exists(file_path):
        os.remove(file_path)


class DdbjCoreData():
    def __init__(self):
        """
        - deplicated
        - 別モジュールのdblink.get_dblinkで関係データは取得する
        accessions.tabを辞書化する
        """
        # accessions辞書化
        d = {}
        with open(args.acc, "r") as input_f:
            reader = csv.reader(input_f, delimiter="\t")
            for row in reader:
                try:
                    d[row[0]] = (row[1], row[2], row[3])
                except:
                    print(row)
            self.acc_dict = d

    def ddbj_dates(self, accession) -> Tuple[str]:
        """
        ddbjのaccessions.tabよりaccessionに対応する
        date_created, date_published, date_modifiedフィールドの値を変える
        """
        # 辞書よりタプルを返す
        return self.acc_dict.get(accession, (None,None,None))


if __name__ == "__main__":
    file_path = args.input
    rm_old_file(args.output)
    xml2jsonl(file_path)
