# coding: UTF-8
from lxml import etree
import xmltodict
import json
import sys
import re
import csv
import os
import sqlite3
from datetime import datetime
import argparse
import requests
from typing import NewType, List, Tuple
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
# TODO
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
# ddbj_bioproject_name = "ddbj_core"
# DDBJ分を交換用のファイルを利用する場合
ddbj_bioproject_name = "ddbj.xml"
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
        # accessions_data = DdbjCoreData()
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



            try:
                published = project["Project"]["ProjectDescr"]["ProjectReleaseDate"]
            except:
                now = datetime.now()
                published = now.strftime("%Y-%m-%dT00:00:00Z")

            # ProjectType.ProjectTypeSubmission.Target.BioSampleSet.ID: 文字列で入力されてしまった場合properties内の値をobjectに整形する
            try:
                biosampleset_ids = project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"]
                if type(biosampleset_ids) is list:
                    project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] = [{"content": x} if type(x) is str else x for x in biosampleset_ids]
                elif type(biosampleset_ids) is str:
                    project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] = {"content": biosampleset_ids}
            except:
                pass

            # properties.Project.Project.ProjectDescr.LocusTagPrefix : 文字列で入力されていた場合properties内の値をobjectに整形する
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

            # properties.Project.Project.ProjectID.LocalID:  文字列で入力されていた場合properties内の値をobjectに整形する
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

            # Organizationを共通項目に出力するとともに、properties内の値をmapping templateに合わせ整形する
            if ddbj_core:
                try:
                    organization = project["Submission"]["Submission"]["Description"]["Organization"]
                    organiztion_obj = []
                    if type(organization) == list:
                        for i, item in enumerate(organization):
                            organization_name = item.get("Name", None)
                            organization_type = item.get("type", "")
                            organization_role = item.get("role", "")
                            organization_url = item.get("url", "")
                            if  type(organization_name) == str:
                                organiztion_obj.append({"name":organization_name, "abbreviation": organization_name,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url })
                    elif type(organization) == dict:
                        organization_name = organization.get("Name")
                        if  type(organization_name) is str:
                            organization_obj = [{"name":organization_name, "abbreviation": organization_name,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url }]
                except:
                    organization_obj = []
            else:
                try:
                    organization = project["Submission"]["Description"]["Organization"]
                    organization_obj = []
                    if type(organization) == list:
                        for i, item in enumerate(organization):
                            organization_name = item.get("Name", None)
                            organization_type = item.get("type", "")
                            organization_role = item.get("role", "")
                            organization_url = item.get("url", "")
                            if  type(organization_name) == str:
                                # properties内部の値もobjectに整形する
                                doc["properties"]["Project"]["Submission"]["Description"]["Organization"][i]["Name"] = {"content":organization_name }
                                organization_obj.append({"name":organization_name, "abbreviation": organization_name,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url })
                            elif type(organization_name) == dict:
                                # Nameがdictの場合はabbr属性がタグについているので、nameとabbrをそれぞれ取得する
                                # propertiesの値はそのままとし共通項目のみ整形する
                                name = organization_name.get("content")
                                abbreviation = organization_name.get("abbr", "")
                                organization_obj.append({"name":name, "abbreviation": abbreviation,
                                                        "role": organization_role, "organizationType": organization_type, "url": organization_url })
                    elif type(organization) == dict:
                        organization_name = organization.get("Name")
                        if  type(organization_name) is str:
                            organization_type = organization.get("type", "")
                            organization_role = organization.get("role", "")
                            organization_url = organization.get("url", "")
                            doc["properties"]["Project"]["Submission"]["Description"]["Organization"]["Name"] = {"content":organization_name }
                            organization_obj = [{"name":organization_name, "abbreviation": organization_name,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url }]
                            # 共通項目用オブジェクトを作り直す
                        elif type(organization_name) == dict:
                            # propertiesの値はそのままとし共通項目のみ整形する
                            name = organization_name.get("content")
                            abbreviation = organization_name.get("abbr", "")
                            organization_type = organization.get("type", "")
                            organization_role = organization.get("role", "")
                            organization_url = organization.get("url", "")
                            organization_obj = [{"name":name, "abbreviation": abbreviation,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url }]
                except:
                    organization_obj = []


            # publicationを共通項目に出力
            # deplicated
            dbtype_patterns = {
                "ePubmed": r"\d+",
                "doi": r".*doi.org.*"
            }

            try:
                publication = project["Project"]["ProjectDescr"]["Publication"]
                publication_obj = []
                # 極稀なbulk insertできないサイズのリストに対応
                if type(publication) == list:
                    # corner case
                    publication = publication[:10000]
                    doc["properties"]["Project"]["Project"]["ProjectDescr"]["Publication"] = publication
                    # 共通項目の設定
                    for item in publication:
                        id = item.get("id", "")
                        dbtype = item.get("DbType")
                        if dbtype in ["ePubmed", "eDOI"]:
                            publication_url = id if dbtype == "eDOI" else f"https://pubmed.ncbi.nlm.nih.gov/{id}/"
                        # IDは記述されているがDbTypeが未入力のケースに対応
                        elif bool(re.match(r"^\d+$", id)):
                            dbtype = "ePubmed"
                            publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id}/"
                        else:
                            publication_url = ""
                        publication_obj.append({
                            "title": item.get("StructuredCitation",{}).get("Title", ""),
                            "date": item.get("date", ""),
                            "id": str(id),
                            "url": publication_url,
                            "status": item.get("status", ""),
                            "Reference": item.get("Reference", ""),
                            "DbType": dbtype
                        })
                elif type(publication) == dict:
                    id = publication.get("id", "")
                    dbtype = item.get("DbType")
                    if dbtype in ["ePubmed", "eDOI"]:
                        publication_url = id if dbtype == "eDOI" else f"https://pubmed.ncbi.nlm.nih.gov/{id}/"
                    elif bool(re.match(r"^\d+$", id)):
                        dbtype = "ePubmed"
                        publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id}/"
                    else:
                        publication_url = ""                
                    publication_obj = [{
                        "title": publication.get("StructuredCitation",{}).get("Title", ""),
                        "date": publication.get("date", ""),
                        "id": str(id),
                        "url": publication_url,
                        "status": publication.get("status", ""),
                        "Reference": publication.get("Reference", ""),
                        "DbType": dbtype
                    }]
            except:
                publication_obj = []

            # TODO: optionで対応する
            # Grantを共通項目に出力
            # properties.Project.Project.ProjectDescr.Grant.Agency: 値が文字列の場合の処理
            try:
                grant = project["Project"]["ProjectDescr"]["Grant"]
                grant_obj = []
                if type(grant) is list:
                    for i, item in enumerate(grant):
                        agency = item.get("Agency")
                        if type(agency) is str:
                            doc["properties"]["Project"]["Project"]["ProjectDescr"]["Grant"][i]["Agency"] = {"abbr": agency, "content":agency }
                            # 
                            grant_obj.append({
                                "id": item.get("GrantId", ""),
                                "title": item.get("Title", ""),
                                "agency": {
                                    "abbreviation": agency,
                                    "name": agency,
                                }
                            })
                        # Agencyにabbr属性がある場合
                        elif type(agency is dict):
                            grant_obj.append({
                                "id": item.get("GrantId", ""),
                                "title": item.get("Title", ""),
                                "agency": {
                                    "abbreviation": agency.get("abbr",""),
                                    "name": agency.get("content", ""),
                                }
                            })
                elif type(grant) is dict:
                    agency = project["Project"]["ProjectDescr"]["Grant"]["Agency"]
                    if  type(agency) is str:
                        doc["properties"]["Project"]["Project"]["ProjectDescr"]["Grant"]["Agency"] = {"abbr": agency, "content":agency }
                        grant_obj = [{
                                    "id": grant.get("GrantId", ""),
                                    "title": grant.get("Title", ""),
                                    "agency": {
                                        "abbreviation": agency,
                                        "name": agency,
                                    }
                                }]
                    elif type(agency) is dict:
                        grant_obj = [{
                                    "id": grant.get("GrantId", ""),
                                    "title": grant.get("Title", ""),
                                    "agency": {
                                        "abbreviation": agency.get("abbr",""),
                                        "name": agency.get("content", ""),
                                    }
                                }]
            except:
                grant_obj = []

            # ExternalLink（schemaはURLとdbXREFを共に許容する・共通項目にはURLを渡す）
            # propertiesの記述に関してはスキーマを合わせないとimportエラーになり可能性があることに留意
            external_link_db = {"GEO": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=",
                                "dbGaP":"https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=",
                                "ENA-SUBMISSION":"https://www.ebi.ac.uk/ena/browser/view/",
                                "SRA": "https://www.ncbi.nlm.nih.gov/sra/",
                                "PUBMED": "https://pubmed.ncbi.nlm.nih.gov/",
                                "DOI": "https://doi.org/",
                                "SRA|http": ""} 
            try:
                # A. ExternalLinkの値がオブジェクトのケース
                external_link_obj = project["Project"]["ProjectDescr"]["ExternalLink"]
                if isinstance(external_link_obj, dict):
                    # PRJNA7,PRJNA8など
                    # 属性にURLを含む場合
                    if external_link_obj.get("URL", None):
                        el_label = external_link_obj.get("label", None)
                        el_url = external_link_obj.get("URL", None)
                        externalLink = [{"label": el_label if el_label else el_url , "url": el_url}]
                    # 属性にdbXREFを含む場合
                    elif external_link_obj.get("dbXREF", None):
                        # external_link_dbをテンプレにしてURLを生成する
                        el_url = external_link_db.get(external_link_obj.get("dbXREF").get("db")) + external_link_obj.get("dbXREF").get("ID")
                        el_label = external_link_obj.get("label") if  external_link_obj.get("label")  else external_link_obj.get("dbXREF").get("ID")
                        externalLink = [{"label": el_label , "url": el_url}]
                # B. ExternalLinkの値がlistのケース（このケースが多いはず）
                elif isinstance(external_link_obj, list):
                    # PRJNA3,PRJNA4,PRJNA5など
                    externalLink = []
                    # TODO: if の階層に留意。一つひとつのオブジェクトごとにurlからdbXREFのケース分けが必要
                    for item in external_link_obj:
                        # 属性にURLを含む場合
                        if item.get("URL"):
                            el_label = item.get("label", None)
                            el_url = item.get("URL", None)
                            externalLink.append({"label": el_label if el_label else el_url , "url": el_url})
                        # 属性にdbXREFを含む場合
                        elif item.get("dbXREF", None):
                            el_url = item.get(external_link_obj.get("dbXREF").get("db")) + external_link_obj.get("ID")
                            el_label = el_label if el_label else item.get("dbXREF").get("ID")
                            externalLink.append({"label": el_label if el_label else el_url , "url": el_url})
            except:
                externalLink = []


            # centerごとの差異があるケース: status, description, title,  ,organization, Nameのxmlからの取得
            if ddbj_core is False:
                # *.ddbj.xml由来では無い場合のレコードの処理
                # organism：propertiesの値を整形したうえ共通項目に書き出す
                try:
                    organism_obj = project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["Organism"]
                    name = organism_obj.get("OrganismName")
                    identifier = organism_obj.get("taxID")
                    organism = {"identifier": str(identifier), "name": name}
                except:
                    # 空の場合nullを返す
                    organism = None

                # ddbj.xmlにはSubmissionの下の属性が無いため以下の処理を行わない
                try:
                    status = project["Submission"]["Description"]["Access"]
                except:
                    status = "public"

                now = datetime.now()
                iso_format_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                # submittedが取得できない場合datetime.now()を渡す, publishedはDBより取得する（Todo: DBを優先とする処理が必要）
                submitted = project["Submission"].get("submitted", iso_format_now)
                last_update = project["Submission"].get("last_update", iso_format_now)

                try:
                    description = project["Project"]["ProjectDescr"].get("Description")
                    title = project["Project"]["ProjectDescr"].get("Title")
                except:
                    description = None
                    title = None

            else:
                # DDBJ_coreの場合の処理
                # accessions.tabよりdateを取得
                # submitted, published, last_update = accessions_data.ddbj_dates(accession)
                status = "public"
                try:
                    organism_obj = project["Project"]["ProjectType"]["ProjectTypeTopAdmin"]["Organism"]
                    name = organism_obj.get("OrganismName")
                    identifier = organism_obj.get("taxID")
                    organism = {"identifier": str(identifier), "name": name}
                except:
                    organism = None

                # 共通項目のTitle, Descriptionを取得する
                try:
                    description = project["Project"]["ProjectDescr"].get("Description", None)
                    title = project["Project"]["ProjectDescr"].get("Title", None)
                except:
                    description = None
                    title = None

            # umbrellaの判定
            # 要検討：例外発生のコストが高いため通常の判定には使わない方が良いという判断でtry&if caseでProjectTypeToopAdminの存在を確認
            try:
                if project["Project"]["ProjectType"].get("ProjectTypeTopAdmin", None):
                    object_type = "UmbrellaBioProject"
                else:
                    object_type = "BioProject"
            except:
                object_type = "BioProject"

            
            # 共通項目
            doc["identifier"]= accession
            doc["distribution"] = [{
                    "type": "DataDownload",
                    "encodingFormat":"JSON",
                    "contentUrl":f"https://ddbj.nig.ac.jp/search/entry/bioproject/{accession}.json"
                }]
            doc["isPartOf"]= "BioProject"
            doc["type"] = "bioproject"
            # umbrellaの判定はobjectTypeに格納する
            doc["objectType"] = object_type
            doc["name"] =  ""
            doc["url"] = "https://ddbj.nig.ac.jp/search/entry/bioproject/" + accession
            doc["organism"] = organism
            doc["title"] = title
            doc["description"] = description
            doc["organization"] = organization_obj
            doc["publication"] = publication_obj
            doc["grant"] = grant_obj
            doc["externalLink"] = externalLink
            doc["dbXref"] = get_related_ids(accession, "bioproject")
            doc["sameAs"] = get_sameas(project)
            # doc["download"] = None
            doc["status"] = status
            doc["visibility"] = "unrestricted-access"
            
            if ddbj_core:
                dates = get_dates(accession)
                doc["dateCreated"] = dates[0]
                doc["dateModified"] = dates[1]
                doc["datePublished"] = dates[2]
            else:
                doc["dateCreated"] = submitted
                doc["dateModified"] = last_update
                doc["datePublished"] = published
            
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
                #doc.pop("accession")
                f.write(json.dumps(header) + "\n")
                json.dump(doc, f)
                f.write("\n")
            except:
                print("Error at: ", doc["accession"])
                raise


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
    depricated: 全てjsonlからbulk insertするので不要
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


def get_dates(accession: str) -> str:
    """
    - ddbjのxmlにはsubmission_date情報が一部欠けているためdate.dbから取得した値を用いる
    Args:
        accession (str): _description_
    """
    table_name = "date_bioproject"
    db = '/home/w3ddbjld/tasks/sra/resources/date.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    q = f"SELECT date_created,date_published,date_modified from {table_name} WHERE accession='{accession}';"
    cur.execute(q)
    res = cur.fetchone()
    try:
        return [res[0], res[2], res[1]]
    except:
        now = datetime.now()
        iso_format_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        return [iso_format_now] * 3

def get_sameas(prj:str)->dict:
    """
    ProjectIDにGEOが含まれるケースでsameAsの値にgeoを返す
    """
    try:
        if prj["Project"]["ProjectID"]["CenterID"]["center"] == "GEO":
            id = prj["Project"]["ProjectID"]["CenterID"]["content"]
            sameas_dct = [{
                "identifier" : id,
                "type": "GEO",
                "url":  f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={id}"
            }] 
            return sameas_dct
        else:
            return []
    except:
        return []


class DdbjCoreData():
    def __init__(self):
        """
        - deplicated(get_datesを利用する)
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
                    print("520: ", row)
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
