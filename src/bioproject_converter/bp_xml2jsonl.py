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

            # organismをパースし共通項目に埋める
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

            # Organizationを共通項目に出力するとともに、properties内部の値もスキーマに合わせる
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
                    organiztion_obj = []
                    if type(organization) == list:
                        for i, item in enumerate(organization):
                            organization_name = item.get("Name", None)
                            organization_type = item.get("type", "")
                            organization_role = item.get("role", "")
                            organization_url = item.get("url", "")
                            if  type(organization_name) == str:
                                # properties内部の値も文字列のままではESのスキーマに合わないため修正する
                                doc["properties"]["Project"]["Submission"]["Description"]["Organization"][i]["Name"] = {"content":organization_name }
                                organiztion_obj.append({"name":organization_name, "abbreviation": organization_name,
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
                            doc["properties"]["Project"]["Submission"]["Description"]["Organization"]["Name"] = {"content":organization_name }
                            organization_obj = [{"name":organization_name, "abbreviation": organization_name,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url }]
                            # 共通項目用オブジェクトを作り直す
                        elif type(organization_name) == dict:
                            # propertiesの値はそのままとし共通項目のみ整形する
                            name = organization_name.get("content")
                            abbreviation = organization_name.get("abbr", "")
                            organization_type = item.get("type", "")
                            organization_role = item.get("role", "")
                            organization_url = item.get("url", "")
                            organization_obj = [{"name":organization_name, "abbreviation": organization_name,
                                                    "role": organization_role, "organizationType": organization_type, "url": organization_url }]
                except:
                    organization_obj = []

            # publicationを共通項目に出力
            # TODO: optionで対応する
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
                    publication = publication[:100000]
                    doc["properties"]["Project"]["Project"]["ProjectDescr"]["Publication"] = publication
                    # 共通項目の設定
                    for item in publication:
                        id = item.get("id", "")
                        for dbtype, patterns in dbtype_patterns.items():
                            if re.match(patterns, id):
                                DbType = dbtype
                            else:
                                pass
                        publication_obj.append({
                            "date": item.get("date", ""),
                            "id": id,
                            "status": item.get("status", ""),
                            "Reference": item.get("Reference", ""),
                            "DbType": DbType
                        })
                elif type(publication) == dict:
                    id = publication.get("id", "")
                    for dbtype, patterns in dbtype_patterns.items():
                        if re.match(patterns, id):
                            DbType = dbtype
                        else:
                            pass
                    publication_obj = [{
                        "date": publication.get("date", ""),
                        "id": id,
                        "status": publication.get("status", ""),
                        "Reference": publication.get("Reference", ""),
                        "DbType": DbType
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
                    # 属性にURLを含む場合
                    if external_link_obj.get("URL", None):
                        el_label = external_link_obj.get("label", None)
                        el_url = external_link_obj.get("URL", None)
                        externalLink = [{"label": el_label if el_label else el_url , "URL": el_url}]
                    # 属性にdbXREFを含む場合
                    elif external_link_obj.get("dbXREF", None):
                        el_url = external_link_db.get(external_link_obj.get("dbXREF").get("db")) + external_link_obj.get("ID")
                        el_label = el_label if el_label else external_link_obj.get("dbXREF").get("ID")
                        externalLink = [{"label": el_label if el_label else el_url , "URL": el_url}]
                # B. ExternalLinkの値がlistのケース
                elif isinstance(external_link_obj, list):
                    externalLink = []
                    # TODO: if の階層に留意。一つひとつのオブジェクトごとにurlからdbXREFのケース分けが必要
                    for item in external_link_obj:
                        # 属性にURLを含む場合
                        if external_link_obj.get("URL", None):
                            el_label = external_link_obj.get("label", None)
                            el_url = external_link_obj.get("URL", None)
                            externalLink.append({"label": el_label if el_label else el_url , "URL": el_url})
                        # 属性にdbXREFを含む場合
                        elif external_link_obj.get("dbXREF", None):
                            el_url = external_link_db.get(external_link_obj.get("dbXREF").get("db")) + external_link_obj.get("ID")
                            el_label = el_label if el_label else external_link_obj.get("dbXREF").get("ID")
                            externalLink.append({"label": el_label if el_label else el_url , "URL": el_url})
            except:
                externalLink = []


            # centerごとの差異があるケース: status, description, title,  ,organization, Nameのxmlからの取得
            if ddbj_core is False:
                # ddbj_coreのフラグがない場合の処理を記述
                # ddbj_coreeにはSubmissionの下の属性が無いため以下の処理を行わない
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
                # DDBJ_coreの場合
                # accessions.tabよりdateを取得
                # submitted, published, last_update = accessions_data.ddbj_dates(accession)
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

            
            # 共通項目

            doc["identifier"]= accession
            doc["distribution"] = {"contentUrl":f"https://ddbj.nig.ac.jp/resource/bioproject/{accession}", "encodingFormat":"JSON"}
            doc["isPartOf"]= "BioProject"
            doc["type"] = "bioproject"
            doc["objectType"] = "BioProject"
            doc["name"] =  None
            doc["url"] = "https://ddbj.nig.ac.jp/search/entry/bioproject/" + accession
            doc["organism"] = organism
            doc["title"] = title
            doc["description"] = description
            doc["organization"] = organization_obj
            doc["publication"] = publication_obj
            doc["grant"] = grant_obj
            doc["externalLink"] = externalLink
            doc["dbXrefs"] = get_related_ids(accession, "bioproject")
            doc["download"] = None
            doc["status"] = status
            doc["visibility"] = "unrestricted-access"

            if ddbj_core:
                dates = get_dates(accession)
                print("dates: ", dates)
                now = datetime.now()
                iso_format_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                try:
                    doc["dateCreated"] = dates[0]
                    doc["dateModified"] = dates[2]
                    doc["datePublished"] = dates[1]
                except:
                    doc["dateCreated"] = iso_format_now
                    doc["dateModified"] = iso_format_now
                    doc["datePublished"] = iso_format_now

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


def get_dates(accession: str) -> str:
    """
    ddbjのxmlにはsubmission_date情報が一部欠けているためdbから取得した値を用いる
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
    return res


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
