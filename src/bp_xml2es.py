from lxml import etree
import xmltodict
import json
import sys
import re
from datetime import datetime
import argparse
from typing import NewType, List

from bp_xref import get_relation

FilePath = NewType('FilePath', str)
batch_size = 200
jsonl_output = "bioproject.jsonl"
sra_accessions_path = None
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("sra_accessions")

# 利用しないため廃止を検討
parser.add_argument("center", nargs="?", default=None)


args = parser.parse_args()

# Todo: テスト・例外処理・ロギングの実装

def xml2jsonl(file:FilePath, center=None) -> dict:
    """
    BioProject XMLをdictに変換・関係データを追加し
    batch_sizeごとにlocalhostのESにbulkインポートする
    """
    context = etree.iterparse(file, tag="Package", recover=True)

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
            # centerにDDBJが指定されarchiveがDDBJでない場合はスキップ
            if center and archive.lower() != center.lower():
                clear_element(element)
                continue
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
                description = project["Project"]["ProjectDescr"]["ProjectDescr"]["Description"]
                title = project["ProjectDescr"]["ProjectDescr"]["Title"]
            except:
                description = None
                title = None

            now = datetime.now()
            # submittedが取得できない場合datetime.now()を渡す
            submitted = project["Submission"].get("submitted", now.strftime("%Y-%m-%dT00:00:00Z"))
            last_update = project["Submission"].get("last_update", None)

            try:
                published = project["Project"]["ProjectDescr"]["ProjectReleaseDate"]
            except:
                published = now.strftime("%Y-%m-%dT00:00:00Z")

            try:
                status = project["Submission"]["Description"]["Access"]
            except:
                status = "public"

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
                
            doc["organism"] = organism
            doc["description"] = description
            doc["title"] = title
            doc["dateCreated"] = submitted
            doc["dateModified"] = last_update
            doc["datePublished"] = published
            doc["status"] = status
            doc["visibility"] = "unrestricted-access"

            doc.update(common_object(accession,))
            doc.update(dbxref(accession))
            docs.append(doc)
            i += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            dict2es(docs)
            docs = []

    if i > 0:
        # 処理の終了時にbatch_sizeに満たない場合、未処理のデータを書き出す
        dict2es(docs)


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


def dbxref(accession: str) -> dict:
    """
    BioProjectに紐づくSRAを取得し、dbXrefsとdbXrefsStatisticsを生成して返す
    """
    dbXrefs = []
    dbXrefsStatistics = []
    xref = get_relation(sra_accessions_path, accession)
    for key, values in xref.items():
        # keyはobjectのtype、valuesはobjectのidentifierの集合
        type = "biosample" if key == "biosample" else f"sra-{key}"
        for v in values:
            dbXrefs.append({"identifier": v, "type": type , "url": f"https://identifiers.org/sra-{key}/{v}"})
        # typeごとにcountを出力
        dbXrefsStatistics.append({"type": type, "count": len(values)})
    dct = {"dbXrefs": dbXrefs, "dbXrefsStatistics": dbXrefsStatistics}
    return dct


def dict2jsonl(docs: List[dict]):
    """
    dictをjsonlに変換して出力する
    """
    with open(jsonl_output, "a") as f:
        for doc in docs:
            header = {"index": {"_index": "bioproject", "_id": doc["accession"]}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


def dict2es():
    pass


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")

def test_bioproject(file:FilePath, center=None) -> dict:
    """
    BioProject XMLをdictに変換し、
    1000エントリXMLを変換するごとにjsonlに追記して出力する
    """
    context = etree.iterparse(file, tag="Package", recover=True)

    i = 0
    docs:list[dict] = []
    for events, element in context:
        if element.tag=="Package":
            i += 1
        clear_element(element)    
    print(i)    

if __name__ == "__main__":
    file_path = args.input
    sra_accessions_path = args.sra_accessions
    center = args.center
    xml2jsonl(file_path, center)
    #test_bioproject(file_path)
