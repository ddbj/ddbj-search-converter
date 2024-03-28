from lxml import etree
import xmltodict
import json
import sys
from datetime import datetime
import argparse
from typing import NewType, List

from bp_xref import get_xref

FilePath = NewType('FilePath', str)
#batch_size = 1000
batch_size = 100
jsonl_output = "bioproject.jsonl"
sra_accessions_path = "./sample/SRA_Accessions_1000.tab"

parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("center", nargs="*", default=None)
args = parser.parse_args()

# Todo: status, visibilityを追加
# Todo: experiment->runの関係をdbxrefに追加
# Todo: 処理速度検討
# Todo: テスト・例外処理・ロギングの実装



def xml2jsonl(file:FilePath, center=None) -> dict:
    """
    BioProject XMLをdictに変換し、
    1000エントリXMLを変換するごとにjsonlに追記して出力する
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
            #doc["metadata"] = metadata
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
            datetime_now = now.strftime("%Y-%m-%dT00:00:00Z")
            try:
                submitted = project["Submission"]["submitted"]
                last_update = project["Submission"]["last_update"]
            except:
                submitted = datetime_now
                last_update = datetime_now

            try:
                published = project["Project"]["ProjectDescr"]["ProjectReleaseDate"]
            except:
                published = datetime_now
                
            doc["organism"] = organism
            doc["description"] = description
            doc["title"] = title
            doc["dateCreated"] = submitted
            doc["dateModified"] = last_update
            doc["datePublished"] = published
            doc.update(common_object(accession,))
            doc.update(dbxref(accession))
            docs.append(doc)
            i += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            dict2jsonl(docs)
            docs = []

    if i > 0:
        # データの最後batch_sizeに満たない場合の処理
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


def dbxref(accession: str) -> dict:
    """
    BioProjectに紐づくSRAを取得し、dbXrefsとdbXrefsStatisticsを生成して返す
    """
    dbXrefs = []
    dbXrefsStatistics = []
    status = None
    visibility = None
    xref = get_xref(sra_accessions_path, accession)
    for key, values in xref.items():
        if key == "Status":
            status = values.pop()
        elif key == "Visibility":
            visibility = values.pop()
        else:
            # keyはobjectのtype、valuesはobjectのidentifierの集合
            type = "biosample" if key == "biosample" else f"sra-{key}"
            for v in values:
                dbXrefs.append({"identifier": v, "type": type , "url": f"https://identifiers.org/sra-{key}/{v}"})
            # typeごとにcountを出力
            dbXrefsStatistics.append({"type": type, "count": len(values)})
    dct = {"dbXrefs": dbXrefs, "dbXrefsStatistics": dbXrefsStatistics, "status": str(status), "visibility": str(visibility)}
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


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


if __name__ == "__main__":
    file_path = args.input
    center = args.center[0]
    xml2jsonl(file_path, center)