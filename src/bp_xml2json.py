from lxml import etree
import xmltodict
import json
import sys
from typing import NewType, List

FilePath = NewType('FilePath', str)
#batch_size = 1000
batch_size = 3
bioproject_jsonl = "bioproject.jsonl"


# Todo: DDBJのインデックステンプレートにあわわせてdictを整形する
# Todo: 追記形式の保存のため実行時にjsonlの初期化必要
# Todo: 処理速度
# Todo: テスト・例外処理・ロギングの実装


def xml2dict(file:FilePath) -> dict:
    """
    BioProject XMLをdictに変換し、
    1000エントリXMLを変換するごとにjsonlに追記して出力する
    """
    context = etree.iterparse(file, tag="Package", recover=True)

    i = 0
    docs:list[dict] = []
    for events, element in context:
        if element.tag=="Package":
            doc = {}
            accession = element.find(".//Project/Project/ProjectID/ArchiveID").attrib["accession"]
            xml_str = etree.tostring(element)
            # metadata = xml2json(xml_str) 
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            #doc["metadata"] = metadata
            # DDBJのSchemaに合わせて必要部分を抽出
            doc["accession"] = accession
            doc["properties"] = {}
            doc["properties"]["Project"] = metadata["Package"]["Project"]

            # Todo:docに共通のobjectを追加
            doc.update(common_object(accession))
            docs.append(doc)
            i += 1

        clear_element(element)
        if i > batch_size:
            i = 0
            dict2jsonl(docs)
            docs = []

            break
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
        "dateCreated": None,
        "dateModified": None,
        "datePublished": None,
        "dbXrefs": [],
        "description": "",
        "distribution": {"contentUrl":"", "encodingFormat":""},
        "identifier": "",
        "isPartOf": "",
        "name": "",
        "organism": {"identifier": "", "name": ""}
    }
    return d


def dict2jsonl(docs: List[dict]):
    """
    dictをjsonlに変換して出力する
    """
    with open(bioproject_jsonl, "a") as f:
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
    file_path = sys.argv[1]
    xml2dict(file_path)