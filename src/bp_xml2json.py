from lxml import etree
import xmltodict
import json
import sys
from typing import NewType, List

FilePath = NewType('FilePath', str)
#batch_size = 1000
batch_size = 2
bioproject_jsonl = "bioproject_test.jsonl"


# Todo: DDBJのインデックステンプレートにあわわせてdictを整形する
# Todo: 最後のイテレーションがbatch_sizeに満たない場合の処理の実装
# Todo: 処理速度
# Todo: テスト・例外処理・ロギングの実装


def xml2dict(file:FilePath) -> dict:
    """
    BioProject XMLをdictに変換し、
    1000エントリXMLを変換するごとにjsonlに追記して出力する
    """
    context = etree.iterparse(file, tag="Package")
    print(file)

    i = 0
    docs:list[dict] = []
    for events, element in context:
        if element.tag=="Package":
            doc = {}
            accession = element.find(".//Project/Project/ProjectID/ArchiveID").attrib["accession"]
            doc["_id"] = accession
            xml_str = etree.tostring(element)
            # metadata = xml2json(xml_str) 
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["metadata"] = metadata

            # Todo:共通のobjectを追加
            # doc.update(add_common_object(accession))

            # Todo: metadataと共通objectをDDBJ ESのスキーマにあわせる

            docs.append(doc)
            i += 1

        clear_element(element)
        if i > batch_size:
            print(f"{batch_size} docs")
            i = 0
            dict2jsonl(docs)
            docs = []

            break


def add_common_object(accession: str) -> dict:
    """
    BioProjectのmetadataに共通のobjectを追加する
    """
    pass


def dict2jsonl(docs: List[dict]):
    """
    dictをjsonlに変換して出力する
    """
    with open(bioproject_jsonl, "a") as f:
        for doc in docs:
            header = {"index": {"index": "bioproject", "_id": doc["_id"]}}    
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]


if __name__ == "__main__":
    file_path = sys.argv[1]
    print(file_path)
    xml2dict(file_path)