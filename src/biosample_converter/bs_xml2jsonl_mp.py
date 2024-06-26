from lxml import etree
import json
import xmltodict
from typing import NewType, List
from multiprocessing import Pool
import argparse


FilePath = NewType('FilePath', str)
batch_size = 10000
parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("input")
parser.add_argument("output")
args = parser.parse_args()


def convert(input_num:str):
    #input_path = args.input + "/split_bs_" + input_num + ".xml"
    # 一時ファイルの置き方によってパスが変わる。出力場所について要検討
    input_path = args.input + "/bs_" + input_num + "_modified.xml"
    print(input_path)
    xml2dict(input_path, input_num)


def xml2dict(input:FilePath, input_num:str):
    context = etree.iterparse(input, tag="BioSample")
    # 開発用のcnt_maxで変換を終える機能
    #cnt = 0
    #cnt_max = 100000
    print("n: ", input_num)
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
            dict2jsonls(docs, input_num)
            docs = []

        #if cnt > cnt_max:
        #    print(cnt)
        #    i = 0
        #    break

    if i > 0:
        dict2jsonls(docs, input_num)

def dict2jsonls(docs: List[dict], n):
    """
    dictをjsonlファイルに書き出す
    Args:
        docks (List[dict]): _description_
    """
    # output_path = args.output + "/split_bs_" + n + ".jsonl"
    # 一時ファイルの置き方によってoutput_pathが変わる。置き方要検討
    output_path = args.output + "/bs_" + n + ".jsonl"
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
    p = Pool(40)
    try:
        # Todo: filesはディレクトリの全てのxmlファイルリストを渡す
        files = [str(x) for x in list(range(1, 60))]
        p.map(convert, files)
    except Exception as e:
        print("main: ", e)



if __name__ == "__main__":
    main()