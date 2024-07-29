import os
import re
import requests
import json
import argparse
from typing import NewType, List
from bioproject_converter.bp_diffs import get_diff_list

parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("former")
parser.add_argument("later")
args = parser.parse_args()
FilePath = NewType('FilePath', str)

def bulk_insert(post_data, path):
    """
    requestsでElasticsearchにndjsonをPOSTする
    POSTするndjsonにはindex行を挿入し改行コードで連結する
    Args:
        docs (List[dict]): 
    """
    file_id = re.split(r"[/.]", path)
    headers = {"Content-Type": "application/x-ndjson"}
    res = requests.post("http://localhost:9200/_bulk", data=post_data, headers=headers)
    if res.status_code == 200 or res.status_code == 201:
            # res.bodyにAPI callのlogを残す
            logs(file_id[-2], res.json())
    else:
        logs(file_id[-2], f"Error: {res.status_code} - {res.text}")


def logs(file_name: FilePath, message: str):
    dir_name = os.path.dirname(args.later)
    log_dir = f"{dir_name}/logs"
    log_file = f"{log_dir}/{file_name}_log.json"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    with open(log_file, "w") as f:
        json.dump(message, f)


def main(former:FilePath, later:FilePath):
    """
    更新されたjsonlリストを生成し、リストのファイルを取得してElasticsearchにjsonlをbulk insertする
    Args:
        post_list (list): _description_
    """
    # 更新分のjsonlのファイル名リストを取得
    diffs = get_diff_list(former, later)
    # リストから更新分ファイルを取得しbulk insertする
    for file_name in diffs:
        # ファイルパスを生成（args.later）
        path = f"{later}/{file_name}"
        with open(path, "r") as f:
            d = f.read()
            bulk_insert(d, path)
    print(len(diffs))


if __name__ == "__main__":
    main(args.former, args.later)

