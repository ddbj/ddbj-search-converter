import os
import requests
import glob
import re
import json
import argparse
from multiprocessing import Pool
from typing import NewType
from bs_diffs import get_diff_list



parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
parser.add_argument("former")
parser.add_argument("later")
parser.add_argument("-f",  action='store_true', help="Insert all records cases with f option")
args = parser.parse_args()
FilePath = NewType('FilePath', str)


def bulk_insert(file_path):
    """
    requestsでElasticsearchにndjsonをPOSTする
    POSTするndjsonにはindex行を挿入し改行コードで連結する
    Args:
        docs (List[dict]): 
    """
    file_id = re.split(r"[/.]", file_path)
    with open(file_path, "r") as f:
        post_data = f.read()
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
    - 更新されたjsonlリストを生成し、リストのファイルを取得してElasticsearchにjsonlをbulk insertする
    - bulk apiは並列で呼び出す    
    Args:
        post_list (list): _description_
    """
    # 初回のinsetのフラグを確認
    first_time = args.f

    # 更新分のjsonlのファイル名リストを取得
    diffs = get_diff_list(former, later)

    # リストから更新分ファイルを取得しbulk insertする
    for file_name in diffs:
        # ファイルパスを生成（args.later）
        path = f"{later}/{file_name}"
        bulk_insert(path)

    if first_time:
        # 指定するディレクトリのファイルリストを取得
        target_pattern = "*.jsonl"
        # for test
        # target_pattern = "bs_1_0.jsonl"
        file_list = glob.glob(os.path.join(later, target_pattern))
        # multiprocessで呼び出す
        p = Pool(32)
        p.map(bulk_insert, file_list)


if __name__ == "__main__":
    main(args.former, args.later)