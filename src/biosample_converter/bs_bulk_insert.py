import os
import requests
import glob
import json
import argparse
from multiprocessing import Pool
from typing import NewType


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
    with open(file_path, "r") as f:
        post_data = f.read()
        headers = {"Content-Type": "application/x-ndjson"}
        res = requests.post("http://localhost:9200/_bulk", data=post_data, headers=headers)
        if res.status_code == 200 or res.status_code == 201:
            pass
        else:
            logs(f"Error: {res.status_code} - {res.text}")


def logs(message: str):
    dir_name = os.path.dirname(args.output)
    log_file = f"{dir_name}/error_log.txt"
    with open(log_file, "a") as f:
        f.write(message + "\n")


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
    # diffs = get_diff_list(former, later)

    # リストから更新分ファイルを取得しbulk insertする
    '''
    for file_name in diffs:
        # ファイルパスを生成（args.later）
        path = f"{later}/{file_name}"
        with open(path, "r") as f:
            d = f.read()
            bulk_insert(d)
    '''

    if first_time:
        # 指定するディレクトリのファイルリストを取得
        target_pattern = "*.jsonl"
        file_list = glob.glob(os.path.join(later, target_pattern))
        # multiprocessで呼び出す
        p = Pool(20)
        p.map(bulk_insert, file_list)


if __name__ == "__main__":
    main(args.former, args.later)