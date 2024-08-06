import argparse
import datetime
import json
import os
import sys
from pathlib import Path
from typing import Any, List

import requests

from ddbj_search_converter.bioproject.bp_diffs import get_diff_list


def parse_args(args: List[str]):
    parser = argparse.ArgumentParser(description="Bulk insert JSON Lines data to Elasticsearch")
    parser.add_argument(
        "former",
        nargs="?",
        default=None,
        help="Path to the former directory (e.g., /path/to/jsonl/20240801)"
    )
    parser.add_argument(
        "later",
        nargs="?",
        default=None,
        help="Path to the later directory (e.g., /path/to/jsonl/20240802)"
    )

    parsed_args = parser.parse_args(args)

    # 差分更新は$(date -d yesterday +%Y%m%d)、$(date +%Y%m%d)のように当日と前日の日付のディレクトリを与えるが、前日分のディレクトリが（停電等で）作られなかった場合更新が止まるので、当日の次に新しいディレクトリを自動的に取得する機能を実装する。

    return parsed_args


def bulk_insert(post_data: Any, file_id: str) -> None:
    """
    requests で Elasticsearch に ndjson を POST する
    POST する ndjson には index 行を挿入し改行コードで連結する

    Args:
        post_data (Any): 実質的には、List[Dict[str, Any]] のような JSON Lines 形式のデータ
        file_id (str): ファイル名に含まれている file ID
    """
    headers = {"Content-Type": "application/x-ndjson"}
    res = requests.post("http://localhost:9200/_bulk", data=post_data, headers=headers, timeout=10)
    if res.status_code in [200, 201]:
        # res.body に API call の log を残す
        logging_bulk_insert(file_id, res.json())
    else:
        logging_bulk_insert(file_id, f"Error: {res.status_code} - {res.text}")


def logging_bulk_insert(file_id: str, message: str) -> None:
    dir_name = os.path.dirname(args.later)
    today = datetime.date.today()
    formatted_data = today.strftime('%Y%m%d')

    log_dir = Path(f"")

    f"{dir_name}/{formatted_data}/logs"
    log_file = f"{log_dir}/{file_name}_log.json"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(message, f)


def main(former: Path, later: Path):
    """
    更新された jsonl リストを生成し、
    リストのファイルを取得して Elasticsearch に jsonl をbulk insert する

    Args:
        post_list (list): _description_
    """
    # 更新分のjsonlのファイル名リストを取得
    # formerとlaterを同じ名前にした場合ファイル全件のリストが返ってくる（初回時）
    diffs = get_diff_list(former, later)
    # リストから更新分ファイルを取得しbulk insertする
    for file_name in diffs:
        # ファイルパスを生成（args.later）
        path = f"{later}/{file_name}"
        with open(path, "r", encoding="utf-8") as f:
            d = f.read()
            bulk_insert(d, path)
    print(len(diffs))


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
