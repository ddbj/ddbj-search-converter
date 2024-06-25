import sys
import os
import re
import hashlib
import argparse
from typing import NewType

FilePath = NewType('FilePath', str)

def get_diff_list(former:FilePath, later:FilePath) -> list:
    """
    二つのディレクトリにある同一の名前のファイルを比較し
    MD5に差があるファイルのリストを返す
    Args:
        former (FilePath): 分割したJSONLの置かれたディレクトリ
        later (FilePath): 分割されたJSONLの置かれたディレクトリ

    Returns:
        list: MD5が一致しないファイルのファイル名リスト
    """
    former_info = get_file_info(former)
    later_info = get_file_info(later)
    unmated_info = get_unmatced_list(former_info, later_info)
    return [x["filename"] for x in unmated_info]


def get_file_info(directory:FilePath) -> list:
    """
    指定されたディレクトリのmd5 hasを含むファイル情報リストを返す
    Args:
        directory (_type_): _description_

    Returns:
        list: _description_
    """
    file_info_lst = []
    for root, _, files in os.walk(directory):
        for filename in files:
            try:
                filepath = os.path.join(root, filename)
                with open(filepath, 'rb') as f:
                    data = f.read()
                md5_hash = hashlib.md5(data).hexdigest()
                file_info = {
                    'filename': filename,
                    'filepath': filepath,
                    'size': os.path.getsize(filepath),
                    'md5_hash': md5_hash,
                }
                file_info_lst.append(file_info)
            except:
                print("error?: ", filename)

    return file_info_lst


def get_unmatced_list(formar:list, later:list)->list:
    """
    二つのリストでfilenameが同じ情報を比較し一致しないファイルの情報を返す
    Args:
        formar (list): _description_
        later (list): _description_

    Returns:
        list: _description_
    """
    # 新しいjsonlのファイル情報と古いjsonlのファイル情報を比較
    unmatched_list = []
    for dct_l in later:
        for dct_f in formar:
            if dct_l["filename"] == dct_f["filename"]:
                diff_dict = {k: v for k, v in dct_l.items() if dct_l["md5_hash"] != dct_f["md5_hash"]}
                if diff_dict:
                    unmatched_list.append(diff_dict)
    return unmatched_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
    parser.add_argument("former")
    parser.add_argument("later")
    args = parser.parse_args()
    l = get_diff_list(args.former, args.later)
    sorted_list = sorted(l, key=lambda x: int(re.findall(r'\d+', x)[0]))
    print(sorted_list, len(sorted_list))