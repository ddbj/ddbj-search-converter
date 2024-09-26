import sys
import os
import re
import hashlib
import argparse
# TODO: utilsパッケージの読み込みが環境によって行えないのでモジュールに関数を含めるようにする
sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.get_2nd_directory import get_second_newest_dir
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
    try:
        former_info = get_file_info(former)
    except:
        next_dir = get_second_newest_dir(later)
        former_info = get_file_info(next_dir)
    later_info = get_file_info(later)
    print("file_info: ", len(former_info), len(later_info))
    unmached_info = get_unmached_list(former_info, later_info)
    return [x["filename"] for x in unmached_info]


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
        print("ok")
        for filename in files:
            ext = filename.split('.')[1]
            if ext == "jsonl":
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


def get_unmached_list(formar:list, later:list)->list:
    """
    二つのリストでfilenameが同じ情報を比較し一致しないファイルの情報を返す
    Args:
        formar (list): _description_
        later (list): _description_

    Returns:
        list: _description_
    """
    # 新しいjsonlのファイル情報と古いjsonlのファイル情報を比較
    # 新しいディレクトリにしか存在しないファイルもリストに含める
    #formar_names = [x["filename"] for x in formar if x["filename"].endswith("jsonl")]
    #later_names = [x["filename"] for x in later if x["filename"].endswith("jsonl")]
    #new_in_later = set(later_names) ^ set(formar_names)
    unmatched_list = []
    for dct_l in later:
        for dct_f in formar:
            if dct_l["filename"] == dct_f["filename"]:
                diff_dict = {k: v for k, v in dct_l.items() if dct_l["md5_hash"] != dct_f["md5_hash"]}
                if diff_dict:
                    unmatched_list.append(diff_dict)
    # laterにのみ追加されたファイルのリスト
    new_file_info = [item for item in later if item['filename'] not in {item['filename'] for item in formar}]
    if new_file_info:
        unmatched_list.append(new_file_info)
    return unmatched_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
    parser.add_argument("former")
    parser.add_argument("later")
    args = parser.parse_args()
    l = get_diff_list(args.former, args.later)
    print("l: ", len(l))
    sorted_list = sorted(l, key=lambda x: int(re.findall(r'\d+', x)[0]))
    print(len(sorted_list))