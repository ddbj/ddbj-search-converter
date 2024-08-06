import argparse
import hashlib
import os
import re
from pathlib import Path

from ddbj_search_converter.utils.get_2nd_directory import get_second_newest_dir


def get_diff_list(former: Path, later: Path) -> list:
    """
    二つのディレクトリにある同一の名前のファイルを比較し
    MD5に差があるファイルのリストを返す
    Args:
        former (FilePath): 分割したJSONLの置かれたディレクトリ
        later (FilePath): 分割されたJSONLの置かれたディレクトリ

    Returns:
        list: MD5が一致しないファイルのファイル名リスト
    """
    # 前回の処理のディレクトリを取得できなかった場合二番目に新しい日付が名前についたディレクトリを取得してそこからファイルリストを生成する
    try:
        former_info = get_file_info(former)
    except Exception:  # pylint: disable=broad-except
        next_dir = get_second_newest_dir(later)
        former_info = get_file_info(next_dir)

    later_info = get_file_info(later)
    if former == later:
        return [x["filename"] for x in former_info]
    else:
        unmated_info = get_unmatced_list(former_info, later_info)
        return [x["filename"] for x in unmated_info]


def get_file_info(directory: Path) -> list:
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
            except Exception:  # pylint: disable=broad-except
                print("error?: ", filename)

    return file_info_lst


def get_unmatced_list(formar: list, later: list) -> list:
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
    # laterにのみ追加されたファイルのリスト
    new_file_info = [item for item in later if item['filename'] not in {item['filename'] for item in formar}]
    if new_file_info:
        unmatched_list.append(new_file_info)
    return unmatched_list


def get_second_newest_dir(new_dir: FilePath):
    """親ディレクトリ内で、2番目に新しい日付のディレクトリのパスを返す

    Args:
        parent_dir: 親ディレクトリのパス

    Returns:
        str: 2番目に新しい日付のディレクトリのパス (存在しなければNone)
    """
    parent_dir = os.path.dirname(new_dir)
    try:
        # サブディレクトリの一覧を取得
        subdirs = [os.path.join(parent_dir, d) for d in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, d))]

        # 日付を抽出するための関数
        def extract_date(dir_path):
            # ディレクトリ名から日付部分を抽出 (日付形式が保証されている前提)
            date_str = os.path.basename(dir_path)
            try:
                return datetime.strptime(date_str, "%Y%m%d")
            except:
                return None

        # 日付でソート (新しい順)
        subdirs = [x for x in subdirs if extract_date(x)]
        sorted_dirs = sorted(subdirs, key=extract_date, reverse=True)

        # 最新の日付ディレクトリをスキップして、2番目に新しいものを返す
        if len(sorted_dirs) >= 2:
            return sorted_dirs[1]
        else:
            return None  # 2番目に新しいディレクトリが存在しない場合

    except (OSError, ValueError) as e:
        # エラーが発生した場合 (親ディレクトリが存在しない、日付の抽出に失敗など)
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BioProject XML to JSONL")
    parser.add_argument("former")
    parser.add_argument("later")
    args = parser.parse_args()
    l = get_diff_list(args.former, args.later)
    sorted_list = sorted(l, key=lambda x: int(re.findall(r'\d+', x)[0]))
    print(sorted_list, len(sorted_list))
