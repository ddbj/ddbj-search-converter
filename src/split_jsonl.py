import os
import toolz
import argparse
from typing import NewType

FilePath = NewType('FilePath', str)


def split_file_from_behind(filename:FilePath,output:FilePath, n):
    """
    jsonlファイルを後方から特定の行数のファイルに分割して出力する
    Args:
        filename (_type_): _description_
        n (_type_): _description_
    """
    with open(filename, 'r') as f:
        lines = toolz.partition(n, reversed(f.readlines()))
        for i, part in enumerate(lines):
            # print(f"{output}/{filename}.part{i}")
            with open(f"{output}/bioproject_part{i}.jsonl", 'w') as out:
                out.writelines(part)

def split_file(filename:FilePath,output:FilePath, n):
    """
    jsonlファイルを後方から特定の行数のファイルに分割して出力する
    Args:
        filename (_type_): _description_
        n (_type_): _description_
    """
    # 出力先を設定
    today = datetime.datetime.now()
    today_str = today.strftime('%Y%m%d')
    output_path = f"{output}/{today_str}"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    with open(filename, 'r') as f:
        lines = toolz.partition(n, f.readlines())
        for i, part in enumerate(lines):
            # print(f"{output}/{filename}.part{i}")
            with open(f"{output}/bioproject_part{i}.jsonl", 'w') as out:
                out.writelines(part)


def rm_old_file(directory:FilePath):
    """
    処理開始前に既存のファイルの存在を確認し削除する
    Args:
        file_path (_type_): 出力ファイルのパス（ディレクトリ）
    """
    # ファイルの存在を確認する
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)


if __name__ == "__main__":
    filename = "input.txt"
    parser = argparse.ArgumentParser(description="Split jsonl file from behind")
    parser.add_argument("filename")
    parser.add_argument("output")
    args = parser.parse_args()
    n = 2000
    # 出力先にファイルが残っていた場合ファイルを削除する
    rm_old_file(args.output)
    split_file(args.filename, args.output, n)