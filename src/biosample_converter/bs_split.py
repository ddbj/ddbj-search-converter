import os
import toolz
import argparse
import datetime
import glob
import re
from multiprocessing import Pool
from typing import NewType

FilePath = NewType('FilePath', str)

parser = argparse.ArgumentParser(description="Split jsonl file from behind")
parser.add_argument("input", help="jsonl source file directory")
parser.add_argument("output", help="target directory")
args = parser.parse_args()
# 25000レコード = 50000行づつ分割
batch_size = 50000

def split_files(file_path: FilePath):
    """
    引数でpathが渡されたjsonlファイルを分割し書き出す
    """
    output_path = args.output
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # 元のファイルの拡張子を除いたファイル名のみ取得
    original_file_name = re.split(r"[/.]", file_path)
    with open(file_path, 'r') as f:
        lines = toolz.partition(batch_size, f.readlines())
        for i, part in enumerate(lines):
            # print(f"{output}/{filename}.part{i}")
            # 元のファイル名に処理ごとの連番をつける
            with open(f"{output_path}/{original_file_name[-2]}_{i}.jsonl", "w") as out:
                out.writelines(part)


def main():
    """
    並列処理のハンドリング
    """
    # ファイルリストを取得
    target_dir = args.input
    target_pattern = "*.jsonl"
    file_list = glob.glob(os.path.join(target_dir, target_pattern))
    p = Pool(20)
    try:
        # cpu_count()次第で分割数は変える
        p.map(split_files, file_list)
    except Exception as e:
        print("main: ", e)


if __name__ == "__main__":
    """
    並列処理でxmlからjsonlへ変換された複数のjsonlファイルを
    Elasticsearchにインポート可能なサイズまで分割して書き出す。
    """
    main()