import os
from datetime import datetime

from typing import NewType

FilePath = NewType('FilePath', str)


def get_second_newest_dir(new_dir:FilePath):
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
      return datetime.strptime(date_str, "%Y%m%d")  # 抽出した文字列を日付オブジェクトに変換

    # 日付でソート (新しい順)
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

# テスト (親ディレクトリのパスを適宜変更してください)
parent_dir = "/path/to/parent/dir"
second_newest_dir = get_second_newest_dir(parent_dir)

if second_newest_dir:
  print(f"2番目に新しいディレクトリ: {second_newest_dir}")
else:
  print("2番目に新しいディレクトリは存在しません")
