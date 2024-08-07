import hashlib
from pathlib import Path
from typing import List


def get_diff_files(previous_dir: Path, current_dir: Path) -> List[Path]:
    """
    引数の dir 内において、md5 hash が異なるファイルのリストを返す
    current_dir に存在し、previous_dir に存在しないファイルも含む
    previous_dir に存在し、current_dir に存在しないファイルは含まない
    """
    diff_files = []
    prev_files_md5 = {}
    for prev_file in previous_dir.glob("*.jsonl"):
        if prev_file.is_file():
            prev_files_md5[prev_file.name] = calculate_md5_hash(prev_file)

    for curr_file in current_dir.glob("*.jsonl"):
        if curr_file.is_file():
            curr_md5 = calculate_md5_hash(curr_file)
            if curr_file.name not in prev_files_md5 or prev_files_md5[curr_file.name] != curr_md5:
                diff_files.append(curr_file)

    return diff_files


def calculate_md5_hash(file: Path) -> str:
    """
    指定されたファイルの md5 hash を計算して返す
    """
    hash_md5 = hashlib.md5()
    with file.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()
