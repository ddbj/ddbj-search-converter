import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from ddbj_search_converter.config import DATE_FORMAT, LOGGER


def bulk_insert_to_es(
    es_base_url: str,
    jsonl: Optional[List[Dict[str, Any]]] = None,
    str_data: Optional[str] = None,
    raise_on_error: bool = True,
) -> None:
    data = None
    if jsonl is not None:
        data = "\n".join(json.dumps(d) for d in jsonl)
    if str_data is not None:
        data = str_data
    if data is None:
        raise ValueError("Either jsonl or str_data must be specified")

    res = requests.post(
        f"{es_base_url}/_bulk",
        data=data,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
    )
    if res.ok:
        pass
    else:
        if raise_on_error:
            res.raise_for_status()
        else:
            LOGGER.error("Failed to bulk insert to Elasticsearch: status_code=%s, response=%s", res.status_code, res.text)


def find_previous_dir(current_dir: Path) -> Path:
    """\
    親ディレクトリ内で、current dir の一つ前の日付のディレクトリのパスを返す
    """
    parent_dir = current_dir.parent
    try:
        current_date = datetime.strptime(current_dir.name, DATE_FORMAT)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {current_dir.name}") from e

    previous_dir = None
    previous_date = None
    for dir_path in parent_dir.iterdir():
        if dir_path.is_dir():
            try:
                dir_date = datetime.strptime(dir_path.name, DATE_FORMAT)
                if dir_date < current_date:
                    if previous_date is None or dir_date > previous_date:
                        previous_dir = dir_path
                        previous_date = dir_date
            except ValueError:
                continue

    if previous_dir is None:
        raise FileNotFoundError(f"Previous directory not found: {current_dir}")

    return previous_dir


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
