"""\
- BP, BS における共通処理をまとめる
- その他、共通化できそうな処理をまとめる
"""
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union

from ddbj_search_converter.config import DATE_FORMAT, LOGGER


def get_recent_dirs(base_dir: Path, latest_dir: Optional[Path], prior_dir: Optional[Path]) -> Tuple[Path, Optional[Path]]:
    """\
    - 最新の日付の dir (latest_dir) とその前の日付のうち最新の dir (prior_dir) を返す
    - latest_dir や prior_dir が指定されていない場合、よしなに処理する
        - if latest_dir is None and prior_dir is None: 最新の日付の dir とその前の日付のうち最新の dir を返す
        - if latest_dir is not None and prior_dir is None: latest_dir とその前の日付のうち最新の dir を返す
        - if latest_dir is None and prior_dir is not None: 最新の日付の dir と prior_dir を返す (恐らく呼ばれないケース)
        - if latest_dir is not None and prior_dir is not None: そもそも、この関数使わなくていい
    """
    if latest_dir is not None and prior_dir is not None:
        return latest_dir, prior_dir

    def _parse_date_from_name(dir_path: Path) -> datetime:
        try:
            return datetime.strptime(dir_path.name, DATE_FORMAT)
        except ValueError as e:
            LOGGER.error("Failed to parse date from %s: %s", dir_path, e)
            raise

    dirs: List[Path] = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and _parse_date_from_name(d) is not None],
        key=_parse_date_from_name,
        reverse=True,
    )

    if not dirs:
        raise ValueError(f"No valid date dirs in {base_dir}")

    if latest_dir is None:
        latest_dir = dirs[0]
    if prior_dir is None:
        prior_candidates = [
            d for d in dirs if _parse_date_from_name(d) < _parse_date_from_name(latest_dir)
        ]
        if prior_candidates:
            prior_dir = prior_candidates[0]
        else:
            prior_dir = None  # 例えば、初回実行時など

    return latest_dir, prior_dir


def _compute_checksum(file: Path) -> str:
    hasher = hashlib.md5()
    with file.open("rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)

    return hasher.hexdigest()


def _is_same_file(file_1: Path, file_2: Path) -> bool:
    if file_1.stat().st_size != file_2.stat().st_size:
        return False
    return _compute_checksum(file_1) == _compute_checksum(file_2)


def find_insert_target_files(latest_dir: Path, prior_dir: Optional[Path]) -> List[Path]:
    """\
    - latest_files にしかないファイルは無条件に対象にする
    - 両方に存在するファイルは、_is_same_file で比較して異なる場合のみ対象にする
    """
    latest_files = {file.name: file for file in latest_dir.glob("*.jsonl")}

    if prior_dir is None:
        return list(latest_files.values())

    prior_files = {file.name: file for file in prior_dir.glob("*.jsonl")}

    target_files = []

    for name, latest_file in latest_files.items():
        if name not in prior_files:
            target_files.append(latest_file)
        else:
            prior_file = prior_files[name]
            if not _is_same_file(latest_file, prior_file):
                target_files.append(latest_file)

    return target_files


def format_date(value: Optional[Union[str, datetime]]) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.strftime(DATE_FORMAT)
        elif isinstance(value, str):
            return datetime.fromisoformat(value).strftime(DATE_FORMAT)
        else:
            raise ValueError(f"Invalid date format: {value}")
    except Exception as e:
        LOGGER.debug("Failed to format datetime %s: %s", value, e)

    return None
