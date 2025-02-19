"""\
- BP, BS における共通処理をまとめる
- その他、共通化できそうな処理をまとめる
"""
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Tuple, Union

from ddbj_search_converter.config import DATE_FORMAT, LOGGER
from ddbj_search_converter.schema import Xref, XrefType


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
    """\
    - to ISO 8601 string
    """
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            raise ValueError(f"Invalid date format: {value}")
    except Exception as e:
        LOGGER.debug("Failed to format datetime %s: %s", value, e)

    return None


ID_PATTERN_MAP: Dict[XrefType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM[NED](\w)?\d+$"),
    "bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+$"),
    "sra-experiment": re.compile(r"[SDE]RX\d+"),
    "sra-run": re.compile(r"[SDE]RR\d+"),
    "sra-sample": re.compile(r"[SDE]RS\d+"),
    "sra-study": re.compile(r"[SDE]RP\d+"),
    "gea": re.compile(r"^E-GEAD-\d+$"),
    "insdc-assembly": re.compile(r"^GCA_[0-9]{9}(\.[0-9]+)?$"),
    "insdc-master": re.compile(r"^([A-Z]0{5}|[A-Z]{2}0{6}|[A-Z]{4,6}0{8,10}|[A-J][A-Z]{2}0{5})$"),
    "insdc": re.compile(r"^([A-Z]\d{5}|[A-Z]{2}\d{6}|[A-Z]{4,6}\d{8,10}|[A-J][A-Z]{2}\d{5})(\.\d+)?$"),
    "metabobank": re.compile(r"^MTB"),
    "taxonomy": re.compile(r"^\d+"),
}

URL_TEMPLATE: Dict[XrefType, str] = {
    "biosample": "https://ddbj.nig.ac.jp/resource/biosample/{id}",
    "bioproject": "https://ddbj.nig.ac.jp/resource/bioproject/{id}",
    "sra-experiment": "https://ddbj.nig.ac.jp/resource/sra-experiment/{id}",
    "sra-run": "https://ddbj.nig.ac.jp/resource/sra-run/{id}",
    "sra-sample": "https://ddbj.nig.ac.jp/resource/sra-sample/{id}",
    "sra-study": "https://ddbj.nig.ac.jp/resource/sra-study/{id}",
    "gea": "https://ddbj.nig.ac.jp/public/ddbj_database/gea/experiment/{prefix}/{id}/",
    "insdc-assembly": "https://www.ncbi.nlm.nih.gov/datasets/genome/{id}",
    "insdc-master": "https://www.ncbi.nlm.nih.gov/nuccore/{id}",
    "insdc": "https://getentry.ddbj.nig.ac.jp/getentry?database=ddbj&accession_number={id}",
    "metabobank": "https://mb2.ddbj.nig.ac.jp/study/{id}.html",
    "taxonomy": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={id}",
}


def to_xref(id_: str) -> Xref:
    for db_type, pattern in ID_PATTERN_MAP.items():
        if pattern.match(id_):
            url_template = URL_TEMPLATE[db_type]
            if db_type == "gea":
                gea_id_num = int(id_.removeprefix("E-GEAD-"))
                prefix = f"E-GEAD-{(gea_id_num // 1000) * 1000:03d}"
                url = url_template.format(prefix=prefix, id=id_)
            else:
                url = url_template.format(id=id_)

            return Xref(identifier=id_, type=db_type, url=url)

    # default は taxonomy を返す
    return Xref(identifier=id_, type="taxonomy", url=URL_TEMPLATE["taxonomy"].format(id=id_))
