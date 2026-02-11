"""Accession ID のパターン定義とバリデーション。"""

import re
from re import Pattern

from ddbj_search_converter.dblink.db import AccessionType

ID_PATTERN_MAP: dict[AccessionType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM[NED](\w)?\d+\Z"),
    "bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+\Z"),
    "umbrella-bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+\Z"),  # bioproject と同じパターン
    "sra-submission": re.compile(r"^[SDE]RA\d+\Z"),
    "sra-study": re.compile(r"^[SDE]RP\d+\Z"),
    "sra-experiment": re.compile(r"^[SDE]RX\d+\Z"),
    "sra-run": re.compile(r"^[SDE]RR\d+\Z"),
    "sra-sample": re.compile(r"^[SDE]RS\d+\Z"),
    "sra-analysis": re.compile(r"^[SDE]RZ\d+\Z"),
    "jga-study": re.compile(r"^JGAS\d+\Z"),
    "jga-dataset": re.compile(r"^JGAD\d+\Z"),
    "jga-dac": re.compile(r"^JGAC\d+\Z"),
    "jga-policy": re.compile(r"^JGAP\d+\Z"),
    "gea": re.compile(r"^E-GEAD-\d+\Z"),
    "geo": re.compile(r"^GSE\d+\Z"),
    "insdc-assembly": re.compile(r"^GCA_[0-9]{9}(\.[0-9]+)?\Z"),
    "insdc-master": re.compile(r"^([A-Z]0{5}|[A-Z]{2}0{6}|[A-Z]{4,6}0{8,10}|[A-J][A-Z]{2}0{5})\Z"),
    "metabobank": re.compile(r"^MTBKS\d+\Z"),
    "hum-id": re.compile(r"^hum\d+\Z"),
    "pubmed-id": re.compile(r"^\d+\Z"),  # pubmed-id は数字のみ (to_xref では最後にフォールバック)
    "taxonomy": re.compile(r"^\d+\Z"),
}


def is_valid_accession(accession_id: str, acc_type: AccessionType) -> bool:
    """指定された AccessionType に対して ID が正しいパターンかを検証する。"""
    pattern = ID_PATTERN_MAP.get(acc_type)
    if pattern is None:
        return False
    return bool(pattern.match(accession_id))
