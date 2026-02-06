"""Accession ID のパターン定義とバリデーション。"""
import re
from typing import Dict, Pattern

from ddbj_search_converter.dblink.db import AccessionType

ID_PATTERN_MAP: Dict[AccessionType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM[NED](\w)?\d+$"),
    "bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+$"),
    "umbrella-bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+$"),  # bioproject と同じパターン
    "sra-submission": re.compile(r"^[SDE]RA\d+$"),
    "sra-study": re.compile(r"^[SDE]RP\d+$"),
    "sra-experiment": re.compile(r"^[SDE]RX\d+$"),
    "sra-run": re.compile(r"^[SDE]RR\d+$"),
    "sra-sample": re.compile(r"^[SDE]RS\d+$"),
    "sra-analysis": re.compile(r"^[SDE]RZ\d+$"),
    "jga-study": re.compile(r"^JGAS\d+$"),
    "jga-dataset": re.compile(r"^JGAD\d+$"),
    "jga-dac": re.compile(r"^JGAC\d+$"),
    "jga-policy": re.compile(r"^JGAP\d+$"),
    "gea": re.compile(r"^E-GEAD-\d+$"),
    "geo": re.compile(r"^GSE\d+$"),
    "insdc-assembly": re.compile(r"^GCA_[0-9]{9}(\.[0-9]+)?$"),
    "insdc-master": re.compile(
        r"^([A-Z]0{5}|[A-Z]{2}0{6}|[A-Z]{4,6}0{8,10}|[A-J][A-Z]{2}0{5})$"
    ),
    "metabobank": re.compile(r"^MTBKS\d+$"),
    "hum-id": re.compile(r"^hum\d+$"),
    "pubmed-id": re.compile(r"^\d+$"),  # pubmed-id は数字のみ (to_xref では最後にフォールバック)
    "taxonomy": re.compile(r"^\d+$"),
}


def is_valid_accession(accession_id: str, acc_type: AccessionType) -> bool:
    """指定された AccessionType に対して ID が正しいパターンかを検証する。"""
    pattern = ID_PATTERN_MAP.get(acc_type)
    if pattern is None:
        return True
    return bool(pattern.match(accession_id))
