"""Accession ID のパターン定義とバリデーション。"""

import re
from re import Pattern
from typing import Final

from ddbj_search_converter.dblink.db import AccessionType

ID_PATTERN_MAP: dict[AccessionType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM[NED](\w)?\d+\Z"),
    "bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+\Z"),
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
    "humandbs": re.compile(r"^hum\d+\Z"),
    "pubmed": re.compile(r"^\d+\Z"),  # pubmed は数字のみ (to_xref では最後にフォールバック)
    "taxonomy": re.compile(r"^\d+\Z"),
}

# BioSample accession を自由文 / カンマ区切り文字列から findall するための anchorless pattern。
# ID_PATTERN_MAP["biosample"] は文字列全体の validation 用 (anchor + `\w?` 付き) で用途が異なる。
BIOSAMPLE_ID_FINDALL_RE: Final[Pattern[str]] = re.compile(r"SAM[NDE]\d+")


def is_valid_accession(accession_id: str, acc_type: AccessionType) -> bool:
    """指定された AccessionType に対して ID が正しいパターンかを検証する。"""
    pattern = ID_PATTERN_MAP.get(acc_type)
    if pattern is None:
        return False
    return bool(pattern.match(accession_id))


_DDBJ_SRA_PREFIXES = ("DRA", "DRR", "DRX", "DRZ", "DRS", "DRP")


def is_ddbj_sra_accession(accession: str) -> bool:
    """DDBJ origin の SRA accession (DRA/DRR/DRX/DRZ/DRS/DRP) かを判定する。

    NCBI SRA Metadata には DDBJ 由来の SRA accession も含まれているため、
    NCBI バッチ (source="sra") で DDBJ origin の不完全 doc を生成しないよう
    skip するときに使う。
    """
    return accession.startswith(_DDBJ_SRA_PREFIXES)


_ZERO_PADDABLE_ACCESSION_RE = re.compile(r"^([A-Za-z]+)(\d+)$")


def is_zero_padding_variant(sid: str, accession: str) -> bool:
    """`sid` が `accession` の「同じ英字 prefix・数値部はゼロ埋め桁数の違いを無視して一致」かを判定する。

    例: ``is_zero_padding_variant("JGAS00000000001", "JGAS000001")`` は True
    (どちらも prefix ``JGAS``・数値 ``1``)。prefix なし / 数値部なし / prefix 違い /
    数値違い / 空文字 はすべて False を返す。

    JGA の SECONDARY_ID には accession のゼロ埋め桁数だけが異なる同一エントリの別表記
    (例: ``JGAS000001`` に対する ``JGAS00000000001``) が入る。sameAs には残すが、
    alias ドキュメントを作ると同一エントリーが一覧で重複するので、その判定に使う。
    """
    m_sid = _ZERO_PADDABLE_ACCESSION_RE.match(sid)
    m_acc = _ZERO_PADDABLE_ACCESSION_RE.match(accession)
    if m_sid is None or m_acc is None:
        return False
    return m_sid.group(1) == m_acc.group(1) and int(m_sid.group(2)) == int(m_acc.group(2))
