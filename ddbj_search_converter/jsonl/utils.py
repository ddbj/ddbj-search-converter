"""JSONL 生成用の共通ユーティリティ関数。"""
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Pattern

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (AccessionType,
                                             get_related_entities_bulk)
from ddbj_search_converter.schema import Xref, XrefType

ID_PATTERN_MAP: Dict[XrefType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM[NED](\w)?\d+$"),
    "bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+$"),
    "umbrella-bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+$"),  # bioproject と同じパターン
    "sra-submission": re.compile(r"[SDE]RA\d+"),
    "sra-study": re.compile(r"[SDE]RP\d+"),
    "sra-experiment": re.compile(r"[SDE]RX\d+"),
    "sra-run": re.compile(r"[SDE]RR\d+"),
    "sra-sample": re.compile(r"[SDE]RS\d+"),
    "sra-analysis": re.compile(r"[SDE]RZ\d+"),
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
    "metabobank": re.compile(r"^MTB"),
    "hum-id": re.compile(r"^hum\d+$"),
    "pubmed-id": re.compile(r"^\d+$"),  # pubmed-id は数字のみ (to_xref では最後にフォールバック)
    "taxonomy": re.compile(r"^\d+"),
}

URL_TEMPLATE: Dict[XrefType, str] = {
    "biosample": "https://ddbj.nig.ac.jp/search/entry/biosample/{id}",
    "bioproject": "https://ddbj.nig.ac.jp/search/entry/bioproject/{id}",
    "umbrella-bioproject": "https://ddbj.nig.ac.jp/search/entry/bioproject/{id}",
    "sra-submission": "https://ddbj.nig.ac.jp/search/entry/sra-submission/{id}",
    "sra-study": "https://ddbj.nig.ac.jp/search/entry/sra-study/{id}",
    "sra-experiment": "https://ddbj.nig.ac.jp/search/entry/sra-experiment/{id}",
    "sra-run": "https://ddbj.nig.ac.jp/search/entry/sra-run/{id}",
    "sra-sample": "https://ddbj.nig.ac.jp/search/entry/sra-sample/{id}",
    "sra-analysis": "https://ddbj.nig.ac.jp/search/entry/sra-analysis/{id}",
    "jga-study": "https://ddbj.nig.ac.jp/search/entry/jga-study/{id}",
    "jga-dataset": "https://ddbj.nig.ac.jp/search/entry/jga-dataset/{id}",
    "jga-dac": "https://ddbj.nig.ac.jp/search/entry/jga-dac/{id}",
    "jga-policy": "https://ddbj.nig.ac.jp/search/entry/jga-policy/{id}",
    "gea": "https://ddbj.nig.ac.jp/public/ddbj_database/gea/experiment/{prefix}/{id}/",
    "geo": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={id}",
    "insdc-assembly": "https://www.ncbi.nlm.nih.gov/datasets/genome/{id}",
    "insdc-master": "https://www.ncbi.nlm.nih.gov/nuccore/{id}",
    "metabobank": "https://mb2.ddbj.nig.ac.jp/study/{id}.html",
    "hum-id": "https://humandbs.dbcls.jp/{id}",
    "pubmed-id": "https://pubmed.ncbi.nlm.nih.gov/{id}/",
    "taxonomy": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={id}",
}


def to_xref(id_: str, *, type_hint: Optional[XrefType] = None) -> Xref:
    """
    ID パターンから Xref を自動生成する。

    type_hint が指定されている場合はそれを優先する。
    指定がない場合は ID_PATTERN_MAP を順に走査してマッチするタイプを判定する。
    どのパターンにもマッチしない場合は taxonomy として扱う。
    """
    if type_hint is not None:
        url_template = URL_TEMPLATE[type_hint]
        if type_hint == "gea":
            gea_id_num = int(id_.removeprefix("E-GEAD-"))
            prefix = f"E-GEAD-{(gea_id_num // 1000) * 1000:03d}"
            url = url_template.format(prefix=prefix, id=id_)
        else:
            url = url_template.format(id=id_)
        return Xref(identifier=id_, type=type_hint, url=url)

    # pubmed-id と taxonomy は数字のみなので最後にチェックする
    # umbrella-bioproject は bioproject と同じパターンなのでパターンマッチでは判定できない
    priority_types: List[XrefType] = [
        "biosample",
        "bioproject",
        "sra-submission",
        "sra-study",
        "sra-experiment",
        "sra-run",
        "sra-sample",
        "sra-analysis",
        "jga-study",
        "jga-dataset",
        "jga-dac",
        "jga-policy",
        "gea",
        "geo",
        "insdc-assembly",
        "insdc-master",
        "metabobank",
        "hum-id",
    ]

    for db_type in priority_types:
        pattern = ID_PATTERN_MAP[db_type]
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
    return Xref(
        identifier=id_, type="taxonomy", url=URL_TEMPLATE["taxonomy"].format(id=id_)
    )


def write_jsonl(output_path: Path, docs: List[Any]) -> None:
    """Pydantic モデルインスタンスのリストを JSONL ファイルに書き込む。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(doc.model_dump_json(by_alias=True))
            f.write("\n")


def get_dbxref_map(
    config: Config,
    entity_type: AccessionType,
    accessions: List[str],
) -> Dict[str, List[Xref]]:
    """dblink DB から関連エントリを取得し、Xref リストに変換する。"""
    if not accessions:
        return {}

    relations = get_related_entities_bulk(
        config, entity_type=entity_type, accessions=accessions
    )

    result: Dict[str, List[Xref]] = {}
    for accession, related_list in relations.items():
        xrefs: List[Xref] = []
        for related_type, related_id in related_list:
            xref = to_xref(related_id, type_hint=related_type)
            xrefs.append(xref)
        xrefs.sort(key=lambda x: x.identifier)
        result[accession] = xrefs

    return result
