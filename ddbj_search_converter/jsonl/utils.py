"""JSONL 生成用の共通ユーティリティ関数。"""

from pathlib import Path
from typing import Any

from ddbj_search_converter.config import SEARCH_BASE_URL, Config
from ddbj_search_converter.dblink.db import AccessionType, get_related_entities_bulk
from ddbj_search_converter.id_patterns import ID_PATTERN_MAP
from ddbj_search_converter.schema import Xref, XrefType

URL_TEMPLATE: dict[XrefType, str] = {
    "biosample": f"{SEARCH_BASE_URL}/search/entry/biosample/{{id}}",
    "bioproject": f"{SEARCH_BASE_URL}/search/entry/bioproject/{{id}}",
    "sra-submission": f"{SEARCH_BASE_URL}/search/entry/sra-submission/{{id}}",
    "sra-study": f"{SEARCH_BASE_URL}/search/entry/sra-study/{{id}}",
    "sra-experiment": f"{SEARCH_BASE_URL}/search/entry/sra-experiment/{{id}}",
    "sra-run": f"{SEARCH_BASE_URL}/search/entry/sra-run/{{id}}",
    "sra-sample": f"{SEARCH_BASE_URL}/search/entry/sra-sample/{{id}}",
    "sra-analysis": f"{SEARCH_BASE_URL}/search/entry/sra-analysis/{{id}}",
    "jga-study": f"{SEARCH_BASE_URL}/search/entry/jga-study/{{id}}",
    "jga-dataset": f"{SEARCH_BASE_URL}/search/entry/jga-dataset/{{id}}",
    "jga-dac": f"{SEARCH_BASE_URL}/search/entry/jga-dac/{{id}}",
    "jga-policy": f"{SEARCH_BASE_URL}/search/entry/jga-policy/{{id}}",
    "gea": "https://ddbj.nig.ac.jp/public/ddbj_database/gea/experiment/{prefix}/{id}/",
    "geo": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={id}",
    "insdc": "https://getentry.ddbj.nig.ac.jp/getentry?database=ddbj&accession_number={id}",
    "insdc-assembly": "https://www.ncbi.nlm.nih.gov/datasets/genome/{id}",
    "insdc-master": "https://www.ncbi.nlm.nih.gov/nuccore/{id}",
    "metabobank": "https://mb2.ddbj.nig.ac.jp/study/{id}.html",
    "humandbs": "https://humandbs.dbcls.jp/{id}",
    "pubmed": "https://pubmed.ncbi.nlm.nih.gov/{id}/",
    "taxonomy": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={id}",
}


def to_xref(id_: str, *, type_hint: XrefType | None = None) -> Xref:
    """
    ID パターンから Xref を自動生成する。

    type_hint が指定されている場合はそれを優先する。
    指定がない場合は ID_PATTERN_MAP を順に走査してマッチするタイプを判定する。
    どのパターンにもマッチしない場合は taxonomy として扱う。
    """
    if type_hint is not None:
        if type_hint not in URL_TEMPLATE:
            raise ValueError(f"Unknown type_hint: {type_hint}")
        url_template = URL_TEMPLATE[type_hint]
        if type_hint == "gea":
            try:
                gea_id_num = int(id_.removeprefix("E-GEAD-"))
            except ValueError:
                gea_id_num = 0
            prefix = f"E-GEAD-{(gea_id_num // 1000) * 1000:03d}"
            url = url_template.format(prefix=prefix, id=id_)
        else:
            url = url_template.format(id=id_)
        return Xref(identifier=id_, type=type_hint, url=url)

    # pubmed と taxonomy は数字のみなので最後にチェックする
    # insdc は ID_PATTERN_MAP にパターンがないため含めない（type_hint 経由でのみ使用）
    priority_types: list[XrefType] = [
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
        "humandbs",
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
    return Xref(identifier=id_, type="taxonomy", url=URL_TEMPLATE["taxonomy"].format(id=id_))


def ensure_attribute_list(
    properties: dict[str, Any],
    attribute_paths: list[list[str]],
) -> None:
    """指定パス末端の dict を [dict] にラップする（in-place）。

    XML → JSON 変換では、Attribute 要素が 1 件のとき dict、複数件のとき list となる。
    この関数は、対象 index ごとに明示指定したパス末端を常に list に揃えることで、
    ダウンストリーム側でのハンドリングを一貫させる。
    途中経路が list の場合は各要素に対して再帰的にパスを辿る。
    """
    for path in attribute_paths:
        if not path:
            continue
        _apply_attribute_list(properties, path)


def _apply_attribute_list(node: Any, path: list[str]) -> None:
    """path をたどって末端を探し、dict なら [dict] にラップする再帰ヘルパ。

    - node が list の場合は全要素に同じ path で再帰（途中経路 list 対応）
    - node が dict でも list でもなければ no-op
    - path の残りが 1 つ（head のみ）なら末端。値が dict のときだけラップ
    - それ以外は head を辿って次のノードで再帰
    """
    if isinstance(node, list):
        for item in node:
            _apply_attribute_list(item, path)
        return
    if not isinstance(node, dict):
        return
    head, *rest = path
    if rest:
        _apply_attribute_list(node.get(head), rest)
        return
    value = node.get(head)
    if isinstance(value, dict):
        node[head] = [value]


def write_jsonl(output_path: Path, docs: list[Any]) -> None:
    """Pydantic モデルインスタンスのリストを JSONL ファイルに書き込む。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(doc.model_dump_json(by_alias=True))
            f.write("\n")


def enrich_umbrella_relations(config: Config, docs: dict[str, Any]) -> None:
    """BioProject の docs に parent/child 関連を設定する。

    Umbrella DB から親子関連を取得し、各 doc の parentBioProjects / childBioProjects を設定する。
    """
    if not docs:
        return

    from ddbj_search_converter.dblink.db import get_umbrella_parent_child_maps

    parent_map, child_map = get_umbrella_parent_child_maps(config, list(docs.keys()))
    for acc, parent_accs in parent_map.items():
        if acc in docs:
            parent_xrefs = [to_xref(pid, type_hint="bioproject") for pid in parent_accs]
            docs[acc].parentBioProjects = sorted(parent_xrefs, key=lambda x: x.identifier)
    for acc, child_accs in child_map.items():
        if acc in docs:
            child_xrefs = [to_xref(cid, type_hint="bioproject") for cid in child_accs]
            docs[acc].childBioProjects = sorted(child_xrefs, key=lambda x: x.identifier)


def get_dbxref_map(
    config: Config,
    entity_type: AccessionType,
    accessions: list[str],
) -> dict[str, list[Xref]]:
    """dblink DB から関連エントリを取得し、Xref リストに変換する。"""
    if not accessions:
        return {}

    relations = get_related_entities_bulk(config, entity_type=entity_type, accessions=accessions)

    result: dict[str, list[Xref]] = {}
    for accession, related_list in relations.items():
        xrefs: list[Xref] = []
        for related_type, related_id in related_list:
            xref = to_xref(related_id, type_hint=related_type)
            xrefs.append(xref)
        xrefs.sort(key=lambda x: x.identifier)
        result[accession] = xrefs

    return result
