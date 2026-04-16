"""
JGA (Japanese Genotype-phenotype Archive) の XML/CSV/TSV から関連を抽出し、DBLink DB に挿入する。

入力:
- {const_dir}/dblink/jga_study_hum_id.tsv  -> jga-study - humandbs
- {const_dir}/dblink/jga_dataset_hum_id.tsv -> jga-dataset - humandbs
- JGA_BASE_PATH/jga-study.xml
    - PUBLICATIONS/PUBLICATION: id 属性 -> pubmed
- JGA_BASE_PATH/*-relation.csv
    - JGA 内部関連 (dataset/study/policy/dac) を構築

出力:
- jga-study -> humandbs
- jga-dataset -> humandbs
- jga-study -> pubmed
- jga-study <-> jga-dataset
- jga-study <-> jga-dac
- jga-study <-> jga-policy
- jga-dataset <-> jga-policy
- jga-dataset <-> jga-dac
- jga-policy <-> jga-dac
"""

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

from lxml import etree

from ddbj_search_converter.config import (
    JGA_ANALYSIS_STUDY_CSV,
    JGA_DATA_EXPERIMENT_CSV,
    JGA_DATASET_ANALYSIS_CSV,
    JGA_DATASET_DATA_CSV,
    JGA_DATASET_HUM_ID_REL_PATH,
    JGA_DATASET_POLICY_CSV,
    JGA_EXPERIMENT_STUDY_CSV,
    JGA_POLICY_DAC_CSV,
    JGA_STUDY_HUM_ID_REL_PATH,
    JGA_STUDY_XML,
    Config,
    get_config,
)
from ddbj_search_converter.dblink.db import AccessionType, IdPairs, load_to_db
from ddbj_search_converter.dblink.utils import filter_sra_pairs_by_blacklist, load_jga_blacklist
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import log_debug, log_info, run_logger
from ddbj_search_converter.logging.schema import DebugCategory

# === CSV relation operations ===


def read_relation_csv(csv_path: Path) -> set[tuple[str, str]]:
    """
    JGA relation CSV を読み込み、(from_id, to_id) の set を返す。
    CSV format: id, from_id, to_id (header あり、1 列目は無視)

    Raises:
        FileNotFoundError: If CSV file is not found.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    result: set[tuple[str, str]] = set()
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) < 3:
                continue
            result.add((row[1], row[2]))

    return result


def join_relations(
    ab: set[tuple[str, str]],
    bc: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """(a, b) と (b, c) を join して (a, c) を返す。"""
    b_to_c: dict[str, set[str]] = defaultdict(set)
    for b, c in bc:
        b_to_c[b].add(c)

    return {(a, c) for a, b in ab for c in b_to_c.get(b, ())}


def reverse_relation(relation: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """(a, b) set を (b, a) set に変換。"""
    return {(b, a) for a, b in relation}


def build_jga_internal_relations() -> dict[str, IdPairs]:
    """
    CSV から JGA 内部関連を構築する。

    Returns:
        Dictionary with keys:
        - dataset_policy: CSV から直接
        - policy_dac: CSV から直接
        - dataset_study: (dataset_analysis JOIN analysis_study) UNION
                         (dataset_data JOIN data_experiment JOIN experiment_study)
        - dataset_dac: dataset_policy JOIN policy_dac
        - study_dac: reverse(dataset_study) JOIN dataset_dac
        - study_policy: reverse(dataset_study) JOIN dataset_policy
    """
    # Read CSV files
    dataset_policy = read_relation_csv(JGA_DATASET_POLICY_CSV)
    policy_dac = read_relation_csv(JGA_POLICY_DAC_CSV)
    dataset_analysis = read_relation_csv(JGA_DATASET_ANALYSIS_CSV)
    analysis_study = read_relation_csv(JGA_ANALYSIS_STUDY_CSV)
    dataset_data = read_relation_csv(JGA_DATASET_DATA_CSV)
    data_experiment = read_relation_csv(JGA_DATA_EXPERIMENT_CSV)
    experiment_study = read_relation_csv(JGA_EXPERIMENT_STUDY_CSV)

    # Calculate dataset_study
    # Path 1: dataset -> analysis -> study
    path1 = join_relations(dataset_analysis, analysis_study)
    # Path 2: dataset -> data -> experiment -> study
    data_study = join_relations(data_experiment, experiment_study)
    path2 = join_relations(dataset_data, data_study)
    dataset_study = path1 | path2

    # Calculate dataset_dac: dataset -> policy -> dac
    dataset_dac = join_relations(dataset_policy, policy_dac)

    # Calculate study relations via dataset
    study_dataset = reverse_relation(dataset_study)
    study_dac = join_relations(study_dataset, dataset_dac)
    study_policy = join_relations(study_dataset, dataset_policy)

    return {
        "dataset_policy": dataset_policy,
        "policy_dac": policy_dac,
        "dataset_study": dataset_study,
        "dataset_dac": dataset_dac,
        "study_dac": study_dac,
        "study_policy": study_policy,
    }


# === XML parsing ===


def _element_to_dict(element: etree._Element) -> dict[str, Any] | str:
    """lxml Element を dict に変換する。属性はプレフィックスなし。"""
    result: dict[str, Any] = {}

    # 属性を追加
    for attr_key, attr_value in element.attrib.items():
        key: str = str(attr_key)
        if "}" in key:
            key = key.split("}")[1]
        result[key] = attr_value

    # テキストコンテンツを処理
    text = element.text
    if text is not None:
        text = text.strip()
        if text:
            if result:  # 属性がある場合
                result["content"] = text
            elif len(element) == 0:  # 子要素がない場合
                return {"content": text} if result else text

    # 子要素を処理
    children: dict[str, list[Any]] = {}
    for child in element:
        child_tag: str = str(child.tag)
        if "}" in child_tag:
            child_tag = child_tag.split("}")[1]

        child_value = _element_to_dict(child)
        if child_tag in children:
            children[child_tag].append(child_value)
        else:
            children[child_tag] = [child_value]

    # 単一要素のリストを値に変換
    for child_key, child_list in children.items():
        if len(child_list) == 1:
            result[child_key] = child_list[0]
        else:
            result[child_key] = child_list

    return result


def load_jga_study_xml() -> list[dict[str, Any]]:
    """jga-study.xml を読み込み、STUDY エントリのリストを返す。

    Raises:
        FileNotFoundError: If XML file is not found.
    """
    xml_path = JGA_STUDY_XML
    if not xml_path.exists():
        raise FileNotFoundError(f"XML file not found: {xml_path}")

    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    # STUDY 要素を取得
    studies: list[dict[str, Any]] = []
    for study_elem in root.findall("STUDY"):
        result = _element_to_dict(study_elem)
        if isinstance(result, dict):
            studies.append(result)

    return studies


def _load_jga_humandbs_file(
    config: Config,
    rel_path: str,
    src_type: AccessionType,
) -> IdPairs:
    """humandbs TSV から JGA accession -> humandbs 関連を読み込む。

    TSV format: jga_accession\thumandbs (ヘッダなし)

    Raises:
        FileNotFoundError: ファイルが存在しない場合。
    """
    path = config.const_dir.joinpath(rel_path)
    if not path.exists():
        raise FileNotFoundError(f"humandbs file not found: {path}")

    pairs: IdPairs = set()
    log_info(f"processing humandbs file: {path}", file=str(path))

    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                log_debug(
                    f"skipping malformed line: {line}",
                    file=str(path),
                    debug_category=DebugCategory.INVALID_ACCESSION_ID,
                    source="jga-humandbs",
                )
                continue
            src_acc, humandbs = parts[0], parts[1]
            if not is_valid_accession(src_acc, src_type):
                log_debug(
                    f"skipping invalid {src_type}: {src_acc}",
                    accession=src_acc,
                    file=str(path),
                    debug_category=DebugCategory.INVALID_ACCESSION_ID,
                    source="jga-humandbs",
                )
                continue
            if not is_valid_accession(humandbs, "humandbs"):
                log_debug(
                    f"skipping invalid humandbs: {humandbs}",
                    accession=humandbs,
                    file=str(path),
                    debug_category=DebugCategory.INVALID_ACCESSION_ID,
                    source="jga-humandbs",
                )
                continue
            pairs.add((src_acc, humandbs))

    log_info(
        f"loaded {len(pairs)} {src_type} -> humandbs from humandbs file",
        file=str(path),
    )

    return pairs


def extract_pubmed_ids(study_entry: dict[str, Any]) -> set[str]:
    """PUBLICATIONS から PUBMED ID を抽出する。"""
    pubs = (study_entry.get("PUBLICATIONS") or {}).get("PUBLICATION", [])

    # Ensure it's a list (single publication case)
    if isinstance(pubs, dict):
        pubs = [pubs]

    pubmed_ids: set[str] = set()
    for pub in pubs:
        if pub.get("DB_TYPE") == "PUBMED":
            pub_id = pub.get("id")
            if pub_id:
                pubmed_ids.add(str(pub_id))

    return pubmed_ids


def extract_jga_study_pubmed_ids() -> IdPairs:
    """jga-study.xml を処理して pubmed の関連を返す。"""
    study_to_pubmed: IdPairs = set()

    xml_file = str(JGA_STUDY_XML)
    studies = load_jga_study_xml()
    for study in studies:
        accession = study.get("accession")
        if not accession:
            continue

        if not is_valid_accession(accession, "jga-study"):
            log_debug(
                f"skipping invalid jga-study: {accession}",
                accession=accession,
                file=xml_file,
                debug_category=DebugCategory.INVALID_ACCESSION_ID,
                source="jga",
            )
            continue

        pubmed_ids = extract_pubmed_ids(study)
        for pub_id in pubmed_ids:
            if is_valid_accession(pub_id, "pubmed"):
                study_to_pubmed.add((accession, pub_id))
            else:
                log_debug(
                    f"skipping invalid pubmed: {pub_id}",
                    accession=pub_id,
                    file=xml_file,
                    debug_category=DebugCategory.INVALID_ACCESSION_ID,
                    source="jga",
                )

    return study_to_pubmed


# === Main ===


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        # Blacklist を読み込む
        jga_blacklist = load_jga_blacklist(config)

        # Load humandbs from TSV
        study_to_humandbs = _load_jga_humandbs_file(config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")
        dataset_to_humandbs = _load_jga_humandbs_file(config, JGA_DATASET_HUM_ID_REL_PATH, "jga-dataset")

        # Extract pubmed from XML
        study_to_pubmed = extract_jga_study_pubmed_ids()
        log_info(f"extracted {len(study_to_pubmed)} JGA study -> pubmed relations")

        # Blacklist でフィルタ
        study_to_humandbs = filter_sra_pairs_by_blacklist(study_to_humandbs, jga_blacklist)
        dataset_to_humandbs = filter_sra_pairs_by_blacklist(dataset_to_humandbs, jga_blacklist)
        study_to_pubmed = filter_sra_pairs_by_blacklist(study_to_pubmed, jga_blacklist)

        # Build JGA internal relations from CSV
        internal_relations = build_jga_internal_relations()
        for name, rel in internal_relations.items():
            log_info(f"built {len(rel)} JGA {name} relations")

        # Blacklist でフィルタ
        for name in internal_relations:
            internal_relations[name] = filter_sra_pairs_by_blacklist(internal_relations[name], jga_blacklist)

        # Load to DB: humandbs (from TSV)
        if study_to_humandbs:
            load_to_db(config, study_to_humandbs, "jga-study", "humandbs")
        if dataset_to_humandbs:
            load_to_db(config, dataset_to_humandbs, "jga-dataset", "humandbs")

        # Load to DB: pubmed (from XML)
        if study_to_pubmed:
            load_to_db(config, study_to_pubmed, "jga-study", "pubmed")

        # Load to DB: CSV-based internal relations
        if internal_relations["dataset_policy"]:
            load_to_db(config, internal_relations["dataset_policy"], "jga-dataset", "jga-policy")

        if internal_relations["policy_dac"]:
            load_to_db(config, internal_relations["policy_dac"], "jga-policy", "jga-dac")

        if internal_relations["dataset_study"]:
            # Reverse to get study -> dataset
            study_to_dataset = reverse_relation(internal_relations["dataset_study"])
            load_to_db(config, study_to_dataset, "jga-study", "jga-dataset")

        if internal_relations["dataset_dac"]:
            load_to_db(config, internal_relations["dataset_dac"], "jga-dataset", "jga-dac")

        if internal_relations["study_dac"]:
            load_to_db(config, internal_relations["study_dac"], "jga-study", "jga-dac")

        if internal_relations["study_policy"]:
            load_to_db(config, internal_relations["study_policy"], "jga-study", "jga-policy")


if __name__ == "__main__":
    main()
