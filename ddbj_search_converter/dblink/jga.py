"""
JGA (Japanese Genotype-phenotype Archive) の XML/CSV から関連を抽出し、DBLink DB に挿入する。

入力:
- JGA_BASE_PATH/jga-study.xml
    - STUDY_ATTRIBUTES/STUDY_ATTRIBUTE: TAG=NBDC Number -> hum-id
    - PUBLICATIONS/PUBLICATION: id 属性 -> pubmed-id
- JGA_BASE_PATH/*-relation.csv
    - JGA 内部関連 (dataset/study) を構築

出力:
- jga-study -> hum-id
- jga-study -> pubmed-id
- jga-study -> jga-dataset
"""
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import xmltodict

from ddbj_search_converter.config import (JGA_ANALYSIS_STUDY_CSV,
                                          JGA_DATA_EXPERIMENT_CSV,
                                          JGA_DATASET_ANALYSIS_CSV,
                                          JGA_DATASET_DATA_CSV,
                                          JGA_EXPERIMENT_STUDY_CSV,
                                          JGA_STUDY_XML, get_config)
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger

# === CSV relation operations ===


def read_relation_csv(csv_path: Path) -> Set[Tuple[str, str]]:
    """
    JGA relation CSV を読み込み、(from_id, to_id) の set を返す。
    CSV format: id, from_id, to_id (header あり、1 列目は無視)
    """
    if not csv_path.exists():
        log_warn(f"CSV file not found: {csv_path}", file=str(csv_path))
        return set()

    result: Set[Tuple[str, str]] = set()
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) < 3:
                continue
            result.add((row[1], row[2]))

    return result


def join_relations(
    ab: Set[Tuple[str, str]],
    bc: Set[Tuple[str, str]],
) -> Set[Tuple[str, str]]:
    """
    (a, b) と (b, c) を join して (a, c) を返す。
    """
    b_to_c: Dict[str, Set[str]] = defaultdict(set)
    for b, c in bc:
        b_to_c[b].add(c)

    return {(a, c) for a, b in ab for c in b_to_c.get(b, ())}


def build_study_dataset_relation() -> IdPairs:
    """
    CSV から study -> dataset 関連を構築する。

    dataset_study = (dataset_analysis JOIN analysis_study) UNION
                    (dataset_data JOIN data_experiment JOIN experiment_study)
    study_dataset = reverse(dataset_study)
    """
    # Read CSV files
    dataset_analysis = read_relation_csv(JGA_DATASET_ANALYSIS_CSV)
    analysis_study = read_relation_csv(JGA_ANALYSIS_STUDY_CSV)
    dataset_data = read_relation_csv(JGA_DATASET_DATA_CSV)
    data_experiment = read_relation_csv(JGA_DATA_EXPERIMENT_CSV)
    experiment_study = read_relation_csv(JGA_EXPERIMENT_STUDY_CSV)

    # Path 1: dataset -> analysis -> study
    path1 = join_relations(dataset_analysis, analysis_study)

    # Path 2: dataset -> data -> experiment -> study
    data_study = join_relations(data_experiment, experiment_study)
    path2 = join_relations(dataset_data, data_study)

    # Union
    dataset_study = path1 | path2

    # Reverse to get study -> dataset
    study_dataset: IdPairs = {(b, a) for a, b in dataset_study}

    return study_dataset


# === XML parsing ===


def load_jga_study_xml() -> List[Dict[str, Any]]:
    """
    jga-study.xml を読み込み、STUDY エントリのリストを返す。
    """
    xml_path = JGA_STUDY_XML
    if not xml_path.exists():
        log_warn(f"XML file not found: {xml_path}", file=str(xml_path))
        return []

    with xml_path.open("rb") as f:
        xml_data = xmltodict.parse(
            f, attr_prefix="", cdata_key="content", process_namespaces=False
        )

    study_set = xml_data.get("STUDY_SET", {})
    studies = study_set.get("STUDY", [])

    # Ensure it's a list (single entry case)
    if isinstance(studies, dict):
        studies = [studies]

    result: List[Dict[str, Any]] = studies if isinstance(studies, list) else []
    return result


def extract_hum_id(study_entry: Dict[str, Any]) -> Optional[str]:
    """
    STUDY_ATTRIBUTES から NBDC Number (hum-id) を抽出する。
    """
    attrs = study_entry.get("STUDY_ATTRIBUTES", {}).get("STUDY_ATTRIBUTE", [])

    # Ensure it's a list (single attribute case)
    if isinstance(attrs, dict):
        attrs = [attrs]

    for attr in attrs:
        if attr.get("TAG") == "NBDC Number":
            value = attr.get("VALUE")
            return str(value) if value is not None else None

    return None


def extract_pubmed_ids(study_entry: Dict[str, Any]) -> Set[str]:
    """
    PUBLICATIONS から PUBMED ID を抽出する。
    """
    pubs = study_entry.get("PUBLICATIONS", {}).get("PUBLICATION", [])

    # Ensure it's a list (single publication case)
    if isinstance(pubs, dict):
        pubs = [pubs]

    pubmed_ids: Set[str] = set()
    for pub in pubs:
        if pub.get("DB_TYPE") == "PUBMED":
            pub_id = pub.get("id")
            if pub_id:
                pubmed_ids.add(str(pub_id))

    return pubmed_ids


def process_jga_study_xml() -> Tuple[IdPairs, IdPairs]:
    """
    jga-study.xml を処理して hum-id と pubmed-id の関連を返す。

    Returns:
        (study_to_hum_id, study_to_pubmed_id)
    """
    study_to_hum_id: IdPairs = set()
    study_to_pubmed_id: IdPairs = set()

    studies = load_jga_study_xml()
    for study in studies:
        accession = study.get("accession")
        if not accession:
            continue

        # Extract hum-id
        hum_id = extract_hum_id(study)
        if hum_id:
            study_to_hum_id.add((accession, hum_id))

        # Extract pubmed-ids
        pubmed_ids = extract_pubmed_ids(study)
        for pub_id in pubmed_ids:
            study_to_pubmed_id.add((accession, pub_id))

    return study_to_hum_id, study_to_pubmed_id


# === Main ===


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        # Extract from XML
        study_to_hum_id, study_to_pubmed_id = process_jga_study_xml()
        log_info(f"extracted {len(study_to_hum_id)} JGA study -> hum-id relations")
        log_info(f"extracted {len(study_to_pubmed_id)} JGA study -> pubmed-id relations")

        # Build from CSV
        study_to_dataset = build_study_dataset_relation()
        log_info(f"built {len(study_to_dataset)} JGA study -> dataset relations")

        # Load to DB
        if study_to_hum_id:
            load_to_db(config, study_to_hum_id, "jga-study", "hum-id")

        if study_to_pubmed_id:
            load_to_db(config, study_to_pubmed_id, "jga-study", "pubmed-id")

        if study_to_dataset:
            load_to_db(config, study_to_dataset, "jga-study", "jga-dataset")


if __name__ == "__main__":
    main()
