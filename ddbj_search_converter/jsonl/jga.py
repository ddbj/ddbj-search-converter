"""JGA JSONL 生成モジュール。"""
import argparse
import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import xmltodict

from ddbj_search_converter.config import (JGA_BASE_PATH, JGA_JSONL_DIR_NAME,
                                          TODAY_STR, Config, get_config)
from ddbj_search_converter.dblink.db import (AccessionType,
                                             get_related_entities_bulk)
from ddbj_search_converter.jsonl.utils import to_xref
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  log_warn, run_logger)
from ddbj_search_converter.schema import JGA, Distribution, Organism, Xref

IndexName = Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"]
INDEX_NAMES: List[IndexName] = ["jga-study", "jga-dataset", "jga-dac", "jga-policy"]

XML_KEYS: Dict[IndexName, Tuple[str, str]] = {
    "jga-study": ("STUDY_SET", "STUDY"),
    "jga-dataset": ("DATASETS", "DATASET"),
    "jga-dac": ("DAC_SET", "DAC"),
    "jga-policy": ("POLICY_SET", "POLICY"),
}

# JGA type から AccessionType へのマッピング
INDEX_TO_ACCESSION_TYPE: Dict[IndexName, AccessionType] = {
    "jga-study": "jga-study",
    "jga-dataset": "jga-dataset",
    "jga-dac": "jga-dac",
    "jga-policy": "jga-policy",
}


def load_jga_xml(xml_path: Path) -> Dict[str, Any]:
    """JGA XML ファイルを読み込んでパースする。"""
    with xml_path.open("rb") as f:
        xml_bytes = f.read()
    xml_metadata: Dict[str, Any] = xmltodict.parse(
        xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
    )
    return xml_metadata


def format_date(value: Optional[str | datetime]) -> Optional[str]:
    """datetime を ISO 8601 形式の文字列に変換する。"""
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass
    return None


_TZ_FIX = re.compile(r"([+-]\d{2})$")
_FRAC_FIX = re.compile(r"(\.\d{1,6})([+-])")


def _format_date_from_csv(value: str) -> str:
    """
    CSV に入っている日付を ISO 8601 形式に変換する。

    CSV 形式例: 2014-07-07 14:00:37.208+09
    """
    fixed_value = value.strip().replace(" ", "T")
    if _TZ_FIX.search(fixed_value):
        fixed_value = _TZ_FIX.sub(r"\1:00", fixed_value)
    fixed_value = fixed_value.replace("Z", "+00:00")
    fixed_value = _FRAC_FIX.sub(
        lambda m: f"{m.group(1).ljust(7, '0')}{m.group(2)}", fixed_value
    )
    date = datetime.fromisoformat(fixed_value)
    result = format_date(date)
    if result is None:
        raise ValueError(f"Failed to format date: {value}")
    return result


def load_date_map(
    jga_base_path: Path, index_name: IndexName
) -> Dict[str, Tuple[str, str, str]]:
    """
    CSV から日付情報を読み込む。

    CSV フォーマット: accession, dateCreated, datePublished, dateModified
    戻り値: {accession: (dateCreated, datePublished, dateModified)}
    """
    type_name = index_name.replace("jga-", "")
    csv_path = jga_base_path.joinpath(f"{type_name}.date.csv")
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file for {index_name} date map does not exist: {csv_path}"
        )

    date_map: Dict[str, Tuple[str, str, str]] = {}
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if len(row) != 4:
                log_warn(f"Invalid row in date map CSV: {row}")
                continue
            accession, date_created, date_published, date_modified = row
            date_map[accession] = (
                _format_date_from_csv(date_created),
                _format_date_from_csv(date_published),
                _format_date_from_csv(date_modified),
            )

    return date_map


def extract_title(entry: Dict[str, Any], index_name: IndexName) -> Optional[str]:
    """JGA エントリからタイトルを抽出する。"""
    title: Any = None
    if index_name == "jga-study":
        title = entry.get("DESCRIPTOR", {}).get("STUDY_TITLE")
    elif index_name in ("jga-dataset", "jga-policy"):
        title = entry.get("TITLE")
    return str(title) if title is not None else None


def extract_description(entry: Dict[str, Any], index_name: IndexName) -> Optional[str]:
    """JGA エントリから説明を抽出する。"""
    description: Any = None
    if index_name == "jga-study":
        description = entry.get("DESCRIPTOR", {}).get("STUDY_ABSTRACT")
    elif index_name == "jga-dataset":
        description = entry.get("DESCRIPTION")
    return str(description) if description is not None else None


def jga_entry_to_jga_instance(entry: Dict[str, Any], index_name: IndexName) -> JGA:
    """JGA XML エントリを JGA インスタンスに変換する。"""
    accession: str = entry["accession"]
    name = entry.get("alias", accession)

    return JGA(
        identifier=accession,
        properties=entry,
        distribution=[
            Distribution(
                type="DataDownload",
                encodingFormat="JSON",
                contentUrl=f"https://ddbj.nig.ac.jp/search/entry/{index_name}/{accession}.json",
            )
        ],
        isPartOf="jga",
        type=index_name,
        name=name,
        url=f"https://ddbj.nig.ac.jp/search/entry/{index_name}/{accession}",
        organism=Organism(identifier="9606", name="Homo sapiens"),
        title=extract_title(entry, index_name),
        description=extract_description(entry, index_name),
        dbXref=[],  # 後で更新
        sameAs=[],
        status="public",
        visibility="controlled-access",
        dateCreated=None,  # 後で更新
        dateModified=None,  # 後で更新
        datePublished=None,  # 後で更新
    )


def get_dbxref_map(
    config: Config, index_name: IndexName, accessions: List[str]
) -> Dict[str, List[Xref]]:
    """
    dblink DB から関連エントリを取得し、Xref リストに変換する。

    jga-study の場合は hum-id, pubmed-id も含める。
    """
    if not accessions:
        return {}

    entity_type = INDEX_TO_ACCESSION_TYPE[index_name]
    relations = get_related_entities_bulk(
        config, entity_type=entity_type, accessions=accessions
    )

    result: Dict[str, List[Xref]] = {}
    for accession, related_list in relations.items():
        xrefs: List[Xref] = []
        for related_type, related_id in related_list:
            xref = to_xref(related_id, type_hint=related_type)  # type: ignore
            xrefs.append(xref)
        # identifier でソート
        xrefs.sort(key=lambda x: x.identifier)
        result[accession] = xrefs

    return result


def write_jsonl(output_path: Path, docs: List[JGA]) -> None:
    """JGA インスタンスのリストを JSONL ファイルに書き込む。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(doc.model_dump_json(by_alias=True) for doc in docs))


def generate_jga_jsonl(
    config: Config,
    index_name: IndexName,
    output_dir: Path,
    jga_base_path: Path,
) -> None:
    """単一の JGA タイプの JSONL ファイルを生成する。"""
    xml_path = jga_base_path.joinpath(f"{index_name}.xml")
    if not xml_path.exists():
        raise FileNotFoundError(f"XML file for {index_name} does not exist: {xml_path}")

    log_info(f"Loading XML file: {xml_path}")
    xml_metadata = load_jga_xml(xml_path)

    # XML からエントリを抽出
    root_key, entry_key = XML_KEYS[index_name]
    try:
        entries = xml_metadata[root_key][entry_key]
        if index_name == "jga-dac":
            entries = [entries]  # DAC は単一エントリなのでリストにラップ
        if not isinstance(entries, list):
            raise ValueError(f"Expected a list for {index_name}, but got: {type(entries)}")
    except Exception as e:
        raise ValueError(f"Failed to parse XML for {index_name}: {e}") from e

    log_info(f"Processing {len(entries)} entries from XML file: {xml_path}")

    # エントリを JGA インスタンスに変換
    jga_instances: Dict[str, JGA] = {}
    for entry in entries:
        jga_instance = jga_entry_to_jga_instance(entry, index_name)
        jga_instances[jga_instance.identifier] = jga_instance

    accessions = list(jga_instances.keys())

    # dbXref を取得して更新
    dbxref_map = get_dbxref_map(config, index_name, accessions)
    for accession, xrefs in dbxref_map.items():
        jga_instance = jga_instances[accession]
        jga_instance.dbXref = xrefs

    # 日付を取得して更新
    date_map = load_date_map(jga_base_path, index_name)
    for accession, (date_created, date_published, date_modified) in date_map.items():
        if accession in jga_instances:
            jga_instance = jga_instances[accession]
            jga_instance.dateCreated = date_created
            jga_instance.datePublished = date_published
            jga_instance.dateModified = date_modified

    # JSONL ファイルに出力
    output_path = output_dir.joinpath(f"{index_name}.jsonl")
    write_jsonl(output_path, list(jga_instances.values()))
    log_info(f"Wrote {len(jga_instances)} entries to JSONL file: {output_path}")


def parse_args(args: List[str]) -> Tuple[Config, Path, Path]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Generate JGA JSONL files from JGA XML files."
    )
    parser.add_argument(
        "--result-dir",
        help=f"Base directory for output. Default: $PWD/ddbj_search_converter_results. "
        f"Output will be stored in {{result_dir}}/{JGA_JSONL_DIR_NAME}/{{date}}/.",
        default=None,
    )
    parser.add_argument(
        "--jga-base-path",
        help=f"Path to JGA XML/CSV files. Default: {JGA_BASE_PATH}",
        default=None,
    )
    parser.add_argument(
        "--debug",
        help="Enable debug mode.",
        action="store_true",
    )

    parsed = parser.parse_args(args)

    config = get_config()
    if parsed.result_dir is not None:
        config.result_dir = Path(parsed.result_dir)
    if parsed.debug:
        config.debug = True

    jga_base_path = (
        Path(parsed.jga_base_path) if parsed.jga_base_path else JGA_BASE_PATH
    )

    output_dir = config.result_dir.joinpath(JGA_JSONL_DIR_NAME, TODAY_STR)

    return config, output_dir, jga_base_path


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, jga_base_path = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_jga_jsonl", config=config):
        log_debug(f"Config: {config.model_dump_json(indent=2)}")
        log_debug(f"Output directory: {output_dir}")
        log_debug(f"JGA base path: {jga_base_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"Output directory: {output_dir}")

        for index_name in INDEX_NAMES:
            generate_jga_jsonl(config, index_name, output_dir, jga_base_path)


if __name__ == "__main__":
    main()
