"""特定の accession ID を指定して JSONL を再生成するモジュール。

hotfix/debug 用途。通常パイプライン (full/incremental) とは独立し、
last_run.json は更新しない。
"""
import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ddbj_search_converter.config import (BP_BASE_DIR_NAME, BS_BASE_DIR_NAME,
                                          JGA_BASE_PATH, TMP_XML_DIR_NAME,
                                          TODAY_STR, Config, get_config)
from ddbj_search_converter.dblink.db import AccessionType
from ddbj_search_converter.dblink.utils import (load_blacklist,
                                                load_jga_blacklist,
                                                load_sra_blacklist)
from ddbj_search_converter.id_patterns import ID_PATTERN_MAP
from ddbj_search_converter.jsonl.bp import \
    _fetch_dates_ddbj as bp_fetch_dates_ddbj
from ddbj_search_converter.jsonl.bp import \
    _fetch_dates_ncbi as bp_fetch_dates_ncbi
from ddbj_search_converter.jsonl.bp import xml_entry_to_bp_instance
from ddbj_search_converter.jsonl.bs import \
    _fetch_dates_ddbj as bs_fetch_dates_ddbj
from ddbj_search_converter.jsonl.bs import \
    _fetch_dates_ncbi as bs_fetch_dates_ncbi
from ddbj_search_converter.jsonl.bs import xml_entry_to_bs_instance
from ddbj_search_converter.jsonl.jga import (INDEX_TO_ACCESSION_TYPE, XML_KEYS,
                                             IndexName,
                                             jga_entry_to_jga_instance,
                                             load_date_map, load_jga_xml)
from ddbj_search_converter.jsonl.sra import XML_TYPES, process_submission_xml
from ddbj_search_converter.jsonl.utils import get_dbxref_map, write_jsonl
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger
from ddbj_search_converter.schema import XrefType
from ddbj_search_converter.sra.tar_reader import (SraXmlType,
                                                  get_dra_tar_reader,
                                                  get_ncbi_tar_reader)
from ddbj_search_converter.sra_accessions_tab import (
    SourceKind, get_accession_info_bulk, lookup_submissions_for_accessions)
from ddbj_search_converter.xml_utils import iterate_xml_element, parse_xml

# type ごとに受け入れる accession パターンを定義
TYPE_PATTERNS: Dict[str, List[AccessionType]] = {
    "bioproject": ["bioproject", "umbrella-bioproject"],
    "biosample": ["biosample"],
    "sra": [
        "sra-submission", "sra-study", "sra-experiment",
        "sra-run", "sra-sample", "sra-analysis",
    ],
    "jga": ["jga-study", "jga-dataset", "jga-dac", "jga-policy"],
}


def load_accessions_from_file(file_path: Path) -> Set[str]:
    """ファイルから accession ID を読み込む (1行1件、空行・#コメント除外)。"""
    accessions: Set[str] = set()
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                accessions.add(line)
    return accessions


def validate_accessions(data_type: str, accessions: Set[str]) -> Set[str]:
    """type に合わない accession を警告ログに出力する。処理は続行する。"""
    valid_types = TYPE_PATTERNS.get(data_type, [])
    valid: Set[str] = set()
    for acc in accessions:
        matched = False
        for acc_type in valid_types:
            pattern = ID_PATTERN_MAP.get(acc_type)
            if pattern and pattern.match(acc):
                matched = True
                break
        if matched:
            valid.add(acc)
        else:
            log_warn(
                f"accession '{acc}' does not match expected patterns for type '{data_type}', skipping",
                accession=acc,
            )
    return valid


# === BioProject ===


def regenerate_bp_jsonl(
    config: Config,
    tmp_xml_dir: Path,
    output_dir: Path,
    target_accessions: Set[str],
) -> None:
    """指定された accession の BioProject JSONL を再生成する。"""
    if not tmp_xml_dir.exists():
        raise FileNotFoundError(f"tmp_xml directory not found: {tmp_xml_dir}")

    bp_blacklist, _ = load_blacklist(config)

    # DDBJ XML + NCBI XML の全ファイルを処理
    xml_files = sorted(
        list(tmp_xml_dir.glob("ddbj_*.xml")) + list(tmp_xml_dir.glob("ncbi_*.xml"))
    )
    log_info(f"found {len(xml_files)} xml files in {tmp_xml_dir}")

    docs: Dict[str, Any] = {}
    found_accessions: Set[str] = set()

    for xml_path in xml_files:
        is_ddbj = xml_path.name.startswith("ddbj_")
        for xml_element in iterate_xml_element(xml_path, "Package"):
            try:
                metadata = parse_xml(xml_element)
                bp_instance = xml_entry_to_bp_instance(metadata["Package"], is_ddbj)

                if bp_instance.identifier not in target_accessions:
                    continue
                if bp_instance.identifier in bp_blacklist:
                    log_warn(f"accession {bp_instance.identifier} is in blacklist, skipping")
                    continue

                docs[bp_instance.identifier] = bp_instance
                found_accessions.add(bp_instance.identifier)
            except Exception as e:
                log_warn(f"failed to parse xml element: {e}", file=str(xml_path))

    not_found = target_accessions - found_accessions
    if not_found:
        log_warn(f"{len(not_found)} accession(s) not found in xml files: {sorted(not_found)}")

    if not docs:
        log_info("no entries found, skipping output")
        return

    # dbXrefs
    dbxref_map = get_dbxref_map(config, "bioproject", list(docs.keys()))
    for accession, xrefs in dbxref_map.items():
        if accession in docs:
            docs[accession].dbXrefs = xrefs

    # 日付取得: DDBJ と NCBI の docs を分けて処理
    ddbj_docs = {acc: doc for acc, doc in docs.items() if acc.startswith("PRJD")}
    ncbi_docs = {acc: doc for acc, doc in docs.items() if not acc.startswith("PRJD")}

    if ddbj_docs:
        bp_fetch_dates_ddbj(config, ddbj_docs)
    if ncbi_docs:
        # NCBI の日付は XML から取得するため、全 NCBI XML を再走査
        for xml_path in sorted(tmp_xml_dir.glob("ncbi_*.xml")):
            bp_fetch_dates_ncbi(xml_path, ncbi_docs)

    output_path = output_dir / "bioproject.jsonl"
    write_jsonl(output_path, list(docs.values()))
    log_info(f"wrote {len(docs)} bioproject entries to {output_path}")


# === BioSample ===


def regenerate_bs_jsonl(
    config: Config,
    tmp_xml_dir: Path,
    output_dir: Path,
    target_accessions: Set[str],
) -> None:
    """指定された accession の BioSample JSONL を再生成する。"""
    if not tmp_xml_dir.exists():
        raise FileNotFoundError(f"tmp_xml directory not found: {tmp_xml_dir}")

    _, bs_blacklist = load_blacklist(config)

    xml_files = sorted(
        list(tmp_xml_dir.glob("ddbj_*.xml")) + list(tmp_xml_dir.glob("ncbi_*.xml"))
    )
    log_info(f"found {len(xml_files)} xml files in {tmp_xml_dir}")

    docs: Dict[str, Any] = {}
    found_accessions: Set[str] = set()

    for xml_path in xml_files:
        is_ddbj = xml_path.name.startswith("ddbj_")
        for xml_element in iterate_xml_element(xml_path, "BioSample"):
            try:
                metadata = parse_xml(xml_element)
                bs_instance = xml_entry_to_bs_instance(metadata, is_ddbj)

                if bs_instance.identifier not in target_accessions:
                    continue
                if bs_instance.identifier in bs_blacklist:
                    log_warn(f"accession {bs_instance.identifier} is in blacklist, skipping")
                    continue

                docs[bs_instance.identifier] = bs_instance
                found_accessions.add(bs_instance.identifier)
            except Exception as e:
                log_warn(f"failed to parse xml element: {e}", file=str(xml_path))

    not_found = target_accessions - found_accessions
    if not_found:
        log_warn(f"{len(not_found)} accession(s) not found in xml files: {sorted(not_found)}")

    if not docs:
        log_info("no entries found, skipping output")
        return

    # dbXrefs
    dbxref_map = get_dbxref_map(config, "biosample", list(docs.keys()))
    for accession, xrefs in dbxref_map.items():
        if accession in docs:
            docs[accession].dbXrefs = xrefs

    # 日付取得
    ddbj_docs = {acc: doc for acc, doc in docs.items() if acc.startswith("SAMD")}
    ncbi_docs = {acc: doc for acc, doc in docs.items() if not acc.startswith("SAMD")}

    if ddbj_docs:
        bs_fetch_dates_ddbj(config, ddbj_docs)
    if ncbi_docs:
        for xml_path in sorted(tmp_xml_dir.glob("ncbi_*.xml")):
            is_ddbj = False
            bs_fetch_dates_ncbi(xml_path, ncbi_docs, is_ddbj)

    output_path = output_dir / "biosample.jsonl"
    write_jsonl(output_path, list(docs.values()))
    log_info(f"wrote {len(docs)} biosample entries to {output_path}")


# === SRA ===


def _classify_sra_source(accession: str) -> Optional[SourceKind]:
    """SRA accession のプレフィックスから source を判定する。"""
    if not accession:
        return None
    prefix = accession[0].upper()
    if prefix == "D":
        return "dra"
    if prefix in ("S", "E"):
        return "sra"
    return None


def regenerate_sra_jsonl(
    config: Config,
    output_dir: Path,
    target_accessions: Set[str],
) -> None:
    """指定された accession の SRA JSONL を再生成する。"""
    blacklist = load_sra_blacklist(config)

    # source ごとに accession を分類
    source_accessions: Dict[SourceKind, List[str]] = {"dra": [], "sra": []}
    for acc in target_accessions:
        source = _classify_sra_source(acc)
        if source:
            source_accessions[source].append(acc)
        else:
            log_warn(f"cannot determine source for accession '{acc}', skipping")

    # entity type 別に結果を集約
    all_entries: Dict[str, List[Any]] = {t: [] for t in XML_TYPES}

    source_kind: SourceKind
    for source_kind in ("dra", "sra"):
        accs = source_accessions[source_kind]
        if not accs:
            continue

        is_dra = source_kind == "dra"
        log_info(f"processing {source_kind.upper()}: {len(accs)} accession(s)")

        # accession → submission を逆引き
        acc_to_sub = lookup_submissions_for_accessions(config, source_kind, accs)

        not_found = set(accs) - set(acc_to_sub.keys())
        if not_found:
            log_warn(f"{len(not_found)} accession(s) not found in {source_kind.upper()} accessions DB: {sorted(not_found)}")

        if not acc_to_sub:
            continue

        # submission の重複を除去
        submissions = sorted(set(acc_to_sub.values()))
        log_info(f"resolved to {len(submissions)} unique submission(s)")

        # tar reader を取得
        if is_dra:
            tar_reader = get_dra_tar_reader(config)
        else:
            tar_reader = get_ncbi_tar_reader(config)

        for sub in submissions:
            # submission に含まれる全 XML を読み込む
            xml_cache: Dict[SraXmlType, Optional[bytes]] = {}
            for xml_type in XML_TYPES:
                xml_cache[xml_type] = tar_reader.read_xml(sub, xml_type)

            # submission に含まれる全 accession を収集
            sub_accessions: List[str] = [sub]
            for xml_type in XML_TYPES:
                if xml_type == "submission":
                    continue
                xml_bytes = xml_cache.get(xml_type)
                if xml_bytes:
                    try:
                        parsed = parse_xml(xml_bytes)
                        set_key = f"{xml_type.upper()}_SET"
                        entry_key = xml_type.upper()
                        entries_data = (parsed.get(set_key) or {}).get(entry_key)
                        if entries_data:
                            if not isinstance(entries_data, list):
                                entries_data = [entries_data]
                            for entry_data in entries_data:
                                acc = entry_data.get("accession")
                                if acc:
                                    sub_accessions.append(acc)
                    except Exception:
                        pass

            # accession info を取得
            accession_info = get_accession_info_bulk(config, source_kind, sub_accessions)

            # submission を処理
            results = process_submission_xml(
                submission=sub,
                blacklist=blacklist,
                accession_info=accession_info,
                is_dra=is_dra,
                xml_cache=xml_cache,
            )

            # target_accessions に含まれるもののみフィルタ
            for xml_type in XML_TYPES:
                for entry in results[xml_type]:
                    if entry.identifier in target_accessions:
                        all_entries[xml_type].append(entry)

        tar_reader.close()

    # dbXrefs を更新
    xref_type_map: Dict[SraXmlType, XrefType] = {
        "submission": "sra-submission",
        "study": "sra-study",
        "experiment": "sra-experiment",
        "run": "sra-run",
        "sample": "sra-sample",
        "analysis": "sra-analysis",
    }
    for xml_type in XML_TYPES:
        entity_type = xref_type_map[xml_type]
        accessions = [e.identifier for e in all_entries[xml_type]]
        if accessions:
            dbxref_map = get_dbxref_map(config, entity_type, accessions)
            for entry in all_entries[xml_type]:
                if entry.identifier in dbxref_map:
                    entry.dbXrefs = dbxref_map[entry.identifier]

    # JSONL を出力 (空の場合はファイル作成しない)
    for xml_type in XML_TYPES:
        if all_entries[xml_type]:
            output_path = output_dir / f"{xml_type}.jsonl"
            write_jsonl(output_path, all_entries[xml_type])
            log_info(f"wrote {len(all_entries[xml_type])} {xml_type} entries to {output_path}")

    total = sum(len(v) for v in all_entries.values())
    if total == 0:
        log_info("no sra entries found, no files written")
    else:
        log_info(f"wrote {total} sra entries in total")


# === JGA ===

# accession prefix → IndexName
JGA_PREFIX_MAP: Dict[str, IndexName] = {
    "JGAS": "jga-study",
    "JGAD": "jga-dataset",
    "JGAC": "jga-dac",
    "JGAP": "jga-policy",
}


def regenerate_jga_jsonl(
    config: Config,
    output_dir: Path,
    jga_base_path: Path,
    target_accessions: Set[str],
) -> None:
    """指定された accession の JGA JSONL を再生成する。"""
    jga_blacklist = load_jga_blacklist(config)

    # 必要な index_name を判定
    needed_indexes: Dict[IndexName, Set[str]] = {}
    for acc in target_accessions:
        prefix = acc[:4]
        index_name = JGA_PREFIX_MAP.get(prefix)
        if index_name:
            needed_indexes.setdefault(index_name, set()).add(acc)
        else:
            log_warn(f"cannot determine JGA index for accession '{acc}', skipping")

    for index_name, accs in needed_indexes.items():
        xml_path = jga_base_path / f"{index_name}.xml"
        if not xml_path.exists():
            log_warn(f"XML file not found: {xml_path}, skipping {index_name}")
            continue

        log_info(f"processing {index_name}: {len(accs)} accession(s)")

        xml_metadata = load_jga_xml(xml_path)

        root_key, entry_key = XML_KEYS[index_name]
        try:
            entries = xml_metadata[root_key][entry_key]
            if index_name == "jga-dac":
                entries = [entries]
            if not isinstance(entries, list):
                entries = [entries]
        except Exception as e:
            log_warn(f"failed to parse XML for {index_name}: {e}")
            continue

        # accession でフィルタ
        jga_instances: Dict[str, Any] = {}
        for entry in entries:
            jga_instance = jga_entry_to_jga_instance(entry, index_name)
            if jga_instance.identifier in accs:
                # blacklist チェック
                if jga_instance.identifier in jga_blacklist:
                    log_warn(f"accession {jga_instance.identifier} is in blacklist, skipping")
                    continue
                jga_instances[jga_instance.identifier] = jga_instance

        not_found = accs - set(jga_instances.keys())
        if not_found:
            log_warn(f"{len(not_found)} accession(s) not found in {index_name} xml: {sorted(not_found)}")

        if not jga_instances:
            continue

        # dbXrefs
        accession_list = list(jga_instances.keys())
        dbxref_map = get_dbxref_map(config, INDEX_TO_ACCESSION_TYPE[index_name], accession_list)
        for accession, xrefs in dbxref_map.items():
            if accession in jga_instances:
                jga_instances[accession].dbXrefs = xrefs

        # 日付
        try:
            date_map = load_date_map(jga_base_path, index_name)
            for accession, (date_created, date_published, date_modified) in date_map.items():
                if accession in jga_instances:
                    jga_instances[accession].dateCreated = date_created
                    jga_instances[accession].datePublished = date_published
                    jga_instances[accession].dateModified = date_modified
        except FileNotFoundError as e:
            log_warn(f"date csv not found: {e}")

        output_path = output_dir / f"{index_name}.jsonl"
        write_jsonl(output_path, list(jga_instances.values()))
        log_info(f"wrote {len(jga_instances)} {index_name} entries to {output_path}")


# === CLI ===

REGENERATE_DIR_NAME = "regenerate"


def parse_args(args: List[str]) -> argparse.Namespace:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Regenerate JSONL for specific accession IDs (hotfix/debug use)."
    )
    parser.add_argument(
        "--type",
        required=True,
        choices=["bioproject", "biosample", "sra", "jga"],
        help="Data type to regenerate.",
    )
    parser.add_argument(
        "--accessions",
        nargs="+",
        default=None,
        help="Accession IDs (space-separated).",
    )
    parser.add_argument(
        "--accession-file",
        default=None,
        help="Path to a file with one accession per line.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory. Default: {result_dir}/regenerate/{date}/",
    )

    parsed = parser.parse_args(args)

    if parsed.accessions is None and parsed.accession_file is None:
        parser.error("at least one of --accessions or --accession-file is required")

    return parsed


def main() -> None:
    """CLI エントリポイント。"""
    parsed = parse_args(sys.argv[1:])

    config = get_config()

    data_type: str = parsed.type

    # accession を収集
    accessions: Set[str] = set()
    if parsed.accessions:
        accessions.update(parsed.accessions)
    if parsed.accession_file:
        accessions.update(load_accessions_from_file(Path(parsed.accession_file)))

    with run_logger(run_name="regenerate_jsonl", config=config):
        log_info(f"regenerate_jsonl: type={data_type}, accessions={len(accessions)}")

        # バリデーション
        accessions = validate_accessions(data_type, accessions)
        if not accessions:
            log_warn("no valid accessions to process")
            return

        log_info(f"target accessions ({len(accessions)}): {sorted(accessions)}")

        # 出力ディレクトリ
        if parsed.output_dir:
            output_dir = Path(parsed.output_dir)
        else:
            output_dir = config.result_dir / REGENERATE_DIR_NAME / TODAY_STR
        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"output directory: {output_dir}")

        if data_type == "bioproject":
            bp_base_dir = config.result_dir / BP_BASE_DIR_NAME
            tmp_xml_dir = bp_base_dir / TMP_XML_DIR_NAME / TODAY_STR
            regenerate_bp_jsonl(config, tmp_xml_dir, output_dir, accessions)

        elif data_type == "biosample":
            bs_base_dir = config.result_dir / BS_BASE_DIR_NAME
            tmp_xml_dir = bs_base_dir / TMP_XML_DIR_NAME / TODAY_STR
            regenerate_bs_jsonl(config, tmp_xml_dir, output_dir, accessions)

        elif data_type == "sra":
            regenerate_sra_jsonl(config, output_dir, accessions)

        elif data_type == "jga":
            regenerate_jga_jsonl(config, output_dir, JGA_BASE_PATH, accessions)

        log_info("regenerate_jsonl completed")


if __name__ == "__main__":
    main()
