"""SRA JSONL 生成モジュール。

DRA (DDBJ) と NCBI SRA の XML を tar ファイルから読み込み、
6 種類 (submission, study, experiment, run, sample, analysis) の JSONL を生成する。
"""
import argparse
import sys
from pathlib import Path
from typing import (Any, Callable, Dict, List, Literal, Optional, Set, Tuple,
                    cast)

from ddbj_search_converter.config import (JSONL_DIR_NAME, SRA_BASE_DIR_NAME,
                                          TODAY_STR, Config, get_config,
                                          read_last_run, write_last_run)
from ddbj_search_converter.dblink.utils import load_sra_blacklist
from ddbj_search_converter.jsonl.utils import get_dbxref_map, write_jsonl
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  log_warn, run_logger)
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.schema import (SRA, Accessibility, Distribution,
                                          Organism, Status, XrefType)
from ddbj_search_converter.sra.tar_reader import (SraXmlType, TarXMLReader,
                                                  get_dra_tar_reader,
                                                  get_ncbi_tar_reader)
from ddbj_search_converter.sra_accessions_tab import (SourceKind,
                                                      get_accession_info_bulk,
                                                      iter_all_submissions,
                                                      iter_updated_submissions)
from ddbj_search_converter.xml_utils import parse_xml

DEFAULT_BATCH_SIZE = 1000
DEFAULT_PARALLEL_NUM = 8

# XML types
XML_TYPES: List[SraXmlType] = [
    "submission", "study", "experiment", "run", "sample", "analysis"
]

# SRA type -> entry type
SraEntryType = Literal["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"]

SRA_TYPE_MAP: Dict[SraXmlType, SraEntryType] = {
    "submission": "sra-submission",
    "study": "sra-study",
    "experiment": "sra-experiment",
    "run": "sra-run",
    "sample": "sra-sample",
    "analysis": "sra-analysis",
}


# === Parse functions ===


def _get_entries(parsed: Dict[str, Any], set_key: str, entry_key: str) -> List[Dict[str, Any]]:
    """SET から entry のリストを取得する。"""
    entry_set = parsed.get(set_key) or {}
    entries = entry_set.get(entry_key)
    if entries is None:
        return []
    if not isinstance(entries, list):
        return [entries]
    return entries


def _get_text(d: Any, key: str) -> Optional[str]:
    """辞書から文字列を取得する。"""
    if d is None or not isinstance(d, dict):
        return None
    v = d.get(key)
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return v.get("content")
    return str(v)


def _normalize_status(status: Optional[str]) -> Status:
    """
    status を INSDC 標準に正規化する。

    入力値 -> 出力値:
    - live -> live
    - unpublished -> unpublished
    - suppressed -> suppressed
    - withdrawn -> withdrawn
    - public -> live (DRA 互換)
    - replaced -> withdrawn (旧値互換)
    - killed -> withdrawn (旧値互換)
    - NULL/その他 -> live (デフォルト)
    """
    if status is None:
        return "live"
    status_lower = status.lower()
    if status_lower in ("live", "unpublished", "suppressed", "withdrawn"):
        return cast(Status, status_lower)
    if status_lower == "public":
        return "live"
    if status_lower in ("replaced", "killed"):
        return "withdrawn"
    return "live"


def _normalize_accessibility(accessibility: Optional[str]) -> Accessibility:
    """
    accessibility を正規化する。

    入力値 -> 出力値:
    - public -> public-access
    - controlled -> controlled-access (BioSample 互換)
    - controlled-access -> controlled-access
    - controlled_access -> controlled-access (アンダースコア互換)
    - NULL/その他 -> public-access (デフォルト)
    """
    if accessibility is None:
        return "public-access"
    accessibility_lower = accessibility.lower().replace("_", "-")
    if accessibility_lower == "controlled-access":
        return "controlled-access"
    if accessibility_lower == "controlled":
        return "controlled-access"
    return "public-access"


def parse_submission(
    xml_bytes: bytes,
    accession: str,
) -> Optional[Dict[str, Any]]:
    """submission XML をパースする。"""
    try:
        parsed = parse_xml(xml_bytes)
        submission = parsed.get("SUBMISSION")
        if submission is None:
            return None

        return {
            "accession": submission.get("accession"),
            "title": _get_text(submission, "TITLE"),
            "description": submission.get("submission_comment") or None,
            "properties": parsed,
        }
    except Exception as e:
        log_warn(f"failed to parse submission xml: {e}", accession=accession)
        return None


def parse_study(
    xml_bytes: bytes,
    accession: str,
) -> List[Dict[str, Any]]:
    """study XML をパースする。複数の STUDY を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = parse_xml(xml_bytes)
        studies = _get_entries(parsed, "STUDY_SET", "STUDY")

        for study in studies:
            descriptor = (study.get("DESCRIPTOR") or {})
            results.append({
                "accession": study.get("accession"),
                "title": _get_text(descriptor, "STUDY_TITLE"),
                "description": _get_text(descriptor, "STUDY_ABSTRACT") or _get_text(descriptor, "STUDY_DESCRIPTION"),
                "properties": {"STUDY_SET": {"STUDY": study}},
            })
    except Exception as e:
        log_warn(f"failed to parse study xml: {e}", accession=accession)
    return results


def parse_experiment(
    xml_bytes: bytes,
    accession: str,
) -> List[Dict[str, Any]]:
    """experiment XML をパースする。複数の EXPERIMENT を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = parse_xml(xml_bytes)
        experiments = _get_entries(parsed, "EXPERIMENT_SET", "EXPERIMENT")

        for exp in experiments:
            design = (exp.get("DESIGN") or {})
            results.append({
                "accession": exp.get("accession"),
                "title": _get_text(exp, "TITLE"),
                "description": _get_text(design, "DESIGN_DESCRIPTION"),
                "properties": {"EXPERIMENT_SET": {"EXPERIMENT": exp}},
            })
    except Exception as e:
        log_warn(f"failed to parse experiment xml: {e}", accession=accession)
    return results


def parse_run(
    xml_bytes: bytes,
    accession: str,
) -> List[Dict[str, Any]]:
    """run XML をパースする。複数の RUN を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = parse_xml(xml_bytes)
        runs = _get_entries(parsed, "RUN_SET", "RUN")

        for run in runs:
            results.append({
                "accession": run.get("accession"),
                "title": _get_text(run, "TITLE"),
                "description": None,
                "properties": {"RUN_SET": {"RUN": run}},
            })
    except Exception as e:
        log_warn(f"failed to parse run xml: {e}", accession=accession)
    return results


def parse_sample(
    xml_bytes: bytes,
    accession: str,
) -> List[Dict[str, Any]]:
    """sample XML をパースする。複数の SAMPLE を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = parse_xml(xml_bytes)
        samples = _get_entries(parsed, "SAMPLE_SET", "SAMPLE")

        for sample in samples:
            sample_name = (sample.get("SAMPLE_NAME") or {})
            organism = None
            tax_id = sample_name.get("TAXON_ID")
            sci_name = sample_name.get("SCIENTIFIC_NAME")
            if tax_id or sci_name:
                organism = Organism(
                    identifier=str(tax_id) if tax_id else None,
                    name=sci_name,
                )

            results.append({
                "accession": sample.get("accession"),
                "title": _get_text(sample, "TITLE"),
                "description": _get_text(sample, "DESCRIPTION"),
                "organism": organism,
                "properties": {"SAMPLE_SET": {"SAMPLE": sample}},
            })
    except Exception as e:
        log_warn(f"failed to parse sample xml: {e}", accession=accession)
    return results


def parse_analysis(
    xml_bytes: bytes,
    accession: str,
) -> List[Dict[str, Any]]:
    """analysis XML をパースする。複数の ANALYSIS を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = parse_xml(xml_bytes)
        analyses = _get_entries(parsed, "ANALYSIS_SET", "ANALYSIS")

        for analysis in analyses:
            results.append({
                "accession": analysis.get("accession"),
                "title": _get_text(analysis, "TITLE"),
                "description": _get_text(analysis, "DESCRIPTION"),
                "properties": {"ANALYSIS_SET": {"ANALYSIS": analysis}},
            })
    except Exception as e:
        log_warn(f"failed to parse analysis xml: {e}", accession=accession)
    return results


# === Model creation ===


def _make_distribution(entry_type: str, identifier: str) -> List[Distribution]:
    """Distribution を作成する。"""
    return [Distribution(
        type="DataDownload",
        encodingFormat="JSON",
        contentUrl=f"https://ddbj.nig.ac.jp/search/entries/{entry_type}/{identifier}.json",
    )]


def _make_url(entry_type: str, identifier: str) -> str:
    """URL を作成する。"""
    return f"https://ddbj.nig.ac.jp/search/entries/{entry_type}/{identifier}"


def create_sra_entry(
    sra_type: SraXmlType,
    parsed: Dict[str, Any],
    status: Status,
    accessibility: Accessibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SRA:
    """SRA エントリを作成する。"""
    entry_type = SRA_TYPE_MAP[sra_type]
    identifier = parsed["accession"]
    return SRA(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution(entry_type, identifier),
        isPartOf="sra",
        type=entry_type,
        name=None,
        url=_make_url(entry_type, identifier),
        organism=parsed.get("organism"),
        title=parsed.get("title"),
        description=parsed.get("description"),
        dbXrefs=[],
        sameAs=[],
        downloadUrl=[],
        status=status,
        accessibility=accessibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


# === Submission processing ===

# parse 関数ディスパッチ
_PARSE_FNS: Dict[SraXmlType, Callable[..., Any]] = {
    "submission": parse_submission,
    "study": parse_study,
    "experiment": parse_experiment,
    "run": parse_run,
    "sample": parse_sample,
    "analysis": parse_analysis,
}


def process_submission_xml(
    tar_reader: TarXMLReader,
    submission: str,
    _config: Config,
    blacklist: Set[str],
    accession_info: Dict[str, Tuple[str, str, Optional[str], Optional[str], Optional[str], str]],
) -> Dict[SraXmlType, List[Any]]:
    """
    1つの submission から全 XML タイプを処理する。

    Args:
        tar_reader: TarXMLReader
        submission: submission accession
        _config: Config (現在未使用、将来の拡張用)
        blacklist: blacklist
        accession_info: {accession: (status, accessibility, received, updated, published, type)}

    Returns:
        {xml_type: [model_instance, ...]}
    """
    results: Dict[SraXmlType, List[Any]] = {t: [] for t in XML_TYPES}

    for xml_type in XML_TYPES:
        xml_bytes = tar_reader.read_xml(submission, xml_type)
        if not xml_bytes:
            continue

        parse_fn = _PARSE_FNS[xml_type]

        if xml_type == "submission":
            parsed = parse_fn(xml_bytes, submission)
            parsed_list = [parsed] if parsed and parsed.get("accession") else []
        else:
            parsed_list = parse_fn(xml_bytes, submission)

        for entry in parsed_list:
            acc = entry.get("accession")
            if not acc or acc in blacklist:
                continue
            info = accession_info.get(acc, ("live", "public", None, None, None, ""))
            status = _normalize_status(info[0])
            accessibility = _normalize_accessibility(info[1])
            sra_entry = create_sra_entry(xml_type, entry, status, accessibility, info[2], info[3], info[4])
            results[xml_type].append(sra_entry)

    return results


# === Main processing ===


def process_source(
    config: Config,
    source: SourceKind,
    output_dir: Path,
    blacklist: Set[str],
    full: bool,
    since: Optional[str],
) -> Dict[str, int]:
    """
    DRA または NCBI SRA を処理する。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        output_dir: 出力ディレクトリ
        blacklist: blacklist
        full: 全件処理するかどうか
        since: 差分更新の基準日時

    Returns:
        {xml_type: count}
    """
    is_dra = source == "dra"
    prefix = "dra" if is_dra else "ncbi"

    log_info(f"processing {source.upper()}...")

    # tar reader を取得
    if is_dra:
        tar_reader = get_dra_tar_reader(config)
    else:
        tar_reader = get_ncbi_tar_reader(config)

    # 対象 submission を取得
    if full or since is None:
        submissions = list(iter_all_submissions(config, source))
        log_info(f"full update mode: {len(submissions)} submissions")
    else:
        submissions = list(iter_updated_submissions(config, source, since))
        log_info(f"incremental update mode: {len(submissions)} submissions (since={since})")

    if not submissions:
        log_info(f"no submissions to process for {source}")
        return {t: 0 for t in XML_TYPES}

    # 結果を格納
    all_entries: Dict[str, List[Any]] = {t: [] for t in XML_TYPES}

    # バッチ処理
    batch_size = DEFAULT_BATCH_SIZE
    for i in range(0, len(submissions), batch_size):
        batch = submissions[i:i + batch_size]
        log_info(f"processing batch {i // batch_size + 1}/{(len(submissions) - 1) // batch_size + 1} ({len(batch)} submissions)")

        # Accessions DB から情報を取得
        # 各 submission に含まれる全 accession のリストを作成
        all_accessions: List[str] = []
        for sub in batch:
            all_accessions.append(sub)
            # submission に関連する accession は tar から取得時に判明するので、
            # ここでは submission 自体のみを追加

        # submission を処理
        for sub in batch:
            # submission に関連する accession を tar から取得
            sub_accessions: List[str] = [sub]

            # 各 XML タイプを確認して accession を収集
            for xml_type in XML_TYPES:
                if xml_type == "submission":
                    continue
                xml_bytes = tar_reader.read_xml(sub, xml_type)
                if xml_bytes:
                    try:
                        parsed = parse_xml(xml_bytes)
                        set_key = f"{xml_type.upper()}_SET"
                        entry_key = xml_type.upper()
                        for entry_data in _get_entries(parsed, set_key, entry_key):
                            acc = entry_data.get("accession")
                            if acc:
                                sub_accessions.append(acc)
                    except Exception as e:
                        log_debug(f"failed to collect accessions from {xml_type} xml: {e}", accession=sub,
                                  debug_category=DebugCategory.XML_ACCESSION_COLLECT_FAILED)

            # Accessions DB から情報を取得
            accession_info = get_accession_info_bulk(config, source, sub_accessions)

            # DRA は Received (dateCreated) が常に空なので SRA 側から補完
            if is_dra:
                missing_received = [acc for acc, info in accession_info.items() if info[2] is None]
                if missing_received:
                    sra_info = get_accession_info_bulk(config, "sra", missing_received)
                    for acc in missing_received:
                        if acc in sra_info and sra_info[acc][2] is not None:
                            dra = accession_info[acc]
                            accession_info[acc] = (dra[0], dra[1], sra_info[acc][2], dra[3], dra[4], dra[5])

            # submission を処理
            results = process_submission_xml(
                tar_reader, sub, config, blacklist, accession_info
            )

            for xml_type in XML_TYPES:
                all_entries[xml_type].extend(results[xml_type])

    # tar reader を閉じる
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

    # JSONL を出力
    counts: Dict[str, int] = {}
    for xml_type in XML_TYPES:
        output_path = output_dir / f"{prefix}_{xml_type}.jsonl"
        write_jsonl(output_path, all_entries[xml_type])
        counts[xml_type] = len(all_entries[xml_type])
        log_info(f"wrote {counts[xml_type]} {xml_type} entries to {output_path.name}")

    return counts


def generate_sra_jsonl(  # pylint: disable=unused-argument
    config: Config,
    output_dir: Path,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
    full: bool = False,
) -> None:
    """
    SRA JSONL ファイルを生成する。

    DRA と NCBI SRA の両方を処理し、別ファイルに出力する。

    Args:
        config: Config オブジェクト
        output_dir: 出力ディレクトリ ({result_dir}/sra/jsonl/{date}/)
        parallel_num: 並列数 (現在は未使用)
        full: True の場合は全件処理、False の場合は差分更新
    """
    # blacklist を読み込む
    blacklist = load_sra_blacklist(config)
    log_info(f"loaded {len(blacklist)} blacklisted accessions")

    # 差分更新の基準日時を取得
    since: Optional[str] = None
    if not full:
        last_run = read_last_run(config)
        since = last_run.get("sra")
        if since is not None:
            log_info(f"incremental update mode: since={since}")
        else:
            log_info("full update mode: no previous run found")
    else:
        log_info("full update mode: --full specified")

    # 出力ディレクトリを作成
    output_dir.mkdir(parents=True, exist_ok=True)

    # DRA を処理
    dra_counts = process_source(config, "dra", output_dir, blacklist, full, since)
    total_dra = sum(dra_counts.values())
    log_info(f"dra total: {total_dra} entries")

    # NCBI SRA を処理
    sra_counts = process_source(config, "sra", output_dir, blacklist, full, since)
    total_sra = sum(sra_counts.values())
    log_info(f"ncbi sra total: {total_sra} entries")

    # 合計を出力
    for xml_type in XML_TYPES:
        total = dra_counts.get(xml_type, 0) + sra_counts.get(xml_type, 0)
        log_info(f"total {xml_type}: {total} entries")

    # last_run.json を更新
    write_last_run(config, "sra")
    log_info("updated last_run.json for sra")


# === CLI ===


def parse_args(args: List[str]) -> Tuple[Config, Path, int, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Generate SRA JSONL files from tar archives."
    )
    parser.add_argument(
        "--parallel-num",
        help=f"Number of parallel workers (currently unused). Default: {DEFAULT_PARALLEL_NUM}",
        type=int,
        default=DEFAULT_PARALLEL_NUM,
    )
    parser.add_argument(
        "--full",
        help="Process all entries instead of incremental update.",
        action="store_true",
    )

    parsed = parser.parse_args(args)

    config = get_config()

    sra_base_dir = config.result_dir / SRA_BASE_DIR_NAME
    output_dir = sra_base_dir / JSONL_DIR_NAME / TODAY_STR

    return config, output_dir, parsed.parallel_num, parsed.full


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, parallel_num, full = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_sra_jsonl", config=config):
        log_debug(f"config: {config.model_dump_json(indent=2)}")
        log_debug(f"output directory: {output_dir}")
        log_debug(f"parallel workers: {parallel_num}")
        log_debug(f"full update: {full}")

        generate_sra_jsonl(config, output_dir, parallel_num, full)


if __name__ == "__main__":
    main()
