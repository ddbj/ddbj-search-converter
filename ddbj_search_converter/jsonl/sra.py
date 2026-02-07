"""SRA JSONL 生成モジュール。

DRA (DDBJ) と NCBI SRA の XML を tar ファイルから読み込み、
6 種類 (submission, study, experiment, run, sample, analysis) の JSONL を生成する。

並列処理アーキテクチャ:
    Producer-Worker パターンを採用。tar reader はシングルスレッドで動作し、
    batch 分の XML を読み込んだら ProcessPoolExecutor の worker に submit する。
    各 worker は独立して DB クエリ、XML パース、dbXrefs 取得、JSONL 出力を行う。
"""
from __future__ import annotations

import argparse
import gc
import sys
from concurrent.futures import (FIRST_COMPLETED, Future, ProcessPoolExecutor,
                                wait)
from pathlib import Path
from typing import (Any, Callable, Dict, Iterator, List, Literal, Optional,
                    Set, Tuple, cast)

from ddbj_search_converter.config import (JSONL_DIR_NAME, SRA_BASE_DIR_NAME,
                                          TODAY_STR, Config, get_config,
                                          read_last_run, write_last_run)
from ddbj_search_converter.config import SEARCH_BASE_URL
from ddbj_search_converter.dblink.utils import load_sra_blacklist
from ddbj_search_converter.jsonl.utils import get_dbxref_map, write_jsonl
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  log_warn, run_logger)
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.schema import (SRA, Accessibility, Distribution,
                                          Organism, Status, XrefType)
from ddbj_search_converter.sra.tar_reader import (SraXmlType, TarXMLReader,
                                                  get_dra_tar_path,
                                                  get_ncbi_tar_path)
from ddbj_search_converter.sra_accessions_tab import (SourceKind,
                                                      get_accession_info_bulk,
                                                      iter_all_submissions,
                                                      iter_updated_submissions)
from ddbj_search_converter.xml_utils import parse_xml

DEFAULT_BATCH_SIZE = 5000
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

XREF_TYPE_MAP: Dict[SraXmlType, XrefType] = {
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
            "submission_date": submission.get("submission_date"),
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
                "alias": study.get("alias"),
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
                "alias": exp.get("alias"),
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
                "alias": run.get("alias"),
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
                "alias": sample.get("alias"),
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
                "alias": analysis.get("alias"),
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
        contentUrl=f"{SEARCH_BASE_URL}/search/entries/{entry_type}/{identifier}.json",
    )]


def _make_url(entry_type: str, identifier: str) -> str:
    """URL を作成する。"""
    return f"{SEARCH_BASE_URL}/search/entries/{entry_type}/{identifier}"


def _get_name_from_alias(accession: str, alias: Optional[str]) -> Optional[str]:
    """alias が accession と異なる場合のみ name として返す。"""
    if alias is None:
        return None
    if alias == accession:
        return None
    return alias


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
        name=_get_name_from_alias(identifier, parsed.get("alias")),
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
    submission: str,
    blacklist: Set[str],
    accession_info: Dict[str, Tuple[str, str, Optional[str], Optional[str], Optional[str], str]],
    is_dra: bool,
    xml_cache: Dict[SraXmlType, Optional[bytes]],
) -> Dict[SraXmlType, List[Any]]:
    """
    1つの submission から全 XML タイプを処理する。

    Args:
        submission: submission accession
        blacklist: blacklist
        accession_info: {accession: (status, accessibility, received, updated, published, type)}
        is_dra: DRA かどうか（DRA の場合は XML の submission_date を dateCreated として使用）
        xml_cache: 事前に読み込んだ XML データのキャッシュ

    Returns:
        {xml_type: [model_instance, ...]}
    """
    results: Dict[SraXmlType, List[Any]] = {t: [] for t in XML_TYPES}

    # DRA の場合、submission_date を保持（他の XML タイプでも使用）
    submission_date: Optional[str] = None

    for xml_type in XML_TYPES:
        xml_bytes = xml_cache.get(xml_type)
        if not xml_bytes:
            continue

        parse_fn = _PARSE_FNS[xml_type]

        if xml_type == "submission":
            parsed = parse_fn(xml_bytes, submission)
            parsed_list = [parsed] if parsed and parsed.get("accession") else []
            # DRA の場合、submission_date を取得
            if is_dra and parsed_list:
                submission_date = parsed_list[0].get("submission_date")
        else:
            parsed_list = parse_fn(xml_bytes, submission)

        for entry in parsed_list:
            acc = entry.get("accession")
            if not acc or acc in blacklist:
                continue
            info = accession_info.get(acc, ("live", "public", None, None, None, ""))
            status = _normalize_status(info[0])
            accessibility = _normalize_accessibility(info[1])

            # DRA の場合は submission_date を dateCreated として使用
            if is_dra:
                date_created = submission_date
            else:
                date_created = info[2]  # Accessions.tab の Received
            date_modified = info[3]
            date_published = info[4]

            sra_entry = create_sra_entry(
                xml_type, entry, status, accessibility,
                date_created, date_modified, date_published
            )
            results[xml_type].append(sra_entry)

    return results


# === Batch processing ===


def _read_batch_xml(
    tar_reader: TarXMLReader,
    batch_subs: List[str],
) -> Dict[str, Dict[SraXmlType, Optional[bytes]]]:
    """
    バッチ分の submission から全 XML を読み込む。

    Args:
        tar_reader: TarXMLReader
        batch_subs: バッチ内の submission accession リスト

    Returns:
        {submission: {xml_type: xml_bytes or None}}
    """
    xml_data: Dict[str, Dict[SraXmlType, Optional[bytes]]] = {}
    for sub in batch_subs:
        xml_data[sub] = {}
        for xml_type in XML_TYPES:
            xml_data[sub][xml_type] = tar_reader.read_xml(sub, xml_type)
    return xml_data


def _extract_accessions_from_xml(
    xml_bytes: Optional[bytes],
    xml_type: SraXmlType,
    sub: str,
) -> List[str]:
    """XML から accession を抽出する。"""
    if not xml_bytes:
        return []

    if xml_type == "submission":
        return []

    try:
        parsed = parse_xml(xml_bytes)
        set_key = f"{xml_type.upper()}_SET"
        entry_key = xml_type.upper()
        accessions = []
        for entry_data in _get_entries(parsed, set_key, entry_key):
            acc = entry_data.get("accession")
            if acc:
                accessions.append(acc)
        return accessions
    except Exception as e:
        log_debug(
            f"failed to collect accessions from {xml_type} xml: {e}",
            accession=sub,
            debug_category=DebugCategory.XML_ACCESSION_COLLECT_FAILED
        )
        return []


def _process_batch_worker(
    config: Config,
    source: SourceKind,
    batch_num: int,
    total_batches: int,
    batch_subs: List[str],
    xml_data: Dict[str, Dict[SraXmlType, Optional[bytes]]],
    blacklist: Set[str],
    output_dir: Path,
    is_dra: bool,
) -> Dict[str, int]:
    """
    1 batch を処理して JSONL を出力するワーカー関数。

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        batch_num: バッチ番号
        total_batches: 総バッチ数
        batch_subs: バッチ内の submission リスト
        xml_data: {submission: {xml_type: xml_bytes}}
        blacklist: blacklist
        output_dir: 出力ディレクトリ
        is_dra: DRA かどうか

    Returns:
        {xml_type: count}
    """
    prefix = "dra" if is_dra else "ncbi"

    # Step 1: accession 収集
    all_accessions: List[str] = []
    for sub in batch_subs:
        all_accessions.append(sub)
        for xml_type in XML_TYPES:
            accessions = _extract_accessions_from_xml(
                xml_data[sub].get(xml_type),
                xml_type,
                sub,
            )
            all_accessions.extend(accessions)

    # Step 2: Accessions DB クエリ（バッチ全体で1回）
    accession_info = get_accession_info_bulk(config, source, all_accessions)

    # Step 3: XML パース + モデル作成
    counts: Dict[str, int] = {t: 0 for t in XML_TYPES}
    batch_entries: Dict[SraXmlType, List[SRA]] = {t: [] for t in XML_TYPES}
    seen_ids: Dict[SraXmlType, Set[str]] = {t: set() for t in XML_TYPES}

    for sub in batch_subs:
        results = process_submission_xml(
            submission=sub,
            blacklist=blacklist,
            accession_info=accession_info,
            is_dra=is_dra,
            xml_cache=xml_data[sub],
        )
        for xml_type in XML_TYPES:
            for entry in results[xml_type]:
                if entry.identifier not in seen_ids[xml_type]:
                    batch_entries[xml_type].append(entry)
                    seen_ids[xml_type].add(entry.identifier)

    # Step 4: dbXrefs 取得（バッチ全体で一括取得）
    for xml_type in XML_TYPES:
        accessions = [e.identifier for e in batch_entries[xml_type]]
        if accessions:
            dbxref_map = get_dbxref_map(config, XREF_TYPE_MAP[xml_type], accessions)
            for entry in batch_entries[xml_type]:
                if entry.identifier in dbxref_map:
                    entry.dbXrefs = dbxref_map[entry.identifier]

    # Step 5: JSONL 出力（XML type ごとに分割ファイル）
    for xml_type in XML_TYPES:
        output_path = output_dir / f"{prefix}_{xml_type}_{batch_num:04d}.jsonl"
        write_jsonl(output_path, batch_entries[xml_type])
        counts[xml_type] = len(batch_entries[xml_type])

    log_info(f"completed batch {batch_num}/{total_batches}")
    return counts


# === Main processing ===


def process_source(
    config: Config,
    source: SourceKind,
    output_dir: Path,
    blacklist: Set[str],
    full: bool,
    since: Optional[str],
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> Dict[str, int]:
    """
    DRA または NCBI SRA を処理する。

    Producer-Worker パターンで並列処理:
    - Producer (メインスレッド): tar から XML を読み込み、buffer に蓄積
    - Worker (worker プロセス): バッチ処理して JSONL 出力

    Args:
        config: Config オブジェクト
        source: "dra" or "sra"
        output_dir: 出力ディレクトリ
        blacklist: blacklist
        full: 全件処理するかどうか
        since: 差分更新の基準日時
        parallel_num: Worker プロセス数

    Returns:
        {xml_type: count}
    """
    is_dra = source == "dra"

    log_info(f"processing {source.upper()}...")

    # tar パスを取得
    if is_dra:
        tar_path = get_dra_tar_path(config)
    else:
        tar_path = get_ncbi_tar_path(config)

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

    # tar を開く
    tar_reader = TarXMLReader(tar_path)

    # submission を tar の offset 順にソート（シーケンシャル読み込み最適化）
    log_info("sorting submissions by tar offset...")
    offsets = tar_reader.get_submission_offsets(submissions)
    sorted_submissions = sorted(submissions, key=lambda s: offsets.get(s, float("inf")))

    batch_size = DEFAULT_BATCH_SIZE
    total_batches = (len(sorted_submissions) - 1) // batch_size + 1
    log_info(f"batch_size={batch_size}, total_batches={total_batches}, parallel_num={parallel_num}")

    # バッチイテレータ
    def batch_iter() -> Iterator[Tuple[int, List[str]]]:
        for i in range(0, len(sorted_submissions), batch_size):
            batch_num = i // batch_size + 1
            batch_subs = sorted_submissions[i:i + batch_size]
            yield batch_num, batch_subs

    batches = batch_iter()
    buffer: List[Tuple[int, List[str], Dict[str, Dict[SraXmlType, Optional[bytes]]]]] = []
    batches_exhausted = False

    # 合計カウント
    total_counts: Dict[str, int] = {t: 0 for t in XML_TYPES}
    completed_batches = 0

    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        pending: Set[Future[Dict[str, int]]] = set()

        while True:
            # バッファが空いていれば先読み（tar reader は止まらない）
            while not batches_exhausted and len(buffer) < parallel_num + 1:
                try:
                    batch_num, batch_subs = next(batches)
                    xml_data = _read_batch_xml(tar_reader, batch_subs)
                    buffer.append((batch_num, batch_subs, xml_data))
                    log_info(f"read batch {batch_num}/{total_batches} into buffer ({len(batch_subs)} submissions)")
                except StopIteration:
                    batches_exhausted = True
                    break

            # worker が空いていて、バッファにデータがあれば submit
            while len(pending) < parallel_num and buffer:
                batch_num, batch_subs, xml_data = buffer.pop(0)
                future = executor.submit(
                    _process_batch_worker,
                    config, source, batch_num, total_batches, batch_subs, xml_data,
                    blacklist, output_dir, is_dra,
                )
                pending.add(future)
                log_info(f"submitted batch {batch_num}/{total_batches}")
                del xml_data  # pickle 化後に参照を解放

            # 終了条件
            if not pending:
                break

            # 1つ完了するまで待機
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for f in done:
                try:
                    counts = f.result()
                    completed_batches += 1
                    for xml_type, count in counts.items():
                        total_counts[xml_type] += count
                    log_info(f"completed batch ({completed_batches}/{total_batches})")
                except Exception as e:
                    log_warn(f"batch processing failed: {e}")

            # 定期的にガベージコレクションを実行してメモリを解放
            if completed_batches % 10 == 0:
                gc.collect()

    # tar reader を閉じる
    tar_reader.close()

    # 結果をログ出力
    for xml_type in XML_TYPES:
        log_info(f"{source} {xml_type}: {total_counts[xml_type]} entries")

    return total_counts


def generate_sra_jsonl(
    config: Config,
    output_dir: Path,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
    full: bool = False,
) -> None:
    """
    SRA JSONL ファイルを生成する。

    DRA と NCBI SRA の両方を処理し、バッチ単位で分割された JSONL ファイルを出力する。
    出力ファイル名: {prefix}_{xml_type}_{batch_num:04d}.jsonl
    例: dra_submission_0001.jsonl, ncbi_run_0042.jsonl

    Args:
        config: Config オブジェクト
        output_dir: 出力ディレクトリ ({result_dir}/sra/jsonl/{date}/)
        parallel_num: Worker プロセス数
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
    dra_counts = process_source(
        config, "dra", output_dir, blacklist, full, since, parallel_num
    )
    total_dra = sum(dra_counts.values())
    log_info(f"dra total: {total_dra} entries")

    # NCBI SRA を処理
    sra_counts = process_source(
        config, "sra", output_dir, blacklist, full, since, parallel_num
    )
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
        help=f"Number of consumer processes for XML parsing. Default: {DEFAULT_PARALLEL_NUM}",
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
        log_debug(f"consumer processes: {parallel_num}")
        log_debug(f"full update: {full}")

        generate_sra_jsonl(config, output_dir, parallel_num, full)


if __name__ == "__main__":
    main()
