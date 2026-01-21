"""SRA JSONL 生成モジュール。

DRA (DDBJ) と NCBI SRA の XML を tar ファイルから読み込み、
6 種類 (submission, study, experiment, run, sample, analysis) の JSONL を生成する。
"""
import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import xmltodict

from ddbj_search_converter.config import (JSONL_DIR_NAME, SRA_BASE_DIR_NAME,
                                          SRA_BLACKLIST_REL_PATH, TODAY_STR,
                                          Config, get_config, read_last_run,
                                          write_last_run)
from ddbj_search_converter.dblink.db import get_related_entities_bulk
from ddbj_search_converter.jsonl.utils import to_xref
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  log_warn, run_logger)
from ddbj_search_converter.schema import (Distribution, SraAnalysis,
                                          SraExperiment, SraOrganism, SraRun,
                                          SraSample, SraSampleAttribute,
                                          SraStatus, SraStudy, SraSubmission,
                                          SraVisibility, Xref, XrefType)
from ddbj_search_converter.sra.tar_reader import (SraXmlType, TarXMLReader,
                                                  get_dra_tar_reader,
                                                  get_ncbi_tar_reader)
from ddbj_search_converter.sra_accessions_tab import (SourceKind,
                                                      get_accession_info_bulk,
                                                      iter_all_submissions,
                                                      iter_updated_submissions)

DEFAULT_BATCH_SIZE = 1000
DEFAULT_PARALLEL_NUM = 8

# XML types
XML_TYPES: List[SraXmlType] = [
    "submission", "study", "experiment", "run", "sample", "analysis"
]


# === Blacklist ===


def load_sra_blacklist(config: Config) -> Set[str]:
    """SRA blacklist を読み込む。"""
    blacklist_path = config.const_dir / SRA_BLACKLIST_REL_PATH
    if not blacklist_path.exists():
        return set()

    blacklist: Set[str] = set()
    with blacklist_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                blacklist.add(line)
    return blacklist


# === Parse functions ===


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


def _normalize_status(status: Optional[str]) -> SraStatus:
    """status を正規化する。"""
    if status is None:
        return "public"
    status_lower = status.lower()
    if status_lower in ("public", "suppressed", "replaced", "killed", "unpublished"):
        return status_lower  # type: ignore
    return "public"


def _normalize_visibility(visibility: Optional[str]) -> SraVisibility:
    """visibility を正規化する。"""
    if visibility is None:
        return "public"
    visibility_lower = visibility.lower()
    if visibility_lower == "controlled-access":
        return "controlled-access"
    return "public"


def parse_submission(
    xml_bytes: bytes,
    is_dra: bool,
    accession: str,
) -> Optional[Dict[str, Any]]:
    """submission XML をパースする。"""
    try:
        parsed = xmltodict.parse(
            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
        )
        submission = parsed.get("SUBMISSION")
        if submission is None:
            return None

        return {
            "accession": submission.get("accession"),
            "title": _get_text(submission, "TITLE"),
            "center_name": submission.get("center_name"),
            "lab_name": submission.get("lab_name"),
            "submission_comment": submission.get("submission_comment"),
            "submission_date": submission.get("submission_date"),
            "properties": parsed,
        }
    except Exception as e:
        log_warn(f"Failed to parse submission XML: {e}", accession=accession)
        return None


def parse_study(
    xml_bytes: bytes,
    is_dra: bool,
    accession: str,
) -> List[Dict[str, Any]]:
    """study XML をパースする。複数の STUDY を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = xmltodict.parse(
            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
        )
        study_set = parsed.get("STUDY_SET", {})
        studies = study_set.get("STUDY")
        if studies is None:
            return []

        if not isinstance(studies, list):
            studies = [studies]

        for study in studies:
            descriptor = study.get("DESCRIPTOR", {})
            study_type = descriptor.get("STUDY_TYPE", {})

            results.append({
                "accession": study.get("accession"),
                "title": _get_text(descriptor, "STUDY_TITLE"),
                "description": _get_text(descriptor, "STUDY_ABSTRACT") or _get_text(descriptor, "STUDY_DESCRIPTION"),
                "study_type": study_type.get("existing_study_type") if isinstance(study_type, dict) else None,
                "center_name": study.get("center_name"),
                "properties": {"STUDY_SET": {"STUDY": study}},
            })
    except Exception as e:
        log_warn(f"Failed to parse study XML: {e}", accession=accession)
    return results


def parse_experiment(
    xml_bytes: bytes,
    is_dra: bool,
    accession: str,
) -> List[Dict[str, Any]]:
    """experiment XML をパースする。複数の EXPERIMENT を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = xmltodict.parse(
            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
        )
        experiment_set = parsed.get("EXPERIMENT_SET", {})
        experiments = experiment_set.get("EXPERIMENT")
        if experiments is None:
            return []

        if not isinstance(experiments, list):
            experiments = [experiments]

        for exp in experiments:
            design = exp.get("DESIGN", {})
            library_desc = design.get("LIBRARY_DESCRIPTOR", {})
            platform = exp.get("PLATFORM", {})

            # プラットフォーム情報を取得
            instrument_model = None
            for platform_name in platform.values():
                if isinstance(platform_name, dict):
                    instrument_model = platform_name.get("INSTRUMENT_MODEL")
                    break

            # library layout を取得
            library_layout = library_desc.get("LIBRARY_LAYOUT", {})
            layout = None
            if library_layout:
                if "PAIRED" in library_layout:
                    layout = "PAIRED"
                elif "SINGLE" in library_layout:
                    layout = "SINGLE"

            results.append({
                "accession": exp.get("accession"),
                "title": _get_text(exp, "TITLE"),
                "description": _get_text(design, "DESIGN_DESCRIPTION"),
                "instrument_model": instrument_model,
                "library_strategy": library_desc.get("LIBRARY_STRATEGY"),
                "library_source": library_desc.get("LIBRARY_SOURCE"),
                "library_selection": library_desc.get("LIBRARY_SELECTION"),
                "library_layout": layout,
                "center_name": exp.get("center_name"),
                "properties": {"EXPERIMENT_SET": {"EXPERIMENT": exp}},
            })
    except Exception as e:
        log_warn(f"Failed to parse experiment XML: {e}", accession=accession)
    return results


def parse_run(
    xml_bytes: bytes,
    is_dra: bool,
    accession: str,
) -> List[Dict[str, Any]]:
    """run XML をパースする。複数の RUN を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = xmltodict.parse(
            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
        )
        run_set = parsed.get("RUN_SET", {})
        runs = run_set.get("RUN")
        if runs is None:
            return []

        if not isinstance(runs, list):
            runs = [runs]

        for run in runs:
            results.append({
                "accession": run.get("accession"),
                "title": _get_text(run, "TITLE"),
                "description": None,
                "run_date": run.get("run_date"),
                "run_center": run.get("run_center"),
                "center_name": run.get("center_name"),
                "properties": {"RUN_SET": {"RUN": run}},
            })
    except Exception as e:
        log_warn(f"Failed to parse run XML: {e}", accession=accession)
    return results


def parse_sample(
    xml_bytes: bytes,
    is_dra: bool,
    accession: str,
) -> List[Dict[str, Any]]:
    """sample XML をパースする。複数の SAMPLE を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = xmltodict.parse(
            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
        )
        sample_set = parsed.get("SAMPLE_SET", {})
        samples = sample_set.get("SAMPLE")
        if samples is None:
            return []

        if not isinstance(samples, list):
            samples = [samples]

        for sample in samples:
            sample_name = sample.get("SAMPLE_NAME", {})
            organism = None
            tax_id = sample_name.get("TAXON_ID")
            sci_name = sample_name.get("SCIENTIFIC_NAME")
            if tax_id or sci_name:
                organism = SraOrganism(
                    identifier=str(tax_id) if tax_id else None,
                    name=sci_name,
                )

            # 属性を取得
            attributes: List[SraSampleAttribute] = []
            sample_attrs = sample.get("SAMPLE_ATTRIBUTES", {}).get("SAMPLE_ATTRIBUTE")
            if sample_attrs:
                if not isinstance(sample_attrs, list):
                    sample_attrs = [sample_attrs]
                for attr in sample_attrs:
                    if isinstance(attr, dict):
                        attributes.append(SraSampleAttribute(
                            tag=attr.get("TAG"),
                            value=attr.get("VALUE"),
                            units=attr.get("UNITS"),
                        ))

            results.append({
                "accession": sample.get("accession"),
                "title": _get_text(sample, "TITLE"),
                "description": _get_text(sample, "DESCRIPTION"),
                "organism": organism,
                "attributes": attributes,
                "center_name": sample.get("center_name"),
                "properties": {"SAMPLE_SET": {"SAMPLE": sample}},
            })
    except Exception as e:
        log_warn(f"Failed to parse sample XML: {e}", accession=accession)
    return results


def parse_analysis(
    xml_bytes: bytes,
    is_dra: bool,
    accession: str,
) -> List[Dict[str, Any]]:
    """analysis XML をパースする。複数の ANALYSIS を返す。"""
    results: List[Dict[str, Any]] = []
    try:
        parsed = xmltodict.parse(
            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
        )
        analysis_set = parsed.get("ANALYSIS_SET", {})
        analyses = analysis_set.get("ANALYSIS")
        if analyses is None:
            return []

        if not isinstance(analyses, list):
            analyses = [analyses]

        for analysis in analyses:
            # 分析タイプを取得
            analysis_type_obj = analysis.get("ANALYSIS_TYPE", {})
            analysis_type = None
            if analysis_type_obj:
                # ANALYSIS_TYPE の子要素のキーが分析タイプ
                for key in analysis_type_obj.keys():
                    analysis_type = key
                    break

            results.append({
                "accession": analysis.get("accession"),
                "title": _get_text(analysis, "TITLE"),
                "description": _get_text(analysis, "DESCRIPTION"),
                "analysis_type": analysis_type,
                "center_name": analysis.get("center_name"),
                "properties": {"ANALYSIS_SET": {"ANALYSIS": analysis}},
            })
    except Exception as e:
        log_warn(f"Failed to parse analysis XML: {e}", accession=accession)
    return results


# === Model creation ===


def _make_distribution(entry_type: str, identifier: str) -> List[Distribution]:
    """Distribution を作成する。"""
    return [Distribution(
        type="DataDownload",
        encodingFormat="JSON",
        contentUrl=f"https://ddbj.nig.ac.jp/search/entry/{entry_type}/{identifier}.json",
    )]


def _make_url(entry_type: str, identifier: str) -> str:
    """URL を作成する。"""
    return f"https://ddbj.nig.ac.jp/search/entry/{entry_type}/{identifier}"


def create_submission(
    parsed: Dict[str, Any],
    status: SraStatus,
    visibility: SraVisibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SraSubmission:
    """SraSubmission を作成する。"""
    identifier = parsed["accession"]
    return SraSubmission(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution("sra-submission", identifier),
        isPartOf="SRA",
        type="sra-submission",
        name=None,
        url=_make_url("sra-submission", identifier),
        title=parsed.get("title"),
        description=parsed.get("submission_comment"),
        centerName=parsed.get("center_name"),
        labName=parsed.get("lab_name"),
        dbXref=[],
        sameAs=[],
        status=status,
        visibility=visibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def create_study(
    parsed: Dict[str, Any],
    status: SraStatus,
    visibility: SraVisibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SraStudy:
    """SraStudy を作成する。"""
    identifier = parsed["accession"]
    return SraStudy(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution("sra-study", identifier),
        isPartOf="SRA",
        type="sra-study",
        name=None,
        url=_make_url("sra-study", identifier),
        organism=None,
        title=parsed.get("title"),
        description=parsed.get("description"),
        studyType=parsed.get("study_type"),
        centerName=parsed.get("center_name"),
        dbXref=[],
        sameAs=[],
        status=status,
        visibility=visibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def create_experiment(
    parsed: Dict[str, Any],
    status: SraStatus,
    visibility: SraVisibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SraExperiment:
    """SraExperiment を作成する。"""
    identifier = parsed["accession"]
    return SraExperiment(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution("sra-experiment", identifier),
        isPartOf="SRA",
        type="sra-experiment",
        name=None,
        url=_make_url("sra-experiment", identifier),
        organism=None,
        title=parsed.get("title"),
        description=parsed.get("description"),
        instrumentModel=parsed.get("instrument_model"),
        libraryStrategy=parsed.get("library_strategy"),
        librarySource=parsed.get("library_source"),
        librarySelection=parsed.get("library_selection"),
        libraryLayout=parsed.get("library_layout"),
        centerName=parsed.get("center_name"),
        dbXref=[],
        sameAs=[],
        status=status,
        visibility=visibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def create_run(
    parsed: Dict[str, Any],
    status: SraStatus,
    visibility: SraVisibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SraRun:
    """SraRun を作成する。"""
    identifier = parsed["accession"]
    return SraRun(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution("sra-run", identifier),
        isPartOf="SRA",
        type="sra-run",
        name=None,
        url=_make_url("sra-run", identifier),
        title=parsed.get("title"),
        description=parsed.get("description"),
        runDate=parsed.get("run_date"),
        runCenter=parsed.get("run_center"),
        centerName=parsed.get("center_name"),
        dbXref=[],
        sameAs=[],
        status=status,
        visibility=visibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def create_sample(
    parsed: Dict[str, Any],
    status: SraStatus,
    visibility: SraVisibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SraSample:
    """SraSample を作成する。"""
    identifier = parsed["accession"]
    return SraSample(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution("sra-sample", identifier),
        isPartOf="SRA",
        type="sra-sample",
        name=None,
        url=_make_url("sra-sample", identifier),
        organism=parsed.get("organism"),
        title=parsed.get("title"),
        description=parsed.get("description"),
        attributes=parsed.get("attributes", []),
        centerName=parsed.get("center_name"),
        dbXref=[],
        sameAs=[],
        status=status,
        visibility=visibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def create_analysis(
    parsed: Dict[str, Any],
    status: SraStatus,
    visibility: SraVisibility,
    date_created: Optional[str],
    date_modified: Optional[str],
    date_published: Optional[str],
) -> SraAnalysis:
    """SraAnalysis を作成する。"""
    identifier = parsed["accession"]
    return SraAnalysis(
        identifier=identifier,
        properties=parsed["properties"],
        distribution=_make_distribution("sra-analysis", identifier),
        isPartOf="SRA",
        type="sra-analysis",
        name=None,
        url=_make_url("sra-analysis", identifier),
        title=parsed.get("title"),
        description=parsed.get("description"),
        analysisType=parsed.get("analysis_type"),
        centerName=parsed.get("center_name"),
        dbXref=[],
        sameAs=[],
        status=status,
        visibility=visibility,
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


# === DBLink ===


def get_dbxref_map(
    config: Config,
    entity_type: XrefType,
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


# === Submission processing ===


def process_submission_xml(
    tar_reader: TarXMLReader,
    submission: str,
    is_dra: bool,
    config: Config,
    blacklist: Set[str],
    accession_info: Dict[str, Tuple[str, str, Optional[str], Optional[str], Optional[str], str]],
) -> Dict[SraXmlType, List[Any]]:
    """
    1つの submission から全 XML タイプを処理する。

    Args:
        tar_reader: TarXMLReader
        submission: submission accession
        is_dra: DRA かどうか
        config: Config
        blacklist: blacklist
        accession_info: {accession: (status, visibility, received, updated, published, type)}

    Returns:
        {xml_type: [model_instance, ...]}
    """
    results: Dict[SraXmlType, List[Any]] = {
        "submission": [],
        "study": [],
        "experiment": [],
        "run": [],
        "sample": [],
        "analysis": [],
    }

    # submission XML を処理
    xml_bytes = tar_reader.read_xml(submission, "submission")
    if xml_bytes:
        parsed = parse_submission(xml_bytes, is_dra, submission)
        if parsed and parsed.get("accession"):
            acc = parsed["accession"]
            if acc not in blacklist:
                info = accession_info.get(acc, ("public", "public", None, None, None, ""))
                status = _normalize_status(info[0])
                visibility = _normalize_visibility(info[1])
                submission_entry = create_submission(parsed, status, visibility, info[2], info[3], info[4])
                results["submission"].append(submission_entry)

    # study XML を処理
    xml_bytes = tar_reader.read_xml(submission, "study")
    if xml_bytes:
        for parsed in parse_study(xml_bytes, is_dra, submission):
            acc = parsed.get("accession")
            if acc and acc not in blacklist:
                info = accession_info.get(acc, ("public", "public", None, None, None, ""))
                status = _normalize_status(info[0])
                visibility = _normalize_visibility(info[1])
                study_entry = create_study(parsed, status, visibility, info[2], info[3], info[4])
                results["study"].append(study_entry)

    # experiment XML を処理
    xml_bytes = tar_reader.read_xml(submission, "experiment")
    if xml_bytes:
        for parsed in parse_experiment(xml_bytes, is_dra, submission):
            acc = parsed.get("accession")
            if acc and acc not in blacklist:
                info = accession_info.get(acc, ("public", "public", None, None, None, ""))
                status = _normalize_status(info[0])
                visibility = _normalize_visibility(info[1])
                experiment_entry = create_experiment(parsed, status, visibility, info[2], info[3], info[4])
                results["experiment"].append(experiment_entry)

    # run XML を処理
    xml_bytes = tar_reader.read_xml(submission, "run")
    if xml_bytes:
        for parsed in parse_run(xml_bytes, is_dra, submission):
            acc = parsed.get("accession")
            if acc and acc not in blacklist:
                info = accession_info.get(acc, ("public", "public", None, None, None, ""))
                status = _normalize_status(info[0])
                visibility = _normalize_visibility(info[1])
                run_entry = create_run(parsed, status, visibility, info[2], info[3], info[4])
                results["run"].append(run_entry)

    # sample XML を処理
    xml_bytes = tar_reader.read_xml(submission, "sample")
    if xml_bytes:
        for parsed in parse_sample(xml_bytes, is_dra, submission):
            acc = parsed.get("accession")
            if acc and acc not in blacklist:
                info = accession_info.get(acc, ("public", "public", None, None, None, ""))
                status = _normalize_status(info[0])
                visibility = _normalize_visibility(info[1])
                sample_entry = create_sample(parsed, status, visibility, info[2], info[3], info[4])
                results["sample"].append(sample_entry)

    # analysis XML を処理
    xml_bytes = tar_reader.read_xml(submission, "analysis")
    if xml_bytes:
        for parsed in parse_analysis(xml_bytes, is_dra, submission):
            acc = parsed.get("accession")
            if acc and acc not in blacklist:
                info = accession_info.get(acc, ("public", "public", None, None, None, ""))
                status = _normalize_status(info[0])
                visibility = _normalize_visibility(info[1])
                analysis_entry = create_analysis(parsed, status, visibility, info[2], info[3], info[4])
                results["analysis"].append(analysis_entry)

    return results


# === Output ===


def write_jsonl(output_path: Path, docs: List[Any]) -> None:
    """モデルインスタンスのリストを JSONL ファイルに書き込む。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(doc.model_dump_json(by_alias=True))
            f.write("\n")


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

    log_info(f"Processing {source.upper()}...")

    # tar reader を取得
    if is_dra:
        tar_reader = get_dra_tar_reader(config)
    else:
        tar_reader = get_ncbi_tar_reader(config)

    # 対象 submission を取得
    if full or since is None:
        submissions = list(iter_all_submissions(config, source))
        log_info(f"Full update mode: {len(submissions)} submissions")
    else:
        submissions = list(iter_updated_submissions(config, source, since))
        log_info(f"Incremental update mode: {len(submissions)} submissions (since={since})")

    if not submissions:
        log_info(f"No submissions to process for {source}")
        return {t: 0 for t in XML_TYPES}

    # 結果を格納
    all_entries: Dict[str, List[Any]] = {t: [] for t in XML_TYPES}

    # バッチ処理
    batch_size = DEFAULT_BATCH_SIZE
    for i in range(0, len(submissions), batch_size):
        batch = submissions[i:i + batch_size]
        log_info(f"Processing batch {i // batch_size + 1}/{(len(submissions) - 1) // batch_size + 1} ({len(batch)} submissions)")

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
                        parsed = xmltodict.parse(
                            xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
                        )
                        # SET の中から accession を抽出
                        set_key = f"{xml_type.upper()}_SET"
                        entry_key = xml_type.upper()
                        entries_data = parsed.get(set_key, {}).get(entry_key)
                        if entries_data:
                            if not isinstance(entries_data, list):
                                entries_data = [entries_data]
                            for entry_data in entries_data:
                                acc = entry_data.get("accession")
                                if acc:
                                    sub_accessions.append(acc)
                    except Exception:
                        pass

            # Accessions DB から情報を取得
            accession_info = get_accession_info_bulk(config, source, sub_accessions)

            # submission を処理
            results = process_submission_xml(
                tar_reader, sub, is_dra, config, blacklist, accession_info
            )

            for xml_type in XML_TYPES:
                all_entries[xml_type].extend(results[xml_type])

    # tar reader を閉じる
    tar_reader.close()

    # dbXref を更新
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
                    entry.dbXref = dbxref_map[entry.identifier]

    # JSONL を出力
    counts: Dict[str, int] = {}
    for xml_type in XML_TYPES:
        output_path = output_dir / f"{prefix}_{xml_type}.jsonl"
        write_jsonl(output_path, all_entries[xml_type])
        counts[xml_type] = len(all_entries[xml_type])
        log_info(f"Wrote {counts[xml_type]} {xml_type} entries to {output_path.name}")

    return counts


def generate_sra_jsonl(
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
    log_info(f"Loaded {len(blacklist)} blacklisted accessions")

    # 差分更新の基準日時を取得
    since: Optional[str] = None
    if not full:
        last_run = read_last_run(config)
        since = last_run.get("sra")
        if since is not None:
            log_info(f"Incremental update mode: since={since}")
        else:
            log_info("Full update mode: no previous run found")
    else:
        log_info("Full update mode: --full specified")

    # 出力ディレクトリを作成
    output_dir.mkdir(parents=True, exist_ok=True)

    # DRA を処理
    try:
        dra_counts = process_source(config, "dra", output_dir, blacklist, full, since)
        total_dra = sum(dra_counts.values())
        log_info(f"DRA total: {total_dra} entries")
    except FileNotFoundError as e:
        log_warn(f"DRA tar not found, skipping: {e}")
        dra_counts = {t: 0 for t in XML_TYPES}

    # NCBI SRA を処理
    try:
        sra_counts = process_source(config, "sra", output_dir, blacklist, full, since)
        total_sra = sum(sra_counts.values())
        log_info(f"NCBI SRA total: {total_sra} entries")
    except FileNotFoundError as e:
        log_warn(f"NCBI SRA tar not found, skipping: {e}")
        sra_counts = {t: 0 for t in XML_TYPES}

    # 合計を出力
    for xml_type in XML_TYPES:
        total = dra_counts.get(xml_type, 0) + sra_counts.get(xml_type, 0)
        log_info(f"Total {xml_type}: {total} entries")

    # last_run.json を更新
    write_last_run(config, "sra")
    log_info("Updated last_run.json for sra")


# === CLI ===


def parse_args(args: List[str]) -> Tuple[Config, Path, int, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Generate SRA JSONL files from tar archives."
    )
    parser.add_argument(
        "--result-dir",
        help="Base directory for output. Default: $PWD/ddbj_search_converter_results. "
        "jsonl: {result_dir}/sra/jsonl/{date}/.",
        default=None,
    )
    parser.add_argument(
        "--date",
        help=f"Date string for output directory. Default: {TODAY_STR}",
        default=TODAY_STR,
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

    date_str = parsed.date
    sra_base_dir = config.result_dir / SRA_BASE_DIR_NAME
    output_dir = sra_base_dir / JSONL_DIR_NAME / date_str

    return config, output_dir, parsed.parallel_num, parsed.full


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, parallel_num, full = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_sra_jsonl", config=config):
        log_debug(f"Config: {config.model_dump_json(indent=2)}")
        log_debug(f"Output directory: {output_dir}")
        log_debug(f"Parallel workers: {parallel_num}")
        log_debug(f"Full update: {full}")

        generate_sra_jsonl(config, output_dir, parallel_num, full)


if __name__ == "__main__":
    main()
