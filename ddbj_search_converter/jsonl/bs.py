"""BioSample JSONL 生成モジュール。"""
import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ddbj_search_converter.config import (BS_BASE_DIR_NAME,
                                          DEFAULT_MARGIN_DAYS, JSONL_DIR_NAME,
                                          SEARCH_BASE_URL, TMP_XML_DIR_NAME,
                                          TODAY_STR, Config, apply_margin,
                                          get_config, read_last_run,
                                          write_last_run)
from ddbj_search_converter.dblink.utils import load_blacklist
from ddbj_search_converter.jsonl.utils import get_dbxref_map, write_jsonl
from ddbj_search_converter.logging.logger import (log_debug, log_error,
                                                  log_info, log_warn,
                                                  run_logger)
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.schema import (Accessibility, Attribute, BioSample,
                                          Distribution, Model, Organism,
                                          Package, Status, Xref)
from ddbj_search_converter.xml_utils import iterate_xml_element, parse_xml

DEFAULT_BATCH_SIZE = 2000
DEFAULT_PARALLEL_NUM = 64


# === Parse functions ===


def parse_accession(sample: Dict[str, Any], is_ddbj: bool) -> str:
    """BioSample から accession を抽出する。"""
    if is_ddbj:
        ids = (sample.get("Ids") or {}).get("Id")
        if ids is None:
            raise ValueError("No Ids found in BioSample")
        if isinstance(ids, list):
            for id_obj in ids:
                if isinstance(id_obj, dict) and id_obj.get("namespace") == "BioSample":
                    content = id_obj.get("content")
                    if content is not None:
                        return str(content)
        elif isinstance(ids, dict):
            if ids.get("namespace") == "BioSample":
                content = ids.get("content")
                if content is not None:
                    return str(content)
        raise ValueError("No BioSample namespace ID found")
    accession = sample.get("accession")
    if accession is None:
        raise ValueError("No accession found in BioSample")
    return str(accession)


def parse_organism(sample: Dict[str, Any], is_ddbj: bool, accession: str = "") -> Optional[Organism]:
    """BioSample から Organism を抽出する。"""
    try:
        organism_obj = (sample.get("Description") or {}).get("Organism")
        if organism_obj is None:
            return None
        if is_ddbj:
            name = organism_obj.get("OrganismName")
        else:
            name = organism_obj.get("taxonomy_name") or organism_obj.get("OrganismName")
        return Organism(
            identifier=str(organism_obj.get("taxonomy_id", "")),
            name=name,
        )
    except Exception as e:
        log_warn(f"failed to parse organism: {e}", accession=accession)
        return None


def parse_title(sample: Dict[str, Any], accession: str = "") -> Optional[str]:
    """BioSample から title を抽出する。"""
    try:
        title = (sample.get("Description") or {}).get("Title")
        return str(title) if title is not None else None
    except Exception as e:
        log_warn(f"failed to parse title: {e}", accession=accession)
        return None


def parse_name(sample: Dict[str, Any], accession: str = "") -> Optional[str]:
    """BioSample から name (SampleName) を抽出する。"""
    try:
        name = (sample.get("Description") or {}).get("SampleName")
        return str(name) if name is not None else None
    except Exception as e:
        log_warn(f"failed to parse name: {e}", accession=accession)
        return None


def parse_description(sample: Dict[str, Any], accession: str = "") -> Optional[str]:
    """BioSample から description を抽出する。"""
    try:
        comment = (sample.get("Description") or {}).get("Comment")
        if comment is None:
            return None
        if isinstance(comment, str):
            return comment
        if isinstance(comment, dict):
            para = comment.get("Paragraph")
            if isinstance(para, str):
                return para
            if isinstance(para, list):
                return " ".join(str(p) for p in para if p is not None)
        return None
    except Exception as e:
        log_warn(f"failed to parse description: {e}", accession=accession)
        return None


def parse_attributes(sample: Dict[str, Any], accession: str = "") -> List[Attribute]:
    """BioSample から Attributes を抽出する。"""
    attributes: List[Attribute] = []
    try:
        attrs = (sample.get("Attributes") or {}).get("Attribute")
        if attrs is None:
            return []
        attr_list = attrs if isinstance(attrs, list) else [attrs]
        for attr in attr_list:
            if isinstance(attr, dict):
                attributes.append(Attribute(
                    attribute_name=attr.get("attribute_name"),
                    display_name=attr.get("display_name"),
                    harmonized_name=attr.get("harmonized_name"),
                    content=attr.get("content"),
                ))
            elif isinstance(attr, str):
                attributes.append(Attribute(
                    attribute_name=None,
                    display_name=None,
                    harmonized_name=None,
                    content=attr,
                ))
    except Exception as e:
        log_warn(f"failed to parse attributes: {e}", accession=accession)
    return attributes


def parse_model(sample: Dict[str, Any], accession: str = "") -> List[Model]:
    """BioSample から Model を抽出する。"""
    models: List[Model] = []
    try:
        model_obj = (sample.get("Models") or {}).get("Model")
        if model_obj is None:
            return []
        model_list = model_obj if isinstance(model_obj, list) else [model_obj]
        for model in model_list:
            if isinstance(model, str):
                models.append(Model(name=model))
            elif isinstance(model, dict):
                content = model.get("content")
                if content is not None:
                    models.append(Model(name=str(content)))
    except Exception as e:
        log_warn(f"failed to parse model: {e}", accession=accession)
    return models


def parse_package(sample: Dict[str, Any], model: List[Model], is_ddbj: bool, accession: str = "") -> Optional[Package]:
    """BioSample から Package を抽出する。"""
    try:
        if is_ddbj:
            if model:
                return Package(name=model[0].name, display_name=model[0].name)
            return None
        package_obj = sample.get("Package")
        if package_obj is None:
            return None
        if isinstance(package_obj, str):
            return Package(name=package_obj, display_name=package_obj)
        if isinstance(package_obj, dict):
            name = package_obj.get("content", "")
            display_name = package_obj.get("display_name") or name
            return Package(name=name, display_name=display_name)
        return None
    except Exception as e:
        log_warn(f"failed to parse package: {e}", accession=accession)
        return None


def parse_same_as(sample: Dict[str, Any], accession: str = "") -> List[Xref]:
    """BioSample から sameAs (SRA ID) を抽出する。"""
    xrefs: List[Xref] = []
    try:
        ids = (sample.get("Ids") or {}).get("Id")
        if ids is None:
            return []
        id_list = ids if isinstance(ids, list) else [ids]
        for id_obj in id_list:
            if not isinstance(id_obj, dict):
                continue
            db = id_obj.get("db")
            content = id_obj.get("content")
            if db == "SRA" and content:
                xrefs.append(Xref(
                    identifier=content,
                    type="sra-sample",
                    url=f"{SEARCH_BASE_URL}/search/entries/sra-sample/{content}",
                ))
    except Exception as e:
        log_warn(f"failed to parse same_as: {e}", accession=accession)
    return xrefs


def parse_date_from_xml(
    sample: Dict[str, Any]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """XML から日付を抽出する (NCBI 用)。"""
    date_created = sample.get("submission_date")
    date_modified = sample.get("last_update")
    date_published = sample.get("publication_date")
    return date_created, date_modified, date_published


def parse_status(sample: Dict[str, Any], accession: str = "") -> Status:
    """
    BioSample から status を抽出する。

    NCBI: Status/@status (live, suppressed など)
    DDBJ: Status 要素なし → "live" とみなす
    """
    try:
        status_obj = sample.get("Status")
        if status_obj is not None and isinstance(status_obj, dict):
            status: str = status_obj.get("status", "live")
            if status == "suppressed":
                return "suppressed"
            return "live"
    except Exception as e:
        log_warn(f"failed to parse status: {e}", accession=accession)
    return "live"


def parse_accessibility(sample: Dict[str, Any], accession: str = "") -> Accessibility:
    """
    BioSample から accessibility (@access) を抽出する。

    NCBI: @access (public, controlled)
    DDBJ: @access (public のみ)

    入力値 -> 出力値:
    - public -> public-access
    - controlled -> controlled-access
    - その他 -> public-access
    """
    try:
        access: str = sample.get("access", "public")
        if access == "controlled":
            return "controlled-access"
        return "public-access"
    except Exception as e:
        log_warn(f"failed to parse accessibility: {e}", accession=accession)
    return "public-access"


# === Properties normalization ===


def normalize_properties(sample: Dict[str, Any]) -> None:
    """properties 内の値を正規化する。"""
    _normalize_owner_name(sample)
    _normalize_model(sample)


def _normalize_owner_name(sample: Dict[str, Any]) -> None:
    """Owner.Name を正規化する。"""
    try:
        owner = sample.get("Owner")
        if owner is None:
            return
        name = owner.get("Name")
        if isinstance(name, str):
            sample["Owner"]["Name"] = {"content": name}
        elif isinstance(name, list):
            for i, item in enumerate(name):
                if isinstance(item, str):
                    sample["Owner"]["Name"][i] = {"content": item}
    except Exception as e:
        log_debug(f"failed to normalize owner name: {e}", debug_category=DebugCategory.NORMALIZE_OWNER_NAME)


def _normalize_model(sample: Dict[str, Any]) -> None:
    """Models.Model を正規化する。"""
    try:
        model = (sample.get("Models") or {}).get("Model")
        if model is None:
            return
        if isinstance(model, str):
            sample["Models"]["Model"] = {"content": model}
        elif isinstance(model, list):
            for i, item in enumerate(model):
                if isinstance(item, str):
                    sample["Models"]["Model"][i] = {"content": item}
    except Exception as e:
        log_debug(f"failed to normalize model: {e}", debug_category=DebugCategory.NORMALIZE_MODEL)


# === Conversion ===


def xml_entry_to_bs_instance(entry: Dict[str, Any], is_ddbj: bool) -> BioSample:
    """XML エントリを BioSample インスタンスに変換する。"""
    sample = entry["BioSample"]
    accession = parse_accession(sample, is_ddbj)

    normalize_properties(sample)

    model = parse_model(sample, accession)

    return BioSample(
        identifier=accession,
        properties={"BioSample": sample},
        distribution=[Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl=f"{SEARCH_BASE_URL}/search/entries/biosample/{accession}.json",
        )],
        isPartOf="BioSample",
        type="biosample",
        name=parse_name(sample, accession),
        url=f"{SEARCH_BASE_URL}/search/entries/biosample/{accession}",
        organism=parse_organism(sample, is_ddbj, accession),
        title=parse_title(sample, accession),
        description=parse_description(sample, accession),
        attributes=parse_attributes(sample, accession),
        model=model,
        package=parse_package(sample, model, is_ddbj, accession),
        dbXrefs=[],  # 後で更新
        sameAs=parse_same_as(sample, accession),
        status=parse_status(sample, accession),
        accessibility=parse_accessibility(sample, accession),
        dateCreated=None,  # 後で更新
        dateModified=None,  # 後で更新
        datePublished=None,  # 後で更新
    )


# === Processing ===


def _fetch_dates_ddbj(config: Config, docs: Dict[str, BioSample]) -> None:
    """DDBJ BioSample の日付を DuckDB キャッシュから取得して設定する。"""
    from ddbj_search_converter.date_cache.db import (  # pylint: disable=import-outside-toplevel
        date_cache_exists, fetch_bs_dates_from_cache)
    if not date_cache_exists(config):
        raise RuntimeError(
            "date cache not found. Run build_bp_bs_date_cache first."
        )

    date_map = fetch_bs_dates_from_cache(config, docs.keys())
    for acc, (dc, dm, dp) in date_map.items():
        if acc in docs:
            docs[acc].dateCreated = dc
            docs[acc].dateModified = dm
            docs[acc].datePublished = dp


def _fetch_dates_ncbi(xml_path: Path, docs: Dict[str, BioSample], is_ddbj: bool) -> None:
    """NCBI BioSample の日付を XML から取得して設定する。"""
    for xml_element in iterate_xml_element(xml_path, "BioSample"):
        try:
            metadata = parse_xml(xml_element)
            sample = metadata["BioSample"]
            accession = parse_accession(sample, is_ddbj)
            if accession in docs:
                date_created, date_modified, date_published = parse_date_from_xml(sample)
                docs[accession].dateCreated = date_created
                docs[accession].dateModified = date_modified
                docs[accession].datePublished = date_published
        except Exception as e:
            log_debug(f"failed to fetch ncbi dates from xml element: {e}", debug_category=DebugCategory.FETCH_DATES_FAILED)


def _process_xml_file_worker(
    config: Config, xml_path: Path, output_path: Path, is_ddbj: bool,
    bs_blacklist: Set[str],
    umbrella_ids: Set[str],
    target_accessions: Optional[Set[str]] = None,
    since: Optional[str] = None,
) -> int:
    """
    XML ファイルを処理して JSONL を出力するワーカー関数。

    Args:
        config: Config オブジェクト
        xml_path: 入力 XML ファイルのパス
        output_path: 出力 JSONL ファイルのパス
        is_ddbj: DDBJ データかどうか
        bs_blacklist: 除外する accession の集合
        target_accessions: 処理対象の accession の集合 (DDBJ 差分更新用)。None の場合は全件処理。
        since: 差分更新の基準日時 (NCBI 用)。None の場合は全件処理。
    """
    log_info(f"processing {xml_path.name} -> {output_path.name}")

    docs: Dict[str, BioSample] = {}
    skipped_count = 0
    filtered_count = 0

    for xml_element in iterate_xml_element(xml_path, "BioSample"):
        try:
            metadata = parse_xml(xml_element)
            bs_instance = xml_entry_to_bs_instance(metadata, is_ddbj)

            # blacklist チェック
            if bs_instance.identifier in bs_blacklist:
                skipped_count += 1
                continue

            # DDBJ 差分更新: target_accessions に含まれないものはスキップ
            if is_ddbj and target_accessions is not None:
                if bs_instance.identifier not in target_accessions:
                    filtered_count += 1
                    continue

            docs[bs_instance.identifier] = bs_instance
        except Exception as e:
            log_warn(f"failed to parse xml element: {e}", file=str(xml_path))

    if skipped_count > 0:
        log_info(f"skipped {skipped_count} blacklisted entries")
    if filtered_count > 0:
        log_info(f"filtered {filtered_count} entries (not in target_accessions)")

    # dbXrefs を一括取得
    dbxref_map = get_dbxref_map(config, "biosample", list(docs.keys()), umbrella_ids=umbrella_ids)
    for accession, xrefs in dbxref_map.items():
        if accession in docs:
            docs[accession].dbXrefs = xrefs

    # 日付を取得
    if is_ddbj:
        _fetch_dates_ddbj(config, docs)
    else:
        _fetch_dates_ncbi(xml_path, docs, is_ddbj)

    # NCBI 差分更新: since 以降に更新されたもののみ残す
    if not is_ddbj and since is not None:
        original_count = len(docs)
        docs = {
            acc: doc for acc, doc in docs.items()
            if doc.dateModified is not None and doc.dateModified >= since
        }
        ncbi_filtered = original_count - len(docs)
        if ncbi_filtered > 0:
            log_info(f"filtered {ncbi_filtered} ncbi entries (dateModified < {since})")

    write_jsonl(output_path, list(docs.values()))
    log_info(f"wrote {len(docs)} entries to {output_path}")

    return len(docs)


def process_xml_file(
    config: Config, xml_path: Path, output_path: Path, is_ddbj: bool,
    bs_blacklist: Optional[Set[str]] = None,
    umbrella_ids: Optional[Set[str]] = None,
    target_accessions: Optional[Set[str]] = None,
    since: Optional[str] = None,
) -> int:
    """単一の XML ファイルを処理して JSONL を出力する。"""
    if bs_blacklist is None:
        _, bs_blacklist = load_blacklist(config)
    if umbrella_ids is None:
        from ddbj_search_converter.dblink.db import \
            get_umbrella_bioproject_ids  # pylint: disable=import-outside-toplevel
        umbrella_ids = get_umbrella_bioproject_ids(config)
    return _process_xml_file_worker(
        config, xml_path, output_path, is_ddbj, bs_blacklist,
        umbrella_ids, target_accessions, since
    )


def generate_bs_jsonl(
    config: Config,
    tmp_xml_dir: Path,
    output_dir: Path,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
    full: bool = False,
    resume: bool = False,
) -> None:
    """
    BioSample JSONL ファイルを生成する。

    tmp_xml ディレクトリから分割済み XML を取得して並列処理する。

    Args:
        config: Config オブジェクト
        tmp_xml_dir: 入力 tmp_xml ディレクトリ ({result_dir}/biosample/tmp_xml/{date}/)
        output_dir: 出力ディレクトリ ({result_dir}/biosample/jsonl/{date}/)
        parallel_num: 並列ワーカー数
        full: True の場合は全件処理、False の場合は差分更新
        resume: True の場合は既存の JSONL ファイルをスキップ
    """
    if not tmp_xml_dir.exists():
        raise FileNotFoundError(f"tmp_xml directory not found: {tmp_xml_dir}")

    # blacklist を読み込む
    _, bs_blacklist = load_blacklist(config)

    # umbrella-bioproject ID セットを取得
    from ddbj_search_converter.dblink.db import \
        get_umbrella_bioproject_ids  # pylint: disable=import-outside-toplevel
    umbrella_ids = get_umbrella_bioproject_ids(config)

    # 差分更新の基準日時を取得
    since: Optional[str] = None
    ddbj_target_accessions: Optional[Set[str]] = None

    if not full:
        last_run = read_last_run(config)
        since = last_run.get("biosample")
        if since is not None:
            original_since = since
            since = apply_margin(since)
            log_info(f"incremental update mode: since={since} (original={original_since}, margin_days={DEFAULT_MARGIN_DAYS})")
            # DDBJ: DuckDB キャッシュから対象 accession を取得
            from ddbj_search_converter.date_cache.db import (  # pylint: disable=import-outside-toplevel
                date_cache_exists,
                fetch_bs_accessions_modified_since_from_cache)
            if not date_cache_exists(config):
                raise RuntimeError(
                    "date cache not found. Run build_bp_bs_date_cache first."
                )
            ddbj_target_accessions = fetch_bs_accessions_modified_since_from_cache(config, since)
            log_info(f"ddbj target accessions: {len(ddbj_target_accessions)}")
        else:
            log_info("full update mode: no previous run found")
    else:
        log_info("full update mode: --full specified")

    # DDBJ XML と NCBI XML をそれぞれ処理
    ddbj_xml_files = sorted(tmp_xml_dir.glob("ddbj_*.xml"))
    ncbi_xml_files = sorted(tmp_xml_dir.glob("ncbi_*.xml"))

    log_info(f"found {len(ddbj_xml_files)} ddbj xml files and {len(ncbi_xml_files)} ncbi xml files")

    tasks: List[Tuple[Path, Path, bool, Optional[Set[str]], Optional[str]]] = []
    skipped_existing = 0
    for xml_file in ddbj_xml_files:
        output_path = output_dir.joinpath(xml_file.stem + ".jsonl")
        if resume and output_path.exists():
            skipped_existing += 1
            continue
        tasks.append((xml_file, output_path, True, ddbj_target_accessions, None))
    for xml_file in ncbi_xml_files:
        output_path = output_dir.joinpath(xml_file.stem + ".jsonl")
        if resume and output_path.exists():
            skipped_existing += 1
            continue
        tasks.append((xml_file, output_path, False, None, since))

    if skipped_existing > 0:
        log_info(f"skipped {skipped_existing} existing files (resume mode)")

    total_count = 0
    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures = {
            executor.submit(
                _process_xml_file_worker, config, xml_path, output_path, is_ddbj,
                bs_blacklist, umbrella_ids, target_accessions, since_param
            ): (xml_path, is_ddbj)
            for xml_path, output_path, is_ddbj, target_accessions, since_param in tasks
        }
        for future in as_completed(futures):
            xml_path, is_ddbj = futures[future]
            try:
                count = future.result()
                total_count += count
            except Exception as e:
                log_error(f"failed to process {xml_path}: {e}", error=e, file=str(xml_path))

    log_info(f"generated {total_count} biosample entries in total")

    # last_run.json を更新
    write_last_run(config, "biosample")
    log_info("updated last_run.json for biosample")


# === CLI ===


def parse_args(args: List[str]) -> Tuple[Config, Path, Path, int, bool, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Generate BioSample JSONL files from split XML files."
    )
    parser.add_argument(
        "--parallel-num",
        help=f"Number of parallel workers. Default: {DEFAULT_PARALLEL_NUM}",
        type=int,
        default=DEFAULT_PARALLEL_NUM,
    )
    parser.add_argument(
        "--full",
        help="Process all entries instead of incremental update.",
        action="store_true",
    )
    parser.add_argument(
        "--resume",
        help="Skip existing JSONL files and resume from where it left off.",
        action="store_true",
    )

    parsed = parser.parse_args(args)

    config = get_config()

    bs_base_dir = config.result_dir / BS_BASE_DIR_NAME
    tmp_xml_dir = bs_base_dir / TMP_XML_DIR_NAME / TODAY_STR
    output_dir = bs_base_dir / JSONL_DIR_NAME / TODAY_STR

    return config, tmp_xml_dir, output_dir, parsed.parallel_num, parsed.full, parsed.resume


def main() -> None:
    """CLI エントリポイント。"""
    config, tmp_xml_dir, output_dir, parallel_num, full, resume = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_bs_jsonl", config=config):
        log_debug(f"config: {config.model_dump_json(indent=2)}")
        log_debug(f"tmp_xml directory: {tmp_xml_dir}")
        log_debug(f"output directory: {output_dir}")
        log_debug(f"parallel workers: {parallel_num}")
        log_debug(f"full update: {full}")
        log_debug(f"resume: {resume}")

        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"output directory: {output_dir}")

        generate_bs_jsonl(config, tmp_xml_dir, output_dir, parallel_num, full, resume)


if __name__ == "__main__":
    main()
