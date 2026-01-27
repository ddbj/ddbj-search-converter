"""
BioSample <-> BioProject 関連を抽出し、DBLink データベースに挿入する。

このモジュールは TSV ファイルを直接出力しない。代わりに、関連を
dblink.tmp.duckdb データベースに書き込む。TSV ファイルは後から
dump_dblink_files CLI コマンドで出力する。

入力:
- 分割済み BioSample XML ({result_dir}/biosample/tmp_xml/{YYYYMMDD}/)
    - ncbi_{n}.xml: NCBI/EBI BioSample 分割ファイル
    - ddbj_{n}.xml: DDBJ BioSample 分割ファイル
    - prepare_biosample_xml CLI で事前に作成する
- SRA/DRA accessions DuckDB (sra_accessions.duckdb, dra_accessions.duckdb)
    - BioSample <-> BioProject 関連を含む
- bpbs_preserved.tsv ({const_dir}/dblink/)
    - 手動でキュレーションされた関連
- blacklist files ({const_dir}/)
    - bp/blacklist.txt: 除外する BioProject accession
    - bs/blacklist.txt: 除外する BioSample accession

注意:
- Assembly 由来の BioSample <-> BioProject 関連は assembly_and_master.py で処理する
- XML は事前に prepare_biosample_xml で分割する必要がある

出力:
- dblink.tmp.duckdb (relation テーブル) に挿入

処理フロー:
1. blacklist ファイルを読み込み
2. 分割済み NCBI BioSample XML を並列処理
3. 分割済み DDBJ BioSample XML を並列処理
4. SRA/DRA accessions DB から関連を抽出
5. preserved file から関連を読み込み
6. blacklist でフィルタリング
7. 全ての関連を TSV に書き出し -> DuckDB にロード
"""
import xml.etree.ElementTree as ET
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from ddbj_search_converter.config import (BP_BS_PRESERVED_REL_PATH,
                                          DRA_DB_FILE_NAME, SRA_DB_FILE_NAME,
                                          Config, get_config)
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.utils import (filter_by_blacklist,
                                                load_blacklist)
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import (log_debug, log_error,
                                                  log_info, run_logger)
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.sra_accessions_tab import iter_bp_bs_relations
from ddbj_search_converter.xml_utils import get_tmp_xml_dir

DEFAULT_PARALLEL_NUM = 32


# === XML processing functions ===

XmlProcessResult = Tuple[List[Tuple[str, str]], List[str]]


def process_ncbi_xml_file(xml_path: Path) -> XmlProcessResult:
    """
    NCBI: accession は <BioSample accession="..."> から取得。
    BP は <Link target="bioproject"> または <Attribute bioproject_accession> から。

    Returns:
        (relations, skipped_accessions)
    """
    results: List[Tuple[str, str]] = []
    skipped: List[str] = []
    current_bs: Optional[str] = None
    current_bs_is_valid = False

    with xml_path.open("r", encoding="utf-8") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            tag = elem.tag.split("}")[-1]

            if event == "start" and tag == "BioSample":
                current_bs = elem.attrib.get("accession")
                current_bs_is_valid = bool(current_bs and is_valid_accession(current_bs, "biosample"))
                if current_bs and not current_bs_is_valid:
                    skipped.append(current_bs)

            elif event == "end" and tag == "BioSample":
                current_bs = None
                current_bs_is_valid = False
                elem.clear()

            elif current_bs_is_valid and event == "end":
                if tag == "Link" and elem.attrib.get("target") == "bioproject":
                    bp = elem.attrib.get("label") or (elem.text or "").strip()
                    if bp and not bp.startswith("PRJ"):
                        bp = f"PRJNA{bp}"
                    if bp and is_valid_accession(bp, "bioproject"):
                        results.append((current_bs, bp))  # type: ignore

                elif tag == "Attribute" and elem.attrib.get("attribute_name") == "bioproject_accession":
                    bp = (elem.text or "").strip()
                    if bp and is_valid_accession(bp, "bioproject"):
                        results.append((current_bs, bp))  # type: ignore

                elem.clear()

    return results, skipped


def process_ddbj_xml_file(xml_path: Path) -> XmlProcessResult:
    """
    DDBJ: accession は <Ids><Id namespace="BioSample"> から取得 (NCBI と異なる)。
    BP は <Attribute attribute_name="bioproject_id"> から。
    (NCBI は bioproject_accession だが、DDBJ は bioproject_id を使用)

    Returns:
        (relations, skipped_accessions)
    """
    results: List[Tuple[str, str]] = []
    skipped: List[str] = []
    current_bs: Optional[str] = None
    current_bs_is_valid = False
    in_ids = False

    with xml_path.open("r", encoding="utf-8") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            tag = elem.tag.split("}")[-1]

            if event == "start" and tag == "BioSample":
                current_bs = None
                current_bs_is_valid = False
                in_ids = False

            elif event == "start" and tag == "Ids":
                in_ids = True

            elif event == "end" and tag == "Ids":
                in_ids = False

            elif event == "end" and tag == "Id" and in_ids:
                if elem.attrib.get("namespace") == "BioSample":
                    current_bs = (elem.text or "").strip()
                    current_bs_is_valid = bool(current_bs and is_valid_accession(current_bs, "biosample"))
                    if current_bs and not current_bs_is_valid:
                        skipped.append(current_bs)
                elem.clear()

            elif event == "end" and tag == "BioSample":
                current_bs = None
                current_bs_is_valid = False
                elem.clear()

            elif current_bs_is_valid and event == "end":
                attr_name = elem.attrib.get("attribute_name")
                # DDBJ uses "bioproject_id", NCBI uses "bioproject_accession"
                if tag == "Attribute" and attr_name in ("bioproject_id", "bioproject_accession"):
                    bp = (elem.text or "").strip()
                    if bp and is_valid_accession(bp, "bioproject"):
                        results.append((current_bs, bp))  # type: ignore
                elem.clear()

    return results, skipped


def process_xml_files_parallel(
    xml_files: List[Path],
    worker_func: Callable[[Path], XmlProcessResult],
    parallel_num: int = DEFAULT_PARALLEL_NUM,
    source: str = "ncbi",
) -> IdPairs:
    results: IdPairs = set()

    if not xml_files:
        return results

    log_info(f"processing {len(xml_files)} XML files with {parallel_num} workers")

    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures: Dict[Future[XmlProcessResult], Path] = {
            executor.submit(worker_func, xml_path): xml_path
            for xml_path in xml_files
        }

        for future in as_completed(futures):
            xml_path = futures[future]
            try:
                file_results, skipped = future.result()
                results.update(file_results)
                log_info(f"processed {xml_path.name}: {len(file_results)} relations",
                         file=str(xml_path))
                for acc in skipped:
                    log_debug(f"skipping invalid biosample: {acc}", accession=acc, file=str(xml_path),
                              debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            except Exception as e:  # pylint: disable=broad-exception-caught
                log_error(f"error processing {xml_path.name}: {e}",
                          error=e, file=str(xml_path))

    return results


def process_ncbi_biosample_xml(
    config: Config,
    bs_to_bp: IdPairs,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> None:
    tmp_xml_dir = get_tmp_xml_dir(config, "biosample")
    ncbi_files = sorted(tmp_xml_dir.glob("ncbi_*.xml"))

    if not ncbi_files:
        raise FileNotFoundError(f"no NCBI XML files found in {tmp_xml_dir}")

    log_info(f"found {len(ncbi_files)} NCBI XML files in {tmp_xml_dir}")
    results = process_xml_files_parallel(ncbi_files, process_ncbi_xml_file, parallel_num, source="ncbi")
    bs_to_bp.update(results)
    log_info(f"extracted {len(results)} NCBI BioSample -> BioProject relations")


def process_ddbj_biosample_xml(
    config: Config,
    bs_to_bp: IdPairs,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> None:
    tmp_xml_dir = get_tmp_xml_dir(config, "biosample")
    ddbj_files = sorted(tmp_xml_dir.glob("ddbj_*.xml"))

    if not ddbj_files:
        raise FileNotFoundError(f"no DDBJ XML files found in {tmp_xml_dir}")

    log_info(f"found {len(ddbj_files)} DDBJ XML files in {tmp_xml_dir}")
    results = process_xml_files_parallel(ddbj_files, process_ddbj_xml_file, parallel_num, source="ddbj")
    bs_to_bp.update(results)
    log_info(f"extracted {len(results)} DDBJ BioSample -> BioProject relations")


def process_sra_dra_accessions(config: Config, bs_to_bp: IdPairs) -> None:
    sra_db_path = config.const_dir.joinpath("sra", SRA_DB_FILE_NAME)
    log_info("processing SRA accessions database", file=str(sra_db_path))
    for bp, bs in iter_bp_bs_relations(config, source="sra"):
        # SRA_Accessions.tab contains invalid BioSample IDs (numeric internal IDs)
        if not bs or not bp:
            continue
        if not is_valid_accession(bs, "biosample"):
            log_debug(f"skipping invalid biosample: {bs}", accession=bs, file=str(sra_db_path),
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source="sra")
            continue
        if not is_valid_accession(bp, "bioproject"):
            log_debug(f"skipping invalid bioproject: {bp}", accession=bp, file=str(sra_db_path),
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source="sra")
            continue
        bs_to_bp.add((bs, bp))

    dra_db_path = config.const_dir.joinpath("sra", DRA_DB_FILE_NAME)
    log_info("processing DRA accessions database", file=str(dra_db_path))
    for bp, bs in iter_bp_bs_relations(config, source="dra"):
        if not bs or not bp:
            continue
        if not is_valid_accession(bs, "biosample"):
            log_debug(f"skipping invalid biosample: {bs}", accession=bs, file=str(dra_db_path),
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source="dra")
            continue
        if not is_valid_accession(bp, "bioproject"):
            log_debug(f"skipping invalid bioproject: {bp}", accession=bp, file=str(dra_db_path),
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source="dra")
            continue
        bs_to_bp.add((bs, bp))


def process_preserved_file(config: Config, bs_to_bp: IdPairs) -> None:
    """TSV format: BioSample\tBioProject

    Raises:
        FileNotFoundError: If preserved file is not found.
    """
    preserved_path = config.const_dir.joinpath(BP_BS_PRESERVED_REL_PATH)
    if not preserved_path.exists():
        raise FileNotFoundError(
            f"preserved file not found: {preserved_path}"
        )

    log_info(f"processing preserved file: {preserved_path}",
             file=str(preserved_path))

    with preserved_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                bs, bp = parts[0], parts[1]
                if not is_valid_accession(bs, "biosample"):
                    log_debug(f"skipping invalid biosample: {bs}", accession=bs, file=str(preserved_path),
                              debug_category=DebugCategory.INVALID_ACCESSION_ID, source="preserved")
                    continue
                if not is_valid_accession(bp, "bioproject"):
                    log_debug(f"skipping invalid bioproject: {bp}", accession=bp, file=str(preserved_path),
                              debug_category=DebugCategory.INVALID_ACCESSION_ID, source="preserved")
                    continue
                bs_to_bp.add((bs, bp))


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist, bs_blacklist = load_blacklist(config)

        bs_to_bp: IdPairs = set()

        process_ncbi_biosample_xml(config, bs_to_bp)
        process_ddbj_biosample_xml(config, bs_to_bp)
        process_sra_dra_accessions(config, bs_to_bp)
        process_preserved_file(config, bs_to_bp)

        log_info(f"total {len(bs_to_bp)} unique BioSample -> BioProject relations (before filtering)")

        bs_to_bp = filter_by_blacklist(bs_to_bp, bp_blacklist, bs_blacklist)

        log_info(f"total {len(bs_to_bp)} unique BioSample -> BioProject relations (after filtering)")

        load_to_db(config, bs_to_bp, "biosample", "bioproject")


if __name__ == "__main__":
    main()
