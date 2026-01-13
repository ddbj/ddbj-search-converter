"""
BioProject の umbrella 関連を抽出し、DBLink データベースに挿入する。

入力:
- 分割済み BioProject XML ({result_dir}/bioproject/tmp_xml/{YYYYMMDD}/)
    - prepare_bioproject_xml CLI で事前に作成する
- blacklist file ({const_dir}/bp/blacklist.txt)
    - 除外する BioProject accession

対象:
- <Link><Hierarchical type="TopAdmin"> のみ
- TopSingle は親子関係ではなく同一プロジェクトの別 ID なので除外

関連の方向:
- primary (子) -> umbrella (親)
- AccessionType: bioproject -> umbrella-bioproject
"""
import xml.etree.ElementTree as ET
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, List, Set, Tuple

from ddbj_search_converter.config import BP_BLACKLIST_REL_PATH, Config, get_config
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.logging.logger import (log_error, log_info, log_warn,
                                                  run_logger)
from ddbj_search_converter.xml_utils import get_tmp_xml_dir

DEFAULT_PARALLEL_NUM = 32


def load_bp_blacklist(config: Config) -> Set[str]:
    bp_blacklist_path = config.const_dir.joinpath(BP_BLACKLIST_REL_PATH)
    bp_blacklist: Set[str] = set()

    if bp_blacklist_path.exists():
        bp_blacklist = set(bp_blacklist_path.read_text(encoding="utf-8").strip().split("\n"))
        bp_blacklist.discard("")
        log_info(f"loaded {len(bp_blacklist)} BioProject blacklist entries",
                 file=str(bp_blacklist_path))
    else:
        log_warn(f"BioProject blacklist not found, skipping: {bp_blacklist_path}",
                 file=str(bp_blacklist_path))

    return bp_blacklist


def filter_by_blacklist(primary_to_umbrella: IdPairs, bp_blacklist: Set[str]) -> IdPairs:
    """umbrella と primary の両方を blacklist でフィルタリング。"""
    original_count = len(primary_to_umbrella)
    filtered = {
        (umbrella, primary)
        for umbrella, primary in primary_to_umbrella
        if umbrella not in bp_blacklist and primary not in bp_blacklist
    }
    removed_count = original_count - len(filtered)
    if removed_count > 0:
        log_info(f"removed {removed_count} relations by blacklist")
    return filtered


def process_xml_file(xml_path: Path) -> List[Tuple[str, str]]:
    """
    分割された BioProject XML から umbrella 関連を抽出。
    <Link><Hierarchical type="TopAdmin"> のみを対象とする。
    """
    results: List[Tuple[str, str]] = []
    inside_link = False
    current_link: Dict[str, str] = {}

    with xml_path.open("r", encoding="utf-8") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            tag = elem.tag.split("}")[-1]

            if event == "start" and tag == "Link":
                inside_link = True
                current_link = {}

            elif inside_link and event == "start":
                if tag == "Hierarchical":
                    current_link["type"] = elem.attrib.get("type", "")
                elif tag == "ProjectIDRef":
                    current_link["project_id"] = elem.attrib.get("accession", "")
                elif tag == "MemberID":
                    current_link["member_id"] = elem.attrib.get("accession", "")

            elif event == "end" and tag == "Link":
                if current_link.get("type") == "TopAdmin":
                    project_id = current_link.get("project_id")
                    member_id = current_link.get("member_id")
                    if project_id and member_id:
                        results.append((project_id, member_id))

                inside_link = False
                current_link.clear()
                elem.clear()

    return results


def process_xml_files_parallel(
    xml_files: List[Path],
    worker_func: Callable[[Path], List[Tuple[str, str]]],
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> IdPairs:
    results: IdPairs = set()

    if not xml_files:
        return results

    log_info(f"processing {len(xml_files)} XML files with {parallel_num} workers")

    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures: Dict[Future[List[Tuple[str, str]]], Path] = {
            executor.submit(worker_func, xml_path): xml_path
            for xml_path in xml_files
        }

        for future in as_completed(futures):
            xml_path = futures[future]
            try:
                file_results = future.result()
                results.update(file_results)
                if file_results:
                    log_info(f"processed {xml_path.name}: {len(file_results)} relations",
                             file=str(xml_path))
            except Exception as e:  # pylint: disable=broad-exception-caught
                log_error(f"error processing {xml_path.name}: {e}",
                          error=e, file=str(xml_path))

    return results


def process_bioproject_xml(
    config: Config,
    primary_to_umbrella: IdPairs,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> None:
    tmp_xml_dir = get_tmp_xml_dir(config, "bioproject")

    # NCBI files
    ncbi_files = sorted(tmp_xml_dir.glob("ncbi_*.xml"))
    if ncbi_files:
        log_info(f"found {len(ncbi_files)} NCBI XML files")
        results = process_xml_files_parallel(ncbi_files, process_xml_file, parallel_num)
        primary_to_umbrella.update(results)
        log_info(f"extracted {len(results)} umbrella relations from NCBI XML")
    else:
        log_warn(f"no NCBI XML files found in {tmp_xml_dir}")

    # DDBJ files
    ddbj_files = sorted(tmp_xml_dir.glob("ddbj_*.xml"))
    if ddbj_files:
        log_info(f"found {len(ddbj_files)} DDBJ XML files")
        count_before = len(primary_to_umbrella)
        results = process_xml_files_parallel(ddbj_files, process_xml_file, parallel_num)
        primary_to_umbrella.update(results)
        count_new = len(primary_to_umbrella) - count_before
        log_info(f"extracted {count_new} umbrella relations from DDBJ XML")
    else:
        log_warn(f"no DDBJ XML files found in {tmp_xml_dir}")


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist = load_bp_blacklist(config)

        primary_to_umbrella: IdPairs = set()
        process_bioproject_xml(config, primary_to_umbrella)
        log_info(f"total {len(primary_to_umbrella)} unique umbrella relations (before filter)")

        primary_to_umbrella = filter_by_blacklist(primary_to_umbrella, bp_blacklist)
        log_info(f"total {len(primary_to_umbrella)} unique umbrella relations (after filter)")

        load_to_db(config, primary_to_umbrella, "bioproject", "umbrella-bioproject")


if __name__ == "__main__":
    main()
