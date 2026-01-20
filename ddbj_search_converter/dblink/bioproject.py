"""
BioProject XML から各種関連を抽出し、DBLink データベースに挿入する。

入力:
- 分割済み BioProject XML ({result_dir}/bioproject/tmp_xml/{YYYYMMDD}/)
    - ncbi_{n}.xml: NCBI BioProject 分割ファイル
    - ddbj_{n}.xml: DDBJ BioProject 分割ファイル
    - prepare_bioproject_xml CLI で事前に作成する
- blacklist file ({const_dir}/bp/blacklist.txt)
    - 除外する BioProject accession

抽出する関連:
1. umbrella 関連 (bioproject -> umbrella-bioproject)
   - <Link><Hierarchical type="TopAdmin"> から抽出
   - TopSingle は親子関係ではなく同一プロジェクトの別 ID なので除外

2. hum-id 関連 (bioproject -> hum-id)
   - <LocalID submission_id="hum0XXX"> から抽出
   - バージョン情報 (例: hum0001.v2) は除去して hum0001 に正規化

3. geo 関連 (bioproject -> geo)
   - <CenterID center="GEO"> から抽出
   - GEO accession (GSExxxx) を取得

出力:
- dblink.tmp.duckdb (relation テーブル) に挿入
"""
import re
import xml.etree.ElementTree as ET
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ddbj_search_converter.config import (BP_BLACKLIST_REL_PATH, Config,
                                          get_config)
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.logging.logger import (log_error, log_info,
                                                  log_warn, run_logger)
from ddbj_search_converter.xml_utils import get_tmp_xml_dir

DEFAULT_PARALLEL_NUM = 32

# hum-id のバージョン部分を除去するパターン (例: hum0001.v2 -> hum0001)
HUM_ID_VERSION_PATTERN = re.compile(r"^(hum\d+)\..*$")


def normalize_hum_id(hum_id: str) -> str:
    """
    hum-id を正規化する。

    バージョン情報 (例: .v2) が含まれる場合は除去する。

    Args:
        hum_id: 正規化前の hum-id (例: hum0001.v2)

    Returns:
        正規化後の hum-id (例: hum0001)
    """
    match = HUM_ID_VERSION_PATTERN.match(hum_id)
    if match:
        return match.group(1)
    return hum_id


@dataclass
class BioProjectRelations:
    """BioProject XML から抽出した関連を格納するデータクラス。"""
    umbrella: List[Tuple[str, str]] = field(default_factory=list)  # (primary, umbrella)
    hum_id: List[Tuple[str, str]] = field(default_factory=list)    # (bioproject, hum_id)
    geo: List[Tuple[str, str]] = field(default_factory=list)       # (bioproject, geo_id)
    skipped_accessions: List[str] = field(default_factory=list)


def process_bioproject_xml_file(xml_path: Path) -> BioProjectRelations:
    """
    BioProject XML ファイルから各種関連を抽出する。

    1回のパースで以下を抽出:
    - umbrella 関連: <Link><Hierarchical type="TopAdmin">
    - hum-id 関連: <LocalID submission_id="hum...">
    - geo 関連: <CenterID center="GEO">

    Args:
        xml_path: 入力 XML ファイルパス

    Returns:
        BioProjectRelations: 抽出した関連
    """
    result = BioProjectRelations()

    # Package 単位の状態
    current_accession: Optional[str] = None
    current_hum_ids: List[str] = []
    current_geo_ids: List[str] = []
    in_project_id = False

    # Link 単位の状態
    inside_link = False
    current_link: Dict[str, str] = {}

    with xml_path.open("r", encoding="utf-8") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            tag = elem.tag.split("}")[-1]

            # === Package 処理 ===
            if event == "start" and tag == "Package":
                current_accession = None
                current_hum_ids = []
                current_geo_ids = []

            elif event == "start" and tag == "ProjectID":
                in_project_id = True

            elif event == "end" and tag == "ProjectID":
                in_project_id = False

            elif in_project_id and event == "end":
                if tag == "ArchiveID":
                    accession = elem.attrib.get("accession")
                    if accession and accession.startswith("PRJ"):
                        current_accession = accession
                    elif accession:
                        result.skipped_accessions.append(accession)
                    elem.clear()

                elif tag == "LocalID":
                    submission_id = elem.attrib.get("submission_id", "")
                    if submission_id.lower().startswith("hum"):
                        normalized = normalize_hum_id(submission_id)
                        current_hum_ids.append(normalized)
                    elem.clear()

                elif tag == "CenterID":
                    center = elem.attrib.get("center", "")
                    if center == "GEO" and elem.text:
                        geo_id = elem.text.strip()
                        if geo_id:
                            current_geo_ids.append(geo_id)
                    elem.clear()

            elif event == "end" and tag == "Package":
                # Package 終了時に hum-id, geo 関連を追加
                if current_accession:
                    for hum_id in current_hum_ids:
                        result.hum_id.append((current_accession, hum_id))
                    for geo_id in current_geo_ids:
                        result.geo.append((current_accession, geo_id))
                current_accession = None
                current_hum_ids = []
                current_geo_ids = []
                elem.clear()

            # === Link 処理 (umbrella) ===
            elif event == "start" and tag == "Link":
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
                        result.umbrella.append((member_id, project_id))

                inside_link = False
                current_link.clear()
                elem.clear()

    return result


def process_xml_files_parallel(
    xml_files: List[Path],
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> Tuple[IdPairs, IdPairs, IdPairs]:
    """
    XML ファイルを並列処理する。

    Args:
        xml_files: 処理対象の XML ファイルリスト
        parallel_num: 並列処理数

    Returns:
        (umbrella_relations, hum_id_relations, geo_relations)
    """
    umbrella_results: IdPairs = set()
    hum_id_results: IdPairs = set()
    geo_results: IdPairs = set()

    if not xml_files:
        return umbrella_results, hum_id_results, geo_results

    log_info(f"processing {len(xml_files)} XML files with {parallel_num} workers")

    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures: Dict[Future[BioProjectRelations], Path] = {
            executor.submit(process_bioproject_xml_file, xml_path): xml_path
            for xml_path in xml_files
        }

        for future in as_completed(futures):
            xml_path = futures[future]
            try:
                file_result = future.result()
                umbrella_results.update(file_result.umbrella)
                hum_id_results.update(file_result.hum_id)
                geo_results.update(file_result.geo)

                counts = []
                if file_result.umbrella:
                    counts.append(f"{len(file_result.umbrella)} umbrella")
                if file_result.hum_id:
                    counts.append(f"{len(file_result.hum_id)} hum-id")
                if file_result.geo:
                    counts.append(f"{len(file_result.geo)} geo")
                if counts:
                    log_info(f"processed {xml_path.name}: {', '.join(counts)}",
                             file=str(xml_path))

                for acc in file_result.skipped_accessions:
                    log_warn(f"skipping invalid bioproject: {acc}",
                             accession=acc, file=str(xml_path))

            except Exception as e:  # pylint: disable=broad-exception-caught
                log_error(f"error processing {xml_path.name}: {e}",
                          error=e, file=str(xml_path))

    return umbrella_results, hum_id_results, geo_results


def load_bp_blacklist(config: Config) -> Set[str]:
    """BioProject blacklist を読み込む。"""
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


def filter_umbrella_by_blacklist(
    primary_to_umbrella: IdPairs,
    bp_blacklist: Set[str],
) -> IdPairs:
    """umbrella と primary の両方を blacklist でフィルタリング。"""
    original_count = len(primary_to_umbrella)
    filtered = {
        (primary, umbrella)
        for primary, umbrella in primary_to_umbrella
        if primary not in bp_blacklist and umbrella not in bp_blacklist
    }
    removed_count = original_count - len(filtered)
    if removed_count > 0:
        log_info(f"removed {removed_count} umbrella relations by blacklist")
    return filtered


def filter_hum_id_by_blacklist(
    bp_to_hum: IdPairs,
    bp_blacklist: Set[str],
) -> IdPairs:
    """bioproject を blacklist でフィルタリング。"""
    original_count = len(bp_to_hum)
    filtered = {
        (bp, hum_id)
        for bp, hum_id in bp_to_hum
        if bp not in bp_blacklist
    }
    removed_count = original_count - len(filtered)
    if removed_count > 0:
        log_info(f"removed {removed_count} hum-id relations by blacklist")
    return filtered


def filter_geo_by_blacklist(
    bp_to_geo: IdPairs,
    bp_blacklist: Set[str],
) -> IdPairs:
    """bioproject を blacklist でフィルタリング。"""
    original_count = len(bp_to_geo)
    filtered = {
        (bp, geo_id)
        for bp, geo_id in bp_to_geo
        if bp not in bp_blacklist
    }
    removed_count = original_count - len(filtered)
    if removed_count > 0:
        log_info(f"removed {removed_count} geo relations by blacklist")
    return filtered


def process_bioproject_xml(
    config: Config,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> Tuple[IdPairs, IdPairs, IdPairs]:
    """
    BioProject XML から関連を抽出する。

    Returns:
        (umbrella_relations, hum_id_relations, geo_relations)
    """
    tmp_xml_dir = get_tmp_xml_dir(config, "bioproject")

    umbrella_all: IdPairs = set()
    hum_id_all: IdPairs = set()
    geo_all: IdPairs = set()

    # NCBI files
    ncbi_files = sorted(tmp_xml_dir.glob("ncbi_*.xml"))
    if ncbi_files:
        log_info(f"found {len(ncbi_files)} NCBI XML files")
        umbrella, hum_id, geo = process_xml_files_parallel(ncbi_files, parallel_num)
        umbrella_all.update(umbrella)
        hum_id_all.update(hum_id)
        geo_all.update(geo)
        log_info(f"NCBI: {len(umbrella)} umbrella, {len(hum_id)} hum-id, {len(geo)} geo relations")
    else:
        log_warn(f"no NCBI XML files found in {tmp_xml_dir}")

    # DDBJ files
    ddbj_files = sorted(tmp_xml_dir.glob("ddbj_*.xml"))
    if ddbj_files:
        log_info(f"found {len(ddbj_files)} DDBJ XML files")
        umbrella, hum_id, geo = process_xml_files_parallel(ddbj_files, parallel_num)
        umbrella_all.update(umbrella)
        hum_id_all.update(hum_id)
        geo_all.update(geo)
        log_info(f"DDBJ: {len(umbrella)} umbrella, {len(hum_id)} hum-id, {len(geo)} geo relations")
    else:
        log_warn(f"no DDBJ XML files found in {tmp_xml_dir}")

    return umbrella_all, hum_id_all, geo_all


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist = load_bp_blacklist(config)

        # Extract relations from XML
        umbrella_relations, hum_id_relations, geo_relations = process_bioproject_xml(config)

        log_info(f"total {len(umbrella_relations)} umbrella relations (before filter)")
        log_info(f"total {len(hum_id_relations)} hum-id relations (before filter)")
        log_info(f"total {len(geo_relations)} geo relations (before filter)")

        # Filter by blacklist
        umbrella_relations = filter_umbrella_by_blacklist(umbrella_relations, bp_blacklist)
        hum_id_relations = filter_hum_id_by_blacklist(hum_id_relations, bp_blacklist)
        geo_relations = filter_geo_by_blacklist(geo_relations, bp_blacklist)

        log_info(f"total {len(umbrella_relations)} umbrella relations (after filter)")
        log_info(f"total {len(hum_id_relations)} hum-id relations (after filter)")
        log_info(f"total {len(geo_relations)} geo relations (after filter)")

        # Load to DB
        if umbrella_relations:
            load_to_db(config, umbrella_relations, "bioproject", "umbrella-bioproject")

        if hum_id_relations:
            load_to_db(config, hum_id_relations, "bioproject", "hum-id")

        if geo_relations:
            load_to_db(config, geo_relations, "bioproject", "geo")


if __name__ == "__main__":
    main()
