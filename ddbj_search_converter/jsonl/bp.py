"""BioProject JSONL 生成モジュール。"""
import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional, Set, Tuple

import xmltodict

from ddbj_search_converter.config import (BP_JSONL_DIR_NAME, TODAY_STR, Config,
                                          get_config)
from ddbj_search_converter.dblink.db import (AccessionType,
                                             get_related_entities_bulk)
from ddbj_search_converter.dblink.utils import load_blacklist
from ddbj_search_converter.jsonl.utils import to_xref
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  log_warn, run_logger)
from ddbj_search_converter.schema import (Agency, BioProject, Distribution,
                                          ExternalLink, Grant, Organism,
                                          Organization, Publication, Xref)

DEFAULT_BATCH_SIZE = 2000
DEFAULT_PARALLEL_NUM = 64
TMP_XML_DIR_NAME = "tmp_xml"

EXTERNAL_LINK_MAP = {
    "GEO": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=",
    "dbGaP": "https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=",
    "ENA-SUBMISSION": "https://www.ebi.ac.uk/ena/browser/view/",
    "SRA": "https://www.ncbi.nlm.nih.gov/sra/",
    "PUBMED": "https://pubmed.ncbi.nlm.nih.gov/",
    "DOI": "https://doi.org/",
    "SRA|http": "https:",
    "3000 rice genomes on aws|https": "https",
    "ENA|http": "https:",
}


# === Parse functions ===


def parse_object_type(project: Dict[str, Any]) -> Literal["BioProject", "UmbrellaBioProject"]:
    """BioProject のオブジェクトタイプを判定する。"""
    if project.get("Project", {}).get("ProjectType", {}).get("ProjectTypeTopAdmin"):
        return "UmbrellaBioProject"
    return "BioProject"


def parse_organism(project: Dict[str, Any], is_ddbj: bool, accession: str = "") -> Optional[Organism]:
    """BioProject から Organism を抽出する。"""
    try:
        if is_ddbj:
            organism_obj = (
                project.get("Project", {})
                .get("ProjectType", {})
                .get("ProjectTypeTopAdmin", {})
                .get("Organism")
            )
        else:
            organism_obj = (
                project.get("Project", {})
                .get("ProjectType", {})
                .get("ProjectTypeSubmission", {})
                .get("Target", {})
                .get("Organism")
            )
        if organism_obj is None:
            return None
        return Organism(
            identifier=str(organism_obj.get("taxID", "")),
            name=organism_obj.get("OrganismName"),
        )
    except Exception as e:
        log_warn(f"Failed to parse organism: {e}", accession=accession)
        return None


def parse_title(project: Dict[str, Any], accession: str = "") -> Optional[str]:
    """BioProject から title を抽出する。"""
    try:
        title = project.get("Project", {}).get("ProjectDescr", {}).get("Title")
        return str(title) if title is not None else None
    except Exception as e:
        log_warn(f"Failed to parse title: {e}", accession=accession)
        return None


def parse_description(project: Dict[str, Any], accession: str = "") -> Optional[str]:
    """BioProject から description を抽出する。"""
    try:
        description = project.get("Project", {}).get("ProjectDescr", {}).get("Description")
        return str(description) if description is not None else None
    except Exception as e:
        log_warn(f"Failed to parse description: {e}", accession=accession)
        return None


def parse_organization(project: Dict[str, Any], is_ddbj: bool, accession: str = "") -> List[Organization]:
    """BioProject から Organization を抽出する。"""
    organizations: List[Organization] = []
    try:
        if is_ddbj:
            organization = (
                project.get("Submission", {})
                .get("Submission", {})
                .get("Description", {})
                .get("Organization")
            )
        else:
            organization = (
                project.get("Project", {})
                .get("ProjectDescr", {})
                .get("Organization")
            )
        if organization is None:
            return []

        org_list = organization if isinstance(organization, list) else [organization]
        for item in org_list:
            name = item.get("Name")
            org_type = item.get("type")
            role = item.get("role")
            url = item.get("url")
            if isinstance(name, str):
                organizations.append(Organization(
                    name=name,
                    organizationType=org_type,
                    role=role,
                    url=url,
                    abbreviation=None,
                ))
            elif isinstance(name, dict):
                content_name = name.get("content")
                if content_name is not None:
                    organizations.append(Organization(
                        name=content_name,
                        organizationType=org_type,
                        role=role,
                        url=url,
                        abbreviation=name.get("abbr"),
                    ))
    except Exception as e:
        log_warn(f"Failed to parse organization: {e}", accession=accession)
    return organizations


def parse_publication(project: Dict[str, Any], accession: str = "") -> List[Publication]:
    """BioProject から Publication を抽出する。"""
    publications: List[Publication] = []
    try:
        publication = project.get("Project", {}).get("ProjectDescr", {}).get("Publication")
        if publication is None:
            return []

        pub_list = publication if isinstance(publication, list) else [publication]
        for item in pub_list:
            id_ = item.get("id")
            dbtype = item.get("DbType")
            publication_url = None
            if dbtype == "DOI":
                publication_url = f"https://doi.org/{id_}"
            elif dbtype == "ePubmed":
                publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
            elif dbtype is not None and dbtype.isdigit():
                dbtype = "ePubmed"
                publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
            publications.append(Publication(
                title=item.get("StructuredCitation", {}).get("Title"),
                date=item.get("date"),
                Reference=item.get("Reference"),
                id=id_,
                url=publication_url,
                DbType=dbtype,
                status=item.get("status"),
            ))
    except Exception as e:
        log_warn(f"Failed to parse publication: {e}", accession=accession)
    return publications


def parse_grant(project: Dict[str, Any], accession: str = "") -> List[Grant]:
    """BioProject から Grant を抽出する。"""
    grants: List[Grant] = []
    try:
        grant = project.get("Project", {}).get("ProjectDescr", {}).get("Grant")
        if grant is None:
            return []

        grant_list = grant if isinstance(grant, list) else [grant]
        for item in grant_list:
            agency = item.get("Agency")
            if isinstance(agency, str):
                grants.append(Grant(
                    id=item.get("GrantId"),
                    title=item.get("Title"),
                    agency=[Agency(abbreviation=agency, name=agency)]
                ))
            elif isinstance(agency, dict):
                grants.append(Grant(
                    id=item.get("GrantId"),
                    title=item.get("Title"),
                    agency=[Agency(
                        abbreviation=agency.get("abbr"),
                        name=agency.get("content"),
                    )]
                ))
    except Exception as e:
        log_warn(f"Failed to parse grant: {e}", accession=accession)
    return grants


def parse_external_link(project: Dict[str, Any], accession: str = "") -> List[ExternalLink]:
    """BioProject から ExternalLink を抽出する。"""
    def _obj_to_external_link(obj: Dict[str, Any]) -> Optional[ExternalLink]:
        url = obj.get("URL")
        if url is not None:
            label = obj.get("label")
            return ExternalLink(url=url, label=label if label is not None else url)

        db_xref = obj.get("dbXREF")
        if db_xref is not None:
            db = db_xref.get("db")
            id_ = db_xref.get("ID")
            if db is not None and id_ is not None:
                if db in EXTERNAL_LINK_MAP:
                    url = EXTERNAL_LINK_MAP[db] + id_
                    label = obj.get("label", id_)
                    return ExternalLink(url=url, label=label)
                else:
                    log_warn(f"Unsupported ExternalLink db: {db}", accession=accession)
        return None

    links: List[ExternalLink] = []
    try:
        external_link_obj = project.get("Project", {}).get("ProjectDescr", {}).get("ExternalLink")
        if external_link_obj is None:
            return []

        if isinstance(external_link_obj, dict):
            link = _obj_to_external_link(external_link_obj)
            if link is not None:
                links.append(link)
        elif isinstance(external_link_obj, list):
            for item in external_link_obj:
                link = _obj_to_external_link(item)
                if link is not None:
                    links.append(link)
    except Exception as e:
        log_warn(f"Failed to parse external_link: {e}", accession=accession)
    return links


def parse_same_as(project: Dict[str, Any], accession: str = "") -> List[Xref]:
    """BioProject から sameAs (GEO) を抽出する。"""
    def _to_geo_xref(center_obj: Dict[str, Any]) -> Optional[Xref]:
        id_ = center_obj.get("content")
        type_ = center_obj.get("center")
        if id_ is None or type_ != "GEO":
            return None
        return Xref(
            identifier=id_,
            type="geo",
            url=f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={id_}",
        )

    try:
        center_obj = project.get("Project", {}).get("ProjectID", {}).get("CenterID")
        if center_obj is None:
            return []

        if isinstance(center_obj, list):
            return [xref for xref in [_to_geo_xref(item) for item in center_obj] if xref is not None]
        elif isinstance(center_obj, dict):
            xref = _to_geo_xref(center_obj)
            return [xref] if xref is not None else []
    except Exception as e:
        log_warn(f"Failed to parse same_as: {e}", accession=accession)
    return []


def parse_status(project: Dict[str, Any], is_ddbj: bool) -> str:
    """BioProject から status を抽出する。"""
    if is_ddbj:
        return "public"
    status = project.get("Submission", {}).get("Description", {}).get("Access", "public")
    return status if isinstance(status, str) else "public"


def parse_date_from_xml(
    project: Dict[str, Any]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """XML から日付を抽出する (NCBI 用)。"""
    date_created = project.get("Submission", {}).get("submitted")
    date_modified = project.get("Submission", {}).get("last_update")
    date_published = project.get("Project", {}).get("ProjectDescr", {}).get("ProjectReleaseDate")
    return date_created, date_modified, date_published


# === Properties normalization ===


def normalize_properties(project: Dict[str, Any]) -> None:
    """properties 内の値を正規化する。"""
    _normalize_biosample_set_id(project)
    _normalize_locus_tag_prefix(project)
    _normalize_local_id(project)
    _normalize_organization_name(project)
    _normalize_grant_agency(project)


def _normalize_biosample_set_id(project: Dict[str, Any]) -> None:
    """BioSampleSet.ID を正規化する。"""
    try:
        bs_set_ids = (
            project.get("Project", {})
            .get("ProjectType", {})
            .get("ProjectTypeSubmission", {})
            .get("Target", {})
            .get("BioSampleSet", {})
            .get("ID")
        )
        if bs_set_ids is None:
            return
        if isinstance(bs_set_ids, list):
            for i, item in enumerate(bs_set_ids):
                if isinstance(item, str):
                    project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"][i] = {"content": item}
        elif isinstance(bs_set_ids, str):
            project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] = {"content": bs_set_ids}
    except Exception:
        pass


def _normalize_locus_tag_prefix(project: Dict[str, Any]) -> None:
    """LocusTagPrefix を正規化する。"""
    try:
        prefix = project.get("Project", {}).get("ProjectDescr", {}).get("LocusTagPrefix")
        if prefix is None:
            return
        if isinstance(prefix, list):
            for i, item in enumerate(prefix):
                if isinstance(item, str):
                    project["Project"]["ProjectDescr"]["LocusTagPrefix"][i] = {"content": item}
        elif isinstance(prefix, str):
            project["Project"]["ProjectDescr"]["LocusTagPrefix"] = {"content": prefix}
    except Exception:
        pass


def _normalize_local_id(project: Dict[str, Any]) -> None:
    """LocalID を正規化する。"""
    try:
        local_id = project.get("Project", {}).get("ProjectID", {}).get("LocalID")
        if local_id is None:
            return
        if isinstance(local_id, list):
            for i, item in enumerate(local_id):
                if isinstance(item, str):
                    project["Project"]["ProjectID"]["LocalID"][i] = {"content": item}
        elif isinstance(local_id, str):
            project["Project"]["ProjectID"]["LocalID"] = {"content": local_id}
    except Exception:
        pass


def _normalize_organization_name(project: Dict[str, Any]) -> None:
    """Organization.Name を正規化する。"""
    def _normalize_single(organization: Dict[str, Any], parent: Any, key: Any) -> None:
        name = organization.get("Name")
        if isinstance(name, str):
            if isinstance(key, int):
                parent[key]["Name"] = {"content": name}
            else:
                parent["Name"] = {"content": name}

    def _normalize_org(org: Any) -> None:
        if org is None:
            return
        if isinstance(org, list):
            for i, item in enumerate(org):
                _normalize_single(item, org, i)
        elif isinstance(org, dict):
            _normalize_single(org, org, None)

    try:
        # DDBJ case: project["Submission"]["Submission"]["Description"]["Organization"]
        submission = project.get("Submission", {})
        if "Submission" in submission:
            org = submission.get("Submission", {}).get("Description", {}).get("Organization")
            _normalize_org(org)

        # NCBI case: project["Project"]["ProjectDescr"]["Organization"]
        org = project.get("Project", {}).get("ProjectDescr", {}).get("Organization")
        _normalize_org(org)
    except Exception:
        pass


def _normalize_grant_agency(project: Dict[str, Any]) -> None:
    """Grant.Agency を正規化する。"""
    try:
        grant = project.get("Project", {}).get("ProjectDescr", {}).get("Grant")
        if grant is None:
            return

        def _normalize_single(g: Dict[str, Any]) -> None:
            agency = g.get("Agency")
            if isinstance(agency, str):
                g["Agency"] = {"abbr": agency, "content": agency}

        if isinstance(grant, list):
            for item in grant:
                _normalize_single(item)
        elif isinstance(grant, dict):
            _normalize_single(grant)
    except Exception:
        pass


# === Conversion ===


def xml_entry_to_bp_instance(entry: Dict[str, Any], is_ddbj: bool) -> BioProject:
    """XML エントリを BioProject インスタンスに変換する。"""
    project = entry["Project"]
    accession = project["Project"]["ProjectID"]["ArchiveID"]["accession"]

    # properties 内の値を正規化
    normalize_properties(project)

    return BioProject(
        identifier=accession,
        properties={"Project": project},
        distribution=[Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl=f"https://ddbj.nig.ac.jp/search/entry/bioproject/{accession}.json",
        )],
        isPartOf="BioProject",
        type="bioproject",
        objectType=parse_object_type(project),
        name=None,
        url=f"https://ddbj.nig.ac.jp/search/entry/bioproject/{accession}",
        organism=parse_organism(project, is_ddbj, accession),
        title=parse_title(project, accession),
        description=parse_description(project, accession),
        organization=parse_organization(project, is_ddbj, accession),
        publication=parse_publication(project, accession),
        grant=parse_grant(project, accession),
        externalLink=parse_external_link(project, accession),
        dbXref=[],  # 後で更新
        sameAs=parse_same_as(project, accession),
        status=parse_status(project, is_ddbj),
        visibility="unrestricted-access",
        dateCreated=None,  # 後で更新
        dateModified=None,  # 後で更新
        datePublished=None,  # 後で更新
    )


# === DBLink ===


INDEX_TO_ACCESSION_TYPE: Dict[str, AccessionType] = {
    "bioproject": "bioproject",
    "umbrella-bioproject": "umbrella-bioproject",
}


def get_dbxref_map(config: Config, accessions: List[str]) -> Dict[str, List[Xref]]:
    """dblink DB から関連エントリを取得し、Xref リストに変換する。"""
    if not accessions:
        return {}

    relations = get_related_entities_bulk(
        config, entity_type="bioproject", accessions=accessions
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


# === Output ===


def write_jsonl(output_path: Path, docs: List[BioProject]) -> None:
    """BioProject インスタンスのリストを JSONL ファイルに書き込む。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(doc.model_dump_json(by_alias=True) for doc in docs))


# === XML iteration ===


def iterate_xml_packages(xml_path: Path) -> Iterator[bytes]:
    """XML ファイルから <Package> 要素を順に抽出する。"""
    inside_package = False
    buffer = bytearray()
    with xml_path.open(mode="rb") as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line.startswith(b"<Package>"):
                inside_package = True
                buffer = bytearray(line)
            elif stripped_line.startswith(b"</Package>"):
                inside_package = False
                buffer.extend(line)
                yield bytes(buffer)
                buffer.clear()
            elif inside_package:
                buffer.extend(line)


# === Processing ===


def _process_xml_file_worker(
    config: Config, xml_path: Path, output_path: Path, is_ddbj: bool,
    bp_blacklist: Set[str],
) -> int:
    """XML ファイルを処理して JSONL を出力するワーカー関数。"""
    log_info(f"Processing {xml_path.name} -> {output_path.name}")

    docs: Dict[str, BioProject] = {}
    skipped_count = 0
    for xml_element in iterate_xml_packages(xml_path):
        try:
            metadata = xmltodict.parse(
                xml_element, attr_prefix="", cdata_key="content", process_namespaces=False
            )
            bp_instance = xml_entry_to_bp_instance(metadata["Package"], is_ddbj)
            if bp_instance.identifier in bp_blacklist:
                skipped_count += 1
                continue
            docs[bp_instance.identifier] = bp_instance
        except Exception as e:
            log_warn(f"Failed to parse XML element: {e}")

    if skipped_count > 0:
        log_info(f"Skipped {skipped_count} blacklisted entries")

    # dbXref を一括取得
    dbxref_map = get_dbxref_map(config, list(docs.keys()))
    for accession, xrefs in dbxref_map.items():
        if accession in docs:
            docs[accession].dbXref = xrefs

    # 日付を取得
    if is_ddbj:
        # DDBJ は PostgreSQL から取得
        try:
            from ddbj_search_converter.postgres.bp_date import \
                fetch_bp_dates_bulk  # pylint: disable=import-outside-toplevel
            date_map = fetch_bp_dates_bulk(config, docs.keys())
            for accession, (date_created, date_modified, date_published) in date_map.items():
                if accession in docs:
                    docs[accession].dateCreated = date_created
                    docs[accession].dateModified = date_modified
                    docs[accession].datePublished = date_published
        except ImportError:
            log_warn("psycopg2 not available, skipping date fetch for DDBJ BioProject")
    else:
        # NCBI は XML から取得
        for xml_element in iterate_xml_packages(xml_path):
            try:
                metadata = xmltodict.parse(
                    xml_element, attr_prefix="", cdata_key="content", process_namespaces=False
                )
                project = metadata["Package"]["Project"]
                accession = project["Project"]["ProjectID"]["ArchiveID"]["accession"]
                if accession in docs:
                    date_created, date_modified, date_published = parse_date_from_xml(project)
                    docs[accession].dateCreated = date_created
                    docs[accession].dateModified = date_modified
                    docs[accession].datePublished = date_published
            except Exception:
                pass

    write_jsonl(output_path, list(docs.values()))
    log_info(f"Wrote {len(docs)} entries to {output_path}")

    return len(docs)


def process_xml_file(
    config: Config, xml_path: Path, output_path: Path, is_ddbj: bool,
    bp_blacklist: Optional[Set[str]] = None,
) -> int:
    """単一の XML ファイルを処理して JSONL を出力する。"""
    if bp_blacklist is None:
        bp_blacklist, _ = load_blacklist(config)
    return _process_xml_file_worker(config, xml_path, output_path, is_ddbj, bp_blacklist)


def generate_bioproject_jsonl(
    config: Config,
    output_dir: Path,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
) -> None:
    """
    BioProject JSONL ファイルを生成する。

    tmp_xml ディレクトリから分割済み XML を取得して並列処理する。
    """
    tmp_xml_dir = output_dir.joinpath(TMP_XML_DIR_NAME)
    if not tmp_xml_dir.exists():
        raise FileNotFoundError(f"tmp_xml directory not found: {tmp_xml_dir}")

    # blacklist を読み込む
    bp_blacklist, _ = load_blacklist(config)

    # DDBJ XML と NCBI XML をそれぞれ処理
    ddbj_xml_files = sorted(tmp_xml_dir.glob("ddbj_bioproject_*.xml"))
    ncbi_xml_files = sorted(tmp_xml_dir.glob("bioproject_*.xml"))

    log_info(f"Found {len(ddbj_xml_files)} DDBJ XML files and {len(ncbi_xml_files)} NCBI XML files")

    tasks: List[Tuple[Path, Path, bool]] = []
    for xml_file in ddbj_xml_files:
        output_path = output_dir.joinpath(xml_file.stem + ".jsonl")
        tasks.append((xml_file, output_path, True))
    for xml_file in ncbi_xml_files:
        output_path = output_dir.joinpath(xml_file.stem + ".jsonl")
        tasks.append((xml_file, output_path, False))

    total_count = 0
    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures = {
            executor.submit(
                _process_xml_file_worker, config, xml_path, output_path, is_ddbj, bp_blacklist
            ): (xml_path, is_ddbj)
            for xml_path, output_path, is_ddbj in tasks
        }
        for future in as_completed(futures):
            xml_path, is_ddbj = futures[future]
            try:
                count = future.result()
                total_count += count
            except Exception as e:
                log_warn(f"Failed to process {xml_path}: {e}")

    log_info(f"Generated {total_count} BioProject entries in total")


# === CLI ===


def parse_args(args: List[str]) -> Tuple[Config, Path, int]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(
        description="Generate BioProject JSONL files from split XML files."
    )
    parser.add_argument(
        "--result-dir",
        help=f"Base directory for output. Default: $PWD/ddbj_search_converter_results. "
        f"Output will be stored in {{result_dir}}/{BP_JSONL_DIR_NAME}/{{date}}/.",
        default=None,
    )
    parser.add_argument(
        "--parallel-num",
        help=f"Number of parallel workers. Default: {DEFAULT_PARALLEL_NUM}",
        type=int,
        default=DEFAULT_PARALLEL_NUM,
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

    output_dir = config.result_dir.joinpath(BP_JSONL_DIR_NAME, TODAY_STR)

    return config, output_dir, parsed.parallel_num


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, parallel_num = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_bioproject_jsonl", config=config):
        log_debug(f"Config: {config.model_dump_json(indent=2)}")
        log_debug(f"Output directory: {output_dir}")
        log_debug(f"Parallel workers: {parallel_num}")

        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"Output directory: {output_dir}")

        generate_bioproject_jsonl(config, output_dir, parallel_num)


if __name__ == "__main__":
    main()
