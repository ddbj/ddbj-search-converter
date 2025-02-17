"""\
- BioProject XML を JSON-Lines に変換する
- is_ddbj で、それぞれ処理が分岐する
    - ddbj xml (/usr/local/resources/bioproject/ddbj_bioproject.xml) の場合は、work_dir/${date}/ddbj_bioproject.jsonl に出力
    - それ以外 (/usr/local/resources/bioproject/bioproject.xml) の場合は、work_dir/${date}/bioproject_${n}.jsonl に BATCH_SIZE (2000) 件ずつ出力
        - -> ddbj_xml と common_xml という呼び方をすることにする
- 生成される JSON-Lines は 1 line が 1 BioProject Accession に対応する
- 並列化処理について:
    - is_ddbj の場合は、並列化されない
    - それ以外の場合は、先に xml を分割してから、並列化して処理する
"""
import argparse
import shutil
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import xmltodict
from pydantic import BaseModel

from ddbj_search_converter.cache_db.bp_date import get_dates as get_bp_dates
from ddbj_search_converter.cache_db.bp_relation_ids import \
    get_relation_ids as get_bp_relation_ids
from ddbj_search_converter.cache_db.to_xref import to_xref
from ddbj_search_converter.config import (BP_JSONL_DIR_NAME, LOGGER, TODAY,
                                          Config, get_config,
                                          set_logging_level)
from ddbj_search_converter.schema import (Agency, BioProject, Distribution,
                                          ExternalLink, Grant, Organism,
                                          Organization, Publication, Xref)

DEFAULT_BATCH_SIZE = 2000
DEFAULT_PARALLEL_NUM = 32
TMP_XML_DIR_NAME = "tmp_xml"
TMP_XML_FILE_NAME = "bioproject_{n}.xml"
DDBJ_JSONL_FILE_NAME = "ddbj_bioproject.jsonl"
COMMON_JSONL_FILE_NAME = "bioproject_{n}.jsonl"


def xml_to_jsonl(
    config: Config,
    xml_file: Path,
    output_dir: Path,
    is_ddbj: bool,
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallel_num: int = DEFAULT_PARALLEL_NUM,
    remove_tmp_dir: bool = False,
) -> None:
    if is_ddbj is True:
        jsonl_file = output_dir.joinpath(DDBJ_JSONL_FILE_NAME)
        xml_to_jsonl_worker(config, xml_file, jsonl_file, is_ddbj, batch_size)
        return

    # common xml の場合は、先に xml を分割してから、並列化して処理する
    tmp_xml_dir = output_dir.joinpath(TMP_XML_DIR_NAME)
    tmp_xml_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Splitting XML file: %s", xml_file)
    tmp_xml_files = split_xml(xml_file, tmp_xml_dir, batch_size)
    jsonl_files = [output_dir.joinpath(f"{xml_file.stem}.jsonl") for xml_file in tmp_xml_files]

    LOGGER.info("Starting parallel conversion of XML to JSON Lines. A total of %d JSON Lines files will be generated.", len(tmp_xml_files))

    with ProcessPoolExecutor(max_workers=parallel_num) as executor:
        futures = [
            executor.submit(
                xml_to_jsonl_worker,
                config,
                tmp_xml_file,
                jsonl_file,
                is_ddbj,
                batch_size,
            )
            for tmp_xml_file, jsonl_file in zip(tmp_xml_files, jsonl_files)
        ]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                LOGGER.error("Failed to convert XML to JSON-Lines: %s", e)

    # 一時ファイルを削除
    if remove_tmp_dir:
        shutil.rmtree(tmp_xml_dir)


def _iterate_xml_element(xml_file: Path) -> Generator[bytes, None, None]:
    """\
    - XML file を行ごとに読み、<Package> ... </Package> の部分を抽出する
    """
    inside_package = False
    buffer = bytearray()
    with xml_file.open(mode="rb") as f:
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


def split_xml(xml_file: Path, output_dir: Path, batch_size: int = DEFAULT_BATCH_SIZE) -> List[Path]:
    head = b'<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>\n'
    tail = b'</PackageSet>'

    output_files: List[Path] = []
    batch_buffer: List[bytes] = []
    file_count = 1
    for xml_element in _iterate_xml_element(xml_file):
        batch_buffer.append(xml_element)

        if len(batch_buffer) >= batch_size:
            output_file = output_dir.joinpath(TMP_XML_FILE_NAME.format(n=file_count))
            output_files.append(output_file)
            with output_file.open(mode="wb") as f:
                f.write(head)
                f.writelines(batch_buffer)
                f.write(tail)
            file_count += 1
            batch_buffer.clear()

    if len(batch_buffer) > 0:
        output_file = output_dir.joinpath(TMP_XML_FILE_NAME.format(n=file_count))
        output_files.append(output_file)
        with output_file.open(mode="wb") as f:
            f.write(head)
            f.writelines(batch_buffer)
            f.write(tail)
        batch_buffer.clear()

    return output_files


def xml_to_jsonl_worker(config: Config, xml_file: Path, jsonl_file: Path, is_ddbj: bool, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    LOGGER.info("Converting XML to JSON-Lines: %s", jsonl_file.name)

    docs: List[BioProject] = []
    for xml_element in _iterate_xml_element(xml_file):
        bp_instance = xml_element_to_bp_instance(config, xml_element, is_ddbj)
        docs.append(bp_instance)

        if len(docs) >= batch_size:
            write_jsonl(jsonl_file, docs, is_append=True)
            docs.clear()

    if len(docs) > 0:
        write_jsonl(jsonl_file, docs, is_append=True)
        docs.clear()


def xml_element_to_bp_instance(config: Config, xml_element: bytes, is_ddbj: bool) -> BioProject:
    metadata = xmltodict.parse(xml_element, attr_prefix="", cdata_key="content", process_namespaces=False)

    project = metadata["Package"]["Project"]
    accession = project["Project"]["ProjectID"]["ArchiveID"]["accession"]

    if is_ddbj:
        date_created, date_modified, date_published = get_bp_dates(config, accession)
    else:
        date_created, date_modified, date_published = _parse_date(project)

    bp_instance = BioProject(
        identifier=accession,
        properties={"Project": project},
        distribution=[Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl=f"https://ddbj.nig.ac.jp/search/entry/bioproject/{accession}.json",
        )],
        isPartOf="BioProject",
        type="bioproject",
        objectType=_parse_object_type(project),
        name=None,
        url=f"https://ddbj.nig.ac.jp/search/entry/bioproject/{accession}",
        organism=_parse_organism(accession, project, is_ddbj),
        title=_parse_title(accession, project),
        description=_parse_description(accession, project),
        organization=_parse_and_update_organization(accession, project, is_ddbj),
        publication=_parse_and_update_publication(accession, project),
        grant=_parse_and_update_grant(accession, project),
        externalLink=_parse_external_link(accession, project),
        dbXref=[to_xref(id_) for id_ in get_bp_relation_ids(config, accession)],
        sameAs=_parse_same_as(accession, project),
        status=_parse_status(project, is_ddbj),
        visibility="unrestricted-access",
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )

    # properties の中の object に対して整形を行う
    _update_prefix(accession, project)
    _update_locus_tag_prefix(accession, project)
    _update_local_id(accession, project)

    return bp_instance


def _parse_object_type(project: Dict[str, Any]) -> Literal["BioProject", "UmbrellaBioProject"]:
    # umbrella の判定
    # 要検討: 例外発生のコストが高いため通常の判定には使わない方が良いという判断で try&if case で ProjectTypeTopAdmin の存在を確認
    if project.get("Project", {}).get("ProjectType", {}).get("ProjectTypeTopAdmin"):
        return "UmbrellaBioProject"

    return "BioProject"


def _parse_organism(accession: str, project: Dict[str, Any], is_ddbj: bool) -> Optional[Organism]:
    try:
        if is_ddbj:
            organism_obj = project.get("Project", {}).get("ProjectType", {}).get("ProjectTypeTopAdmin", {}).get("Organism", None)
        else:
            organism_obj = project.get("Project", {}).get("ProjectType", {}).get("ProjectTypeSubmission", {}).get("Target", {}).get("Organism", None)
        if organism_obj is None:
            return None

        return Organism(
            identifier=str(organism_obj["taxID"]),
            name=organism_obj["OrganismName"],
        )
    except Exception as e:
        LOGGER.warning("Failed to parse organism with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())
        return None


def _parse_title(accession: str, project: Dict[str, Any]) -> Optional[str]:
    try:
        title = project["Project"]["ProjectDescr"]["Title"]
        return str(title)
    except Exception as e:
        LOGGER.warning("Failed to parse title with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())
        return None


def _parse_description(accession: str, project: Dict[str, Any]) -> Optional[str]:
    try:
        description = project.get("Project", {}).get("ProjectDescr", {}).get("Description", None)
        if description is None:
            return None

        return str(description)
    except Exception as e:
        LOGGER.warning("Failed to parse description with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())
        return None


def _parse_and_update_organization(accession: str, project: Dict[str, Any], is_ddbj: bool) -> List[Organization]:
    """\
    Organization の parse、引数の project の update も行う
    """
    organizations: List[Organization] = []

    try:
        if is_ddbj:
            organization = project.get("Submission", {}).get("Submission", {}).get("Description", {}).get("Organization", None)
        else:
            organization = project.get("Project", {}).get("ProjectDescr", {}).get("Organization", None)
        if organization is None:
            return []

        if isinstance(organization, list):
            for i, item in enumerate(organization):
                name = item.get("Name", None)
                _type = item.get("type", None)
                role = item.get("role", None)
                url = item.get("url", None)
                if isinstance(name, str):
                    # 引数の project の update を行う
                    project["Submission"]["Description"]["Organization"][i]["Name"] = {"content": name}
                    organizations.append(Organization(
                        name=name,
                        organizationType=_type,
                        role=role,
                        url=url,
                        abbreviation=None,
                    ))
                elif isinstance(name, dict):
                    content_name = name.get("content", None)
                    if content_name is not None:
                        abbreviation = name.get("abbr", None)
                        organizations.append(Organization(
                            name=content_name,
                            organizationType=_type,
                            role=role,
                            url=url,
                            abbreviation=abbreviation,
                        ))
        elif isinstance(organization, dict):
            name = organization.get("Name", None)
            _type = organization.get("type", None)
            role = organization.get("role", None)
            url = organization.get("url", None)
            if isinstance(name, str):
                # 引数の project の update を行う
                project["Submission"]["Description"]["Organization"]["Name"] = {"content": name}
                organizations.append(Organization(
                    name=name,
                    organizationType=_type,
                    role=role,
                    url=url,
                    abbreviation=None,
                ))
            elif isinstance(name, dict):
                content_name = name.get("content", None)
                if content_name is not None:
                    abbreviation = name.get("abbr", None)
                    organizations.append(Organization(
                        name=content_name,
                        organizationType=_type,
                        role=role,
                        url=url,
                        abbreviation=abbreviation,
                    ))

    except Exception as e:
        LOGGER.warning("Failed to parse organization with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return organizations


def _parse_and_update_publication(accession: str, project: Dict[str, Any]) -> List[Publication]:
    """\
    Publication の parse、引数の project の update も行う
    """
    publications: List[Publication] = []

    try:
        publication = project.get("Project", {}).get("ProjectDescr", {}).get("Publication", None)
        if publication is None:
            return []

        if isinstance(publication, list):
            # 引数の project の update を行う
            project["Project"]["ProjectDescr"]["Publication"] = publication

            for item in publication:
                id_ = item.get("id", None)
                dbtype = item.get("DbType", None)
                publication_url = None
                if dbtype == "DOI":
                    publication_url = f"https://doi.org/{id_}"
                elif dbtype == "ePubmed":
                    publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
                elif dbtype.isdigit():
                    dbtype = "ePubmed"
                    publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
                publications.append(Publication(
                    title=item.get("StructuredCitation", {}).get("Title", None),
                    date=item.get("date", None),
                    Reference=item.get("Reference", None),
                    id=id_,
                    url=publication_url,
                    DbType=dbtype,
                    status=item.get("status", None),
                ))
        elif isinstance(publication, dict):
            id_ = publication.get("id", None)
            dbtype = publication.get("DbType", None)
            publication_url = None
            if dbtype == "DOI":
                publication_url = f"https://doi.org/{id_}"
            elif dbtype == "ePubmed":
                publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
            elif dbtype.isdigit():
                dbtype = "ePubmed"
                publication_url = f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
            publications.append(Publication(
                title=publication.get("StructuredCitation", {}).get("Title", None),
                date=publication.get("date", None),
                Reference=publication.get("Reference", None),
                id=id_,
                url=publication_url,
                DbType=dbtype,
                status=publication.get("status", None),
            ))

    except Exception as e:
        LOGGER.warning("Failed to parse publication with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return publications


def _parse_and_update_grant(accession: str, project: Dict[str, Any]) -> List[Grant]:
    grants: List[Grant] = []

    try:
        grant = project.get("Project", {}).get("ProjectDescr", {}).get("Grant", None)
        if grant is None:
            return []

        if isinstance(grant, list):
            for i, item in enumerate(grant):
                agency = item.get("Agency", None)
                if isinstance(agency, str):
                    # 引数の project の update を行う
                    project["Project"]["ProjectDescr"]["Grant"][i]["Agency"] = {"abbr": agency, "content": agency}
                    grants.append(Grant(
                        id=item.get("GrantId", None),
                        title=item.get("Title", None),
                        agency=[Agency(
                            abbreviation=agency,
                            name=agency,
                        )]
                    ))
                elif isinstance(agency, dict):
                    grants.append(Grant(
                        id=item.get("GrantId", None),
                        title=item.get("Title", None),
                        agency=[Agency(
                            abbreviation=agency.get("abbr", None),
                            name=agency.get("content", None),
                        )]
                    ))
        elif isinstance(grant, dict):
            agency = grant.get("Agency", None)
            if isinstance(agency, str):
                project["Project"]["ProjectDescr"]["Grant"]["Agency"] = {"abbr": agency, "content": agency}
                grants.append(Grant(
                    id=grant.get("GrantId", None),
                    title=grant.get("Title", None),
                    agency=[Agency(
                        abbreviation=agency,
                        name=agency,
                    )]
                ))
            elif isinstance(agency, dict):
                grants.append(Grant(
                    id=grant.get("GrantId", None),
                    title=grant.get("Title", None),
                    agency=[Agency(
                        abbreviation=agency.get("abbr", None),
                        name=agency.get("content", None),
                    )]
                ))
    except Exception as e:
        LOGGER.warning("Failed to parse grant with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return grants


EXTERNAL_LINK_MAP = {
    "GEO": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=",
    "dbGaP": "https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=",
    "ENA-SUBMISSION": "https://www.ebi.ac.uk/ena/browser/view/",
    "SRA": "https://www.ncbi.nlm.nih.gov/sra/",
    "PUBMED": "https://pubmed.ncbi.nlm.nih.gov/",
    "DOI": "https://doi.org/",
    "SRA|http": "https:",  # <ID>//www.ncbi.nlm.nih.gov/bioproject/PRJNA41439/</ID>
    "3000 rice genomes on aws|https": "https",  # <ID>//aws.amazon.com/public-data-sets/3000-rice-genome/</ID>
    "ENA|http": "https:",  # <ID>//www.ebi.ac.uk/ena/data/view/ERP005654</ID>
}


def _parse_external_link(accession: str, project: Dict[str, Any]) -> List[ExternalLink]:
    def _obj_to_external_link(obj: Dict[str, Any]) -> Optional[ExternalLink]:
        url = obj.get("URL", None)
        if url is not None:
            # PRJNA7, PRJNA8 など属性に URL を含む場合
            label = obj.get("label", None)
            return ExternalLink(
                url=url,
                label=label if label is not None else url,
            )

        db_xref = obj.get("dbXREF", None)
        if db_xref is not None:
            # 属性に dbXREF を含む場合
            db = db_xref.get("db", None)
            id_ = db_xref.get("ID", None)
            if db is not None and id_ is not None:
                if db in EXTERNAL_LINK_MAP:
                    url = EXTERNAL_LINK_MAP[db] + id_
                    label = obj.get("label", id_)
                    return ExternalLink(
                        url=url,
                        label=label,
                    )
                else:
                    LOGGER.warning("External link db %s is not supported", db)
                    return None

        return None

    links: List[ExternalLink] = []

    try:
        external_link_obj = project.get("Project", {}).get("ProjectDescr", {}).get("ExternalLink", None)
        if external_link_obj is None:
            return []

        if isinstance(external_link_obj, dict):
            external_link_instance = _obj_to_external_link(external_link_obj)
            if external_link_instance is not None:
                links.append(external_link_instance)

        elif isinstance(external_link_obj, list):
            # PRJNA3, PRJNA4, PRJNA5 など
            for item in external_link_obj:
                external_link_instance = _obj_to_external_link(item)
                if external_link_instance is not None:
                    links.append(external_link_instance)

    except Exception as e:
        LOGGER.warning("Failed to parse external link with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return links


def _parse_same_as(accession: str, project: Dict[str, Any]) -> List[Xref]:
    def _to_geo_xref(center_obj: Dict[str, Any]) -> Optional[Xref]:
        id_ = center_obj.get("content", None)
        type_ = center_obj.get("center", None)
        if id_ is None or type_ is None:
            return None
        if type_ != "GEO":
            return None
        return Xref(
            identifier=id_,
            type=type_,
            url=f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={id_}",
        )

    try:
        center_obj = project.get("Project", {}).get("ProjectID", {}).get("CenterID", None)
        if center_obj is None:
            return []

        if isinstance(center_obj, list):
            return [xref for xref in [_to_geo_xref(item) for item in center_obj] if xref is not None]
        elif isinstance(center_obj, dict):
            return [xref for xref in [_to_geo_xref(center_obj)] if xref is not None]

    except Exception as e:
        LOGGER.warning("Failed to parse sameAs with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return []


def _parse_status(project: Dict[str, Any], is_ddbj: bool) -> str:
    if is_ddbj:
        return "public"
    else:
        status = project.get("Submission", {}).get("Description", {}).get("Access", "public")
        if isinstance(status, str):
            return status
        else:
            return "public"


def _parse_date(project: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """\
    Return (dateCreated, dateModified, datePublished)
    """
    date_created = project.get("Submission", {}).get("submitted", None)
    date_modified = project.get("Submission", {}).get("last_update", None)
    date_published = project.get("Project", {}).get("ProjectDescr", {}).get("ProjectReleaseDate", None)

    return (date_created, date_modified, date_published)


def _update_prefix(accession: str, project: Dict[str, Any]) -> None:
    """\
    - ProjectType.ProjectTypeSubmission.Target.BioSampleSet.ID
    - 文字列で入力されてしまった場合、properties 内の値を object に整形する
    """
    try:
        bs_set_ids = project.get("Project", {}).get("ProjectType", {}).get(
            "ProjectTypeSubmission", {}).get("Target", {}).get("BioSampleSet", {}).get("ID", None)
        if bs_set_ids is None:
            return None

        if isinstance(bs_set_ids, list):
            for i, item in enumerate(bs_set_ids):
                if isinstance(item, str):
                    project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"][i] = {"content": item}
        elif isinstance(bs_set_ids, str):
            project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] = {"content": bs_set_ids}
    except Exception as e:
        LOGGER.warning("Failed to update prefix with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def _update_locus_tag_prefix(accession: str, project: Dict[str, Any]) -> None:
    """\
    - properties.Project.Project.ProjectDescr.LocusTagPrefix
    - 文字列で入力されていた場合 properties 内の値を object に整形する
    """
    try:
        prefix = project.get("Project", {}).get("ProjectDescr", {}).get("LocusTagPrefix", None)
        if prefix is None:
            return None

        if isinstance(prefix, list):
            for i, item in enumerate(prefix):
                if isinstance(item, str):
                    project["Project"]["ProjectDescr"]["LocusTagPrefix"][i] = {"content": item}
        elif isinstance(prefix, str):
            project["Project"]["ProjectDescr"]["LocusTagPrefix"] = {"content": prefix}
    except Exception as e:
        LOGGER.warning("Failed to update locus tag prefix with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def _update_local_id(accession: str, project: Dict[str, Any]) -> None:
    """\
    - properties.Project.Project.ProjectID.LocalID
    - 文字列で入力されていた場合 properties 内の値を object に整形する
    """
    try:
        local_id = project.get("Project", {}).get("ProjectID", {}).get("LocalID", None)
        if local_id is None:
            return None

        if isinstance(local_id, list):
            for i, item in enumerate(local_id):
                if isinstance(item, str):
                    project["Project"]["ProjectID"]["LocalID"][i] = {"content": item}
        elif isinstance(local_id, str):
            project["Project"]["ProjectID"]["LocalID"] = {"content": local_id}
    except Exception as e:
        LOGGER.warning("Failed to update local id with accession %s: %s", accession, e)
        LOGGER.warning("Traceback:\n%s", traceback.format_exc())

    return None


def write_jsonl(output_file: Path, docs: List[BioProject], is_append: bool = False) -> None:
    """\
    - memory のほうが多いと見越して、一気に書き込む
    """
    mode = "a" if is_append else "w"
    is_file_exists = output_file.exists()
    with output_file.open(mode=mode, encoding="utf-8") as f:
        if is_append and is_file_exists:
            f.write("\n")
        f.write("\n".join(doc.model_dump_json(by_alias=True) for doc in docs))


# === CLI implementation ===


class Args(BaseModel):
    xml_file: Path
    is_ddbj: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE
    parallel_num: int = DEFAULT_PARALLEL_NUM
    remove_tmp_dir: bool = False


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(
        description="""\
            Convert BioProject XML to JSON-Lines
        """
    )

    parser.add_argument(
        "--xml-file",
        help="""\
            BioProject XML file path (Required).
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--work-dir",
        help=f"""\
            The base directory where the script outputs are stored.
            By default, it is set to $PWD/ddbj_search_converter_results.
            The resulting JSON-Lines files will be stored in {{work_dir}}/{BP_JSONL_DIR_NAME}/{{date}}.
        """,
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--is-ddbj",
        action="store_true",
        help="""\
            Whether the input xml file is ddbj or not.
            This bool determines the processing branch.
        """,
    )
    parser.add_argument(
        "--batch-size",
        help=f"The number of records to store in a single JSON-Lines file. Default is {DEFAULT_BATCH_SIZE}",
        type=int,
        default=DEFAULT_BATCH_SIZE,
    )
    parser.add_argument(
        "--parallel-num",
        help=f"The number of parallel processes to use. Default is {DEFAULT_PARALLEL_NUM}",
        type=int,
        default=DEFAULT_PARALLEL_NUM,
    )
    parser.add_argument(
        "--remove-tmp-dir",
        help="Remove the temporary directory after processing",
        action="store_true",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.xml_file is None:
        raise Exception("Argument '--xml-file' is required.")
    xml_file = Path(parsed_args.xml_file).resolve()
    if xml_file.exists() is False:
        raise FileNotFoundError(f"File not found: {xml_file}")
    if parsed_args.work_dir is not None:
        config.work_dir = Path(parsed_args.work_dir)
        config.work_dir.mkdir(parents=True, exist_ok=True)
    if parsed_args.debug:
        config.debug = True

    return config, Args(
        xml_file=xml_file,
        is_ddbj=parsed_args.is_ddbj,
        batch_size=parsed_args.batch_size,
        parallel_num=parsed_args.parallel_num,
        remove_tmp_dir=parsed_args.remove_tmp_dir,
    )


def main() -> None:
    LOGGER.info("Start converting BioProject XML to JSON-Lines")
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.debug("Config:\n%s", config.model_dump_json(indent=2))
    LOGGER.debug("Args:\n%s", args.model_dump_json(indent=2))

    output_dir = config.work_dir.joinpath(BP_JSONL_DIR_NAME).joinpath(TODAY)
    output_dir.mkdir(parents=True, exist_ok=True)
    LOGGER.info("Output directory: %s", output_dir)
    xml_to_jsonl(
        config=config,
        xml_file=args.xml_file,
        output_dir=output_dir,
        is_ddbj=args.is_ddbj,
        batch_size=args.batch_size,
        parallel_num=args.parallel_num,
        remove_tmp_dir=args.remove_tmp_dir,
    )

    LOGGER.info("Finished converting BioProject XML to JSON-Lines")


if __name__ == "__main__":
    main()
