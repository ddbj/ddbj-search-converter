"""\
- BioProject XML を JSON-Lines に変換する
- is_ddbj で、それぞれ処理が分岐する
    - ddbj xml (/usr/local/resources/bioproject/ddbj_bioproject.xml) の場合は、work_dir/${date}/ddbj_bioproject.jsonl に出力
    - それ以外 (/usr/local/resources/bioproject/bioproject.xml) の場合は、work_dir/${date}/bioproject_${n}.jsonl に BATCH_SIZE (2000) 件ずつ出力
        - -> ddbj_xml と common_xml という呼び方をすることにする
- 生成される JSON-Lines は 1 line が 1 BioProject Accession に対応する
"""
import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import xmltodict
from lxml import etree
from pydantic import BaseModel

from ddbj_search_converter.cache_db.bp_date import get_dates as get_bp_dates
from ddbj_search_converter.cache_db.bp_date import \
    get_session as get_bp_date_session
from ddbj_search_converter.cache_db.fusion_getter import get_xrefs
from ddbj_search_converter.config import (LOGGER, Config, default_config,
                                          get_config, set_logging_level)
from ddbj_search_converter.schema import (Agency, BioProject, Distribution,
                                          ExternalLink, Grant, Organism,
                                          Organization, Publication, Xref)

BATCH_SIZE = 2000
DDBJ_JSONL_FILE_NAME = "ddbj_bioproject.jsonl"
COMMON_JSONL_FILE_NAME = "bioproject_{n}.jsonl"

# from ddbj_search_converter.utils import bulk_insert_to_es


# def parse_args(args: List[str]) -> Tuple[Config, Args]:
#     parser = argparse.ArgumentParser(description="Convert BioProject XML to JSON-Lines")

#     parser.add_argument(
#         "xml_file",
#         help="BioProject XML file path",
#     )
#     parser.add_argument(
#         "output_file",
#         help="Output JSON-Lines file path, if not specified, it will not output to a file",
#         nargs="?",
#         default=None,
#     )
#     parser.add_argument(
#         "--accessions-tab-file",
#         help="DDBJ accessions.tab file path",
#         nargs="?",
#         default=None,
#     )
#     parser.add_argument(
#         "--debug",
#         action="store_true",
#         help="Enable debug mode",
#     )

#     parsed_args = parser.parse_args(args)

#     # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
#     config = get_config()
#     if parsed_args.es_base_url != default_config.es_base_url:
#         config.es_base_url = parsed_args.es_base_url
#     if parsed_args.debug:
#         config.debug = parsed_args.debug

#     # Args の型変換と validation
#     xml_file = Path(parsed_args.xml_file)
#     if not xml_file.exists():
#         LOGGER.error("Input BioProject XML file not found: %s", xml_file)
#         sys.exit(1)

#     output_file = None
#     if parsed_args.output_file is None:
#         if parsed_args.bulk_es is False:
#             LOGGER.error("Output file path is required if not bulk inserting to Elasticsearch")
#             sys.exit(1)
#     else:
#         output_file = Path(parsed_args.output_file)
#         if output_file.exists():
#             LOGGER.info("Output file %s already exists, will overwrite", output_file)
#             output_file.unlink()

#     accessions_tab_file = None
#     if parsed_args.accessions_tab_file is not None:
#         accessions_tab_file = Path(parsed_args.accessions_tab_file)
#         if not accessions_tab_file.exists():
#             LOGGER.error("DDBJ_Accessions.tab file not found: %s", accessions_tab_file)
#             sys.exit(1)

#     return (config, Args(
#         xml_file=xml_file,
#         output_file=output_file,
#         accessions_tab_file=accessions_tab_file,
#         bulk_es=parsed_args.bulk_es,
#         batch_size=parsed_args.batch_size,
#     ))


def xml_to_jsonl(
    config: Config,
    xml_file: Path,
    is_ddbj: bool,
    output_dir: Path,
    batch_size: int = BATCH_SIZE,
) -> None:
    context = etree.iterparse(xml_file, tag="Package", recover=True)
    docs: List[BioProject] = []
    batch_count = 0
    file_count = 1
    with get_bp_date_session(config) as session:
        for _events, element in context:
            if element.tag == "Package":
                # Package tag 単位で xml を変換する
                # center が指定されている場合は指定された center のデータのみ変換する（ddbjのみ対応）
                accession = element.find(".//Project/Project/ProjectID/ArchiveID").attrib["accession"]
                xml_str = etree.tostring(element)
                metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
                project = metadata["Package"]["Project"]

                if is_ddbj:
                    date_created, date_modified, date_published = get_bp_dates(session, accession)
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
                    dbXref=get_xrefs(config, accession, "bioproject"),
                    sameAs=_parse_same_as(project),
                    status=_parse_status(project, is_ddbj),
                    visibility="unrestricted-access",
                    dateCreated=date_created,
                    dateModified=date_modified,
                    datePublished=date_published,
                )

                # properties の中の object に対して整形を行う
                _update_prefix(project)
                _update_locus_tag_prefix(project)
                _update_local_id(project)

                docs.append(bp_instance)

                batch_count += 1
                if batch_count >= batch_size:
                    output_file = output_dir.joinpath(
                        DDBJ_JSONL_FILE_NAME if is_ddbj else COMMON_JSONL_FILE_NAME.format(n=file_count)
                    )
                    write_jsonl(output_file, docs, is_append=is_ddbj)
                    batch_count = 0
                    file_count += 1
                    docs = []

            # メモリリークを防ぐために要素をクリアする
            clear_element(element)

    if len(docs) > 0:
        # 余りの docs の書き込み
        output_file = output_dir.joinpath(
            DDBJ_JSONL_FILE_NAME if is_ddbj else COMMON_JSONL_FILE_NAME.format(n=file_count)
        )
        write_jsonl(output_file, docs, is_append=is_ddbj)


def _parse_object_type(project: Dict[str, Any]) -> Literal["BioProject", "UmbrellaBioProject"]:
    # umbrella の判定
    # 要検討: 例外発生のコストが高いため通常の判定には使わない方が良いという判断で try&if case で ProjectTypeTopAdmin の存在を確認
    if project.get("Project", {}).get("ProjectType", {}).get("ProjectTypeTopAdmin"):
        return "UmbrellaBioProject"

    return "BioProject"


def _parse_organism(accession: str, project: Dict[str, Any], is_ddbj: bool) -> Optional[Organism]:
    try:
        if is_ddbj:
            organism_obj = project["Project"]["ProjectType"]["ProjectTypeTopAdmin"]["Organism"]
        else:
            organism_obj = project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["Organism"]
        return Organism(
            identifier=str(organism_obj["taxID"]),
            name=organism_obj["OrganismName"],
        )
    except Exception as e:
        LOGGER.debug("Failed to parse organism with accession %s: %s", accession, e)
        return None


def _parse_title(accession: str, project: Dict[str, Any]) -> Optional[str]:
    try:
        title = project["Project"]["ProjectDescr"]["Title"]
        return str(title)
    except Exception as e:
        LOGGER.debug("Failed to parse title with accession %s: %s", accession, e)
        return None


def _parse_description(accession: str, project: Dict[str, Any]) -> Optional[str]:
    try:
        description = project["Project"]["ProjectDescr"]["Description"]
        return str(description)
    except Exception as e:
        LOGGER.debug("Failed to parse description with accession %s: %s", accession, e)
        return None


def _parse_and_update_organization(accession: str, project: Dict[str, Any], is_ddbj: bool) -> List[Organization]:
    """\
    Organization の parse、引数の project の update も行う
    """
    organizations: List[Organization] = []

    try:
        if is_ddbj:
            organization = project["Submission"]["Submission"]["Description"]["Organization"]
        else:
            organization = project["Submission"]["Description"]["Organization"]

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

    except Exception as e:
        # TODO: 恐らく、エラー処理が不十分
        LOGGER.debug("Failed to parse organization with accession %s: %s", accession, e)

    return organizations


def _parse_and_update_publication(accession: str, project: Dict[str, Any]) -> List[Publication]:
    """\
    Publication の parse、引数の project の update も行う
    """
    publications: List[Publication] = []

    try:
        publication = project["Project"]["ProjectDescr"]["Publication"]
        if isinstance(publication, list):
            # bulk insert できないサイズのリストに対応
            publication = publication[:10000]
            # 引数の project の update を行う
            project["Project"]["ProjectDescr"]["Publication"] = publication

            for item in publication:
                id_ = item.get("id", None)
                id_ = str(id_) if id_ is not None else None
                if id_ is None:
                    LOGGER.debug("Publication ID is not found with accession %s", accession)
                    continue
                dbtype = item.get("DbType", None)
                publication_url = None
                if dbtype in ["ePubmed", "eDOI"]:
                    publication_url = id_ if dbtype == "eDOI" else f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
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
            id_ = str(id_) if id_ is not None else None
            if id_ is None:
                LOGGER.debug("Publication ID is not found with accession %s", accession)
                raise ValueError("Publication ID is not found")
            dbtype = publication.get("DbType", None)
            publication_url = None
            if dbtype in ["ePubmed", "eDOI"]:
                publication_url = id_ if dbtype == "eDOI" else f"https://pubmed.ncbi.nlm.nih.gov/{id_}/"
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
        # TODO: 恐らく、エラー処理が不十分
        LOGGER.debug("Failed to parse publication with accession %s: %s", accession, e)

    return publications


def _parse_and_update_grant(accession: str, project: Dict[str, Any]) -> List[Grant]:
    grants: List[Grant] = []

    try:
        grant = project["Project"]["ProjectDescr"]["Grant"]
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
        # TODO: 恐らく、エラー処理が不十分
        LOGGER.debug("Failed to parse grant with accession %s: %s", accession, e)

    return grants


EXTERNAL_LINK_MAP = {
    "GEO": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=",
    "dbGaP": "https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=",
    "ENA-SUBMISSION": "https://www.ebi.ac.uk/ena/browser/view/",
    "SRA": "https://www.ncbi.nlm.nih.gov/sra/",
    "PUBMED": "https://pubmed.ncbi.nlm.nih.gov/",
    "DOI": "https://doi.org/",
    "SRA|http": "",  # TODO: これなに
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
                    raise ValueError(f"External link db {db} is not supported")

        return None

    links: List[ExternalLink] = []

    try:
        external_link_obj = project["Project"]["ProjectDescr"]["ExternalLink"]
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
        LOGGER.debug("Failed to parse external link with accession %s: %s", accession, e)

    return links


def _parse_same_as(project: Dict[str, Any]) -> List[Xref]:
    try:
        center = project["Project"]["ProjectID"]["CenterID"]["center"]
        if center == "GEO":
            id_ = project["Project"]["ProjectID"]["CenterID"]["content"]
            return [Xref(
                identifier=id_,
                type="GEO",
                url=f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={id_}",
            )]
    except Exception as _e:
        # そもそも sameAs が存在しない場合もある
        # LOGGER.debug("Failed to parse sameAs: %s", e)
        pass

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


def _update_prefix(project: Dict[str, Any]) -> None:
    """\
    - ProjectType.ProjectTypeSubmission.Target.BioSampleSet.ID
    - 文字列で入力されてしまった場合、properties 内の値を object に整形する
    """
    try:
        bs_set_ids = project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"]
        if isinstance(bs_set_ids, list):
            for i, item in enumerate(bs_set_ids):
                if isinstance(item, str):
                    project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"][i] = {"content": item}
        elif isinstance(bs_set_ids, str):
            project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] = {"content": bs_set_ids}
    except Exception as e:
        LOGGER.debug("Failed to update prefix: %s", e)


def _update_locus_tag_prefix(project: Dict[str, Any]) -> None:
    """\
    - properties.Project.Project.ProjectDescr.LocusTagPrefix
    - 文字列で入力されていた場合 properties 内の値を object に整形する
    """
    try:
        prefix = project["Project"]["ProjectDescr"]["LocusTagPrefix"]
        if isinstance(prefix, list):
            for i, item in enumerate(prefix):
                if isinstance(item, str):
                    project["Project"]["Project"]["ProjectDescr"]["LocusTagPrefix"][i] = {"content": item}
        elif isinstance(prefix, str):
            project["Project"]["Project"]["ProjectDescr"]["LocusTagPrefix"] = {"content": prefix}
    except Exception as e:
        LOGGER.debug("Failed to update locus tag prefix: %s", e)


def _update_local_id(project: Dict[str, Any]) -> None:
    """\
    - properties.Project.Project.ProjectID.LocalID
    - 文字列で入力されていた場合 properties 内の値を object に整形する
    """
    try:
        local_id = project["Project"]["ProjectID"]["LocalID"]
        if isinstance(local_id, list):
            for i, item in enumerate(local_id):
                if isinstance(item, str):
                    project["Project"]["Project"]["ProjectID"]["LocalID"][i] = {"content": item}
        elif isinstance(local_id, str):
            project["Project"]["Project"]["ProjectID"]["LocalID"] = {"content": local_id}
    except Exception as e:
        LOGGER.debug("Failed to update local id: %s", e)


def write_jsonl(output_file: Path, docs: List[BioProject], is_append: bool = False) -> None:
    """\
    - memory のほうが多いと見越して、一気に書き込む
    """
    mode = "a" if is_append else "w"
    with output_file.open(mode=mode, encoding="utf-8") as f:
        if is_append:
            f.write("\n")
        f.write("\n".join(doc.model_dump_json() for doc in docs))


def clear_element(element: Any) -> None:
    try:
        element.clear()
        while element.getprevious() is not None:
            try:
                del element.getparent()[0]
            except Exception as e:
                LOGGER.debug("Failed to clear element: %s", e)
    except Exception as e:
        LOGGER.debug("Failed to clear element: %s", e)


# def main() -> None:
#     config, args = parse_args(sys.argv[1:])
#     set_logging_level(config.debug)
#     LOGGER.info("Start converting BioProject XML %s to JSON-Lines", args.xml_file)
#     LOGGER.info("Config: %s", config.model_dump())
#     LOGGER.info("Args: %s", args.model_dump())

#     is_ddbj = False
#     accessions_data = {}
#     if CORE_FILENAME_PATTERN in args.xml_file.name:
#         if args.accessions_tab_file is None:
#             LOGGER.error("Your input xml file seems to be ddbj_core, so you need to specify accessions_tab_file")
#             sys.exit(1)
#         is_ddbj = True
#         accessions_data = parse_accessions_tab_file(args.accessions_tab_file)

#     xml2jsonl(
#         args.xml_file,
#         args.output_file,
#         args.bulk_es,
#         config.es_base_url,
#         is_ddbj,
#         accessions_data,
#         args.batch_size,
#     )

#     LOGGER.info("Finished converting BioProject XML %s to JSON-Lines", args.xml_file)
# if __name__ == "__main__":
#     main()
