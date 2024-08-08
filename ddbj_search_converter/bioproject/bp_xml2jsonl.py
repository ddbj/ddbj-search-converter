import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import xmltodict
from lxml import etree
from pydantic import BaseModel

from ddbj_search_converter.config import (LOGGER, Config, default_config,
                                          get_config, set_logging_level)
from ddbj_search_converter.utils import bulk_insert_to_es

# (date_created, date_published, date_modified) が None の場合のデフォルト値
TODAY = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
CORE_FILENAME_PATTERN = "ddbj_core"


# === type def. ===


class Organism(BaseModel):
    identifier: str
    name: str


class Distribution(BaseModel):
    contentUrl: str
    EncodingWarning: str = "JSON"


class Organization(BaseModel):
    content: str


class Grant(BaseModel):
    abbr: str
    content: str


class ExternalLink(BaseModel):
    label: Optional[str]
    URL: Optional[str]


class CommonDocument(BaseModel):
    identifier: str
    distribution: Distribution
    isPartOf: str = "BioProject"
    type: str = "bioproject"
    name: Optional[str]
    url: Optional[str]
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    organization: List[Organization]
    publication: List[str]
    grant: List[Grant]
    externalLink: List[ExternalLink]
    download: Optional[str]
    status: str = "public"
    visibility: str = "unrestricted-access"
    datePublished: str
    dateCreated: str
    dateModified: str


AccessionsData = Dict[str, Tuple[str, str, str]]


# === functions ===


class Args(BaseModel):
    xml_file: Path
    output_file: Optional[Path]
    accessions_tab_file: Optional[Path]
    bulk_es: bool
    batch_size: int = 200


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    parser = argparse.ArgumentParser(description="Convert BioProject XML to JSON-Lines")

    parser.add_argument(
        "xml_file",
        help="BioProject XML file path",
    )
    parser.add_argument(
        "output_file",
        help="Output JSON-Lines file path, if not specified, it will not output to a file",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--accessions-tab-file",
        help="DDBJ accessions.tab file path",
        nargs="?",
        default=None,
    )
    parser.add_argument(
        "--bulk-es",
        action="store_true",
        help="Insert data to Elasticsearch",
    )
    parser.add_argument(
        "--es-base-url",
        help="Elasticsearch base URL (default: http://localhost:9200)",
        default=default_config.es_base_url,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of documents to write or insert at once (default: 200)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    parsed_args = parser.parse_args(args)

    # 優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)
    config = get_config()
    if parsed_args.es_base_url != default_config.es_base_url:
        config.es_base_url = parsed_args.es_base_url
    if parsed_args.debug:
        config.debug = parsed_args.debug

    # Args の型変換と validation
    xml_file = Path(parsed_args.xml_file)
    if not xml_file.exists():
        LOGGER.error("Input BioProject XML file not found: %s", xml_file)
        sys.exit(1)

    output_file = None
    if parsed_args.output_file is None:
        if parsed_args.bulk_es is False:
            LOGGER.error("Output file path is required if not bulk inserting to Elasticsearch")
            sys.exit(1)
    else:
        output_file = Path(parsed_args.output_file)
        if output_file.exists():
            LOGGER.info("Output file %s already exists, will overwrite", output_file)
            output_file.unlink()

    accessions_tab_file = None
    if parsed_args.accessions_tab_file is not None:
        accessions_tab_file = Path(parsed_args.accessions_tab_file)
        if not accessions_tab_file.exists():
            LOGGER.error("DDBJ_Accessions.tab file not found: %s", accessions_tab_file)
            sys.exit(1)

    return (config, Args(
        xml_file=xml_file,
        output_file=output_file,
        accessions_tab_file=accessions_tab_file,
        bulk_es=parsed_args.bulk_es,
        batch_size=parsed_args.batch_size,
    ))


def xml2jsonl(
    xml_file: Path,
    output_file: Optional[Path],
    bulk_es: bool,
    es_base_url: str,
    is_core: bool,
    accession_data: AccessionsData,
    batch_size: int,
) -> None:
    """\
    BioProject XMLをdictに変換・関係データを追加し
    batch_sizeごとにlocalhostのESにbulkインポートする
    """
    context = etree.iterparse(xml_file, tag="Package", recover=True)
    docs: List[Dict[str, Any]] = []
    batch_count = 0
    for _events, element in context:
        if element.tag == "Package":
            # Package tag 単位で xml を変換する
            # center が指定されている場合は指定された center のデータのみ変換する（ddbj のみ対応）
            accession = element.find(".//Project/Project/ProjectID/ArchiveID").attrib["accession"]

            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            project = metadata["Package"]["Project"]

            doc = {
                "properties": {
                    "Project": project,
                },
            }

            # 関係データを取得と project の更新
            common_doc = CommonDocument(
                identifier=accession,
                distribution=Distribution(contentUrl=f"https://ddbj.nig.ac.jp/resource/bioproject/{accession}"),
                isPartOf="BioProject",
                type="bioproject",
                name=None,
                url=None,
                organism=_parse_organism(project, is_core),
                title=_parse_title(project),
                description=_parse_description(project),
                organization=_parse_and_update_organization(project),
                publication=_parse_and_update_publication(project),
                grant=_parse_and_update_grant(project),
                externalLink=_parse_external_link(project),
                download=None,
                status=_parse_status(project, is_core),
                visibility="unrestricted-access",
                datePublished=_parse_date_published(project, is_core, accession, accession_data),
                dateCreated=_parse_date_created(project, is_core, accession, accession_data),
                dateModified=_parse_date_modified(project, is_core, accession, accession_data),
            )
            _update_prefix(project)
            _update_local_id(project)

            doc.update(common_doc.model_dump())
            docs.append(doc)

            batch_count += 1
            if batch_count > batch_size:
                jsonl = docs_to_jsonl(docs)
                if output_file is not None:
                    dump_to_file(output_file, jsonl)
                if bulk_es:
                    bulk_insert_to_es(es_base_url, jsonl=jsonl, raise_on_error=False)
                batch_count = 0
                docs = []

        # メモリリークを防ぐために要素をクリアする
        clear_element(element)


def _parse_organism(project: Dict[str, Any], is_core: bool) -> Optional[Organism]:
    try:
        if is_core:
            organism_obj = project["Project"]["ProjectType"]["ProjectTypeTopAdmin"]["Organism"]
        else:
            organism_obj = project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["Organism"]
        return Organism(
            identifier=organism_obj.get("OrganismName"),
            name=organism_obj.get("taxID"),
        )
    except Exception as e:
        LOGGER.debug("Failed to parse organism from %s: %s", project, e)
        return None


def _parse_date_published(
    project: Dict[str, Any],
    is_core: bool,
    accession: str,
    accessions_data: AccessionsData,
) -> str:
    date = None
    if is_core:
        # (date_created, date_published, date_modified)
        data = accessions_data.get(accession, (None, None, None))
        date = data[1]
    else:
        try:
            date = project["Project"]["ProjectDescr"]["ProjectReleaseDate"]  # type: ignore
        except Exception as e:
            LOGGER.debug("Failed to parse date_published from %s: %s", project, e)

    return date or TODAY


def _parse_date_created(
    project: Dict[str, Any],
    is_core: bool,
    accession: str,
    accessions_data: AccessionsData,
) -> str:
    date = None
    if is_core:
        # (date_created, date_published, date_modified)
        data = accessions_data.get(accession, (None, None, None))
        date = data[0]
    else:
        try:
            date = project["Submission"]["submitted"]  # type: ignore
        except Exception as e:
            LOGGER.debug("Failed to parse date_created from %s: %s", project, e)

    return date or TODAY


def _parse_date_modified(
    project: Dict[str, Any],
    is_core: bool,
    accession: str,
    accessions_data: AccessionsData,
) -> str:
    date = None
    if is_core:
        # (date_created, date_published, date_modified)
        data = accessions_data.get(accession, (None, None, None))
        date = data[2]
    else:
        try:
            date = project["Submission"]["last_update"]  # type: ignore
        except Exception as e:
            LOGGER.debug("Failed to parse date_modified from %s: %s", project, e)

    return date or TODAY


def _update_prefix(
    project: Dict[str, Any],
) -> None:
    """\
    str や List[str] を {"content": str} などに変換する
    """
    # properties.Project.Project.ProjectDescr.LocusTagPrefix: 値が文字列の場合の処理
    try:
        prefix = project["Project"]["ProjectDescr"]["LocusTagPrefix"]
        if isinstance(prefix, list):
            for i, item in enumerate(prefix):
                if isinstance(item, str):
                    project["Project"]["ProjectDescr"]["LocusTagPrefix"][i] = {"content": item}
        elif isinstance(prefix, str):
            project["Project"]["ProjectDescr"]["LocusTagPrefix"] = {"content": prefix}
    except Exception as e:
        LOGGER.debug("Failed to update prefix from %s: %s", project, e)


def _update_local_id(
    project: Dict[str, Any],
) -> None:
    """\
    str や List[str] を {"content": str} などに変換する
    """
    # properties.Project.Project.ProjectID.LocalID: 値が文字列のケースの処理
    try:
        local_id = project["Project"]["ProjectID"]["LocalID"]
        if isinstance(local_id, list):
            for i, item in enumerate(local_id):
                if isinstance(item, str):
                    project["Project"]["ProjectID"]["LocalID"][i] = {"content": item}
        elif isinstance(local_id, str):
            project["Project"]["ProjectID"]["LocalID"] = {"content": local_id}
    except Exception as e:
        LOGGER.debug("Failed to update local_id from %s: %s", project, e)


def _parse_and_update_organization(
    project: Dict[str, Any],
) -> List[Organization]:
    """\
    parse もするし、update もする
    str や List[str] を {"content": str} などに変換する
    """
    parsed_organization = []
    try:
        organization = project["Submission"]["Description"]["Organization"]
        if isinstance(organization, list):
            for i, item in enumerate(organization):
                organization_name = item.get("Name")
                if isinstance(organization_name, str):
                    project["Submission"]["Description"]["Organization"][i]["Name"] = {"content": organization_name}
                    parsed_organization.append(Organization(content=organization_name))
                elif isinstance(organization_name, dict):
                    # case: organization_name = {"content": str}
                    parsed_organization.append(Organization(content=organization_name["content"]))
        elif isinstance(organization, dict):
            organization_name = organization.get("Name")
            if isinstance(organization_name, str):
                project["Submission"]["Description"]["Organization"]["Name"] = {"content": organization_name}
                parsed_organization.append(Organization(content=organization_name))
            elif isinstance(organization_name, dict):
                # case: organization_name = {"content": str}
                parsed_organization.append(Organization(content=organization_name["content"]))
    except Exception as e:
        LOGGER.debug("Failed to parse organization from %s: %s", project, e)

    return parsed_organization


def _parse_and_update_publication(
    project: Dict[str, Any],
) -> List[str]:
    publication = []
    try:
        publication = project["Project"]["ProjectDescr"]["Publication"]
        # 極稀に bulk insert できないサイズのリストがあるため
        if isinstance(publication, list) and len(publication) > 100000:
            # TODO: project["Project"]["ProjectDescr"]["Publication"] に代入しなくていいのか？
            project["Project"]["ProjectDescr"] = publication[:100000]
    except Exception as e:
        LOGGER.debug("Failed to parse publication from %s: %s", project, e)

    return publication


def _parse_and_update_grant(
    project: Dict[str, Any],
) -> List[Grant]:
    parsed_grant = []
    try:
        grant = project["Project"]["ProjectDescr"]["Grant"]
        if isinstance(grant, list):
            for i, item in enumerate(grant):
                agency = item.get("Agency")
                if isinstance(agency, str):
                    project["Project"]["ProjectDescr"]["Grant"][i]["Agency"] = {"abbr": agency, "content": agency}
                    parsed_grant.append(Grant(abbr=agency, content=agency))
        elif isinstance(grant, dict):
            agency = project["Project"]["ProjectDescr"]["Grant"]["Agency"]
            if isinstance(agency, str):
                project["Project"]["ProjectDescr"]["Grant"]["Agency"] = {"abbr": agency, "content": agency}
                parsed_grant.append(Grant(abbr=agency, content=agency))
    except Exception as e:
        LOGGER.debug("Failed to parse grant from %s: %s", project, e)

    return parsed_grant


EXTERNAL_LINKS = {
    "GEO": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=",
    "dbGaP": "https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=",
    "ENA-SUBMISSION": "https://www.ebi.ac.uk/ena/browser/view/",
    "SRA": "https://www.ncbi.nlm.nih.gov/sra/",
    "PUBMED": "https://pubmed.ncbi.nlm.nih.gov/",
    "DOI": "https://doi.org/",
    "SRA|http": "",
}


def _parse_external_link(
    project: Dict[str, Any],
) -> List[ExternalLink]:
    """\
    ExternalLink（schema は URL と dbXREF を共に許容する・共通項目には URL を渡す）
    properties の記述に関してはスキーマを合わせないと import エラーになり可能性があることに留意
    """
    parsed_external_link = []

    try:
        # A. ExternalLink の値がオブジェクトのケース
        external_link_obj = project["Project"]["ProjectDescr"]["ExternalLink"]
        if isinstance(external_link_obj, dict):
            if "URL" in external_link_obj:
                # 属性に URL を含む場合
                parsed_external_link.append(ExternalLink(
                    URL=external_link_obj.get("URL", None),
                    label=external_link_obj.get("label", None),
                ))

            elif "dbXREF" in external_link_obj:
                # 属性に dbXREF を含む場合
                db_xref_obj = external_link_obj.get("dbXREF", {})
                db_name = db_xref_obj.get("db", None)

                db_base_url = EXTERNAL_LINKS.get(db_name, None)
                if db_base_url is not None:
                    # TODO: external_link_obj["ID"] で大丈夫なのか？
                    url = db_base_url + external_link_obj["ID"]
                label = db_xref_obj.get("ID", None)
                if url is not None or label is not None:
                    parsed_external_link.append(ExternalLink(
                        URL=url,
                        label=label,
                    ))

        # B. ExternalLink の値が list のケース
        elif isinstance(external_link_obj, list):
            # TODO: if の階層に留意。一つひとつのオブジェクトごとに url から dbXREF のケース分けが必要
            for item in external_link_obj:
                if "URL" in item:
                    # 属性に URL を含む場合
                    parsed_external_link.append(ExternalLink(
                        URL=item.get("URL", None),
                        label=item.get("label", None),
                    ))

                elif "dbXREF" in item:
                    # 属性に dbXREF を含む場合
                    db_xref_obj = item.get("dbXREF", {})
                    db_name = db_xref_obj.get("db", None)

                    db_base_url = EXTERNAL_LINKS.get(db_name, None)
                    if db_base_url is not None:
                        # TODO: item["ID"] で大丈夫なのか？
                        url = db_base_url + item["ID"]
                    label = db_xref_obj.get("ID", None)
                    if url is not None or label is not None:
                        parsed_external_link.append(ExternalLink(
                            URL=url,
                            label=label,
                        ))

    except Exception as e:
        LOGGER.debug("Failed to parse external_link from %s: %s", project, e)

    return parsed_external_link


def _parse_status(
    project: Dict[str, Any],
    is_core: bool,
) -> str:
    """\
    ddbj_core には Submission の下の属性が無いため以下の処理を行わない
    """
    status = "public"
    if is_core is False:
        try:
            status = project["Submission"]["Description"]["Access"]
        except Exception as e:
            LOGGER.debug("Failed to parse status from %s: %s", project, e)

    return status


def _parse_title(
    project: Dict[str, Any],
) -> Optional[str]:
    title = None
    try:
        title = project["Project"]["ProjectDescr"]["Title"]
    except Exception as e:
        LOGGER.debug("Failed to parse title from %s: %s", project, e)

    return title


def _parse_description(
    project: Dict[str, Any],
) -> Optional[str]:
    description = None
    try:
        description = project["Project"]["ProjectDescr"]["Description"]
    except Exception as e:
        LOGGER.debug("Failed to parse description from %s: %s", project, e)

    return description


def docs_to_jsonl(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """\
    JSON-Lines だが、実際には、ES へと bulk insert するための形式となっている
    そのため、index と body が交互になっている
    """
    jsonl = []
    for doc in docs:
        jsonl.append({"index": {"_index": "bioproject", "_id": doc["identifier"]}})
        jsonl.append(doc)

    return jsonl


def dump_to_file(output_file: Path, jsonl: List[Dict[str, Any]]) -> None:
    with output_file.open("a") as f:
        for line in jsonl:
            f.write(json.dumps(line) + "\n")


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


def parse_accessions_tab_file(accessions_tab_file: Path) -> AccessionsData:
    """\
    key: accession
    value: (date_created, date_published, date_modified)
    """
    data = {}
    with accessions_tab_file.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) == 0:
                continue
            try:
                data[row[0]] = (row[1], row[2], row[3])
            except Exception as e:
                LOGGER.debug("Failed to parse accessions.tab row %s: %s", row, e)

    return data


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    set_logging_level(config.debug)
    LOGGER.info("Start converting BioProject XML %s to JSON-Lines", args.xml_file)
    LOGGER.info("Config: %s", config.model_dump())
    LOGGER.info("Args: %s", args.model_dump())

    is_core = False
    accessions_data = {}
    if CORE_FILENAME_PATTERN in args.xml_file.name:
        if args.accessions_tab_file is None:
            LOGGER.error("Your input xml file seems to be ddbj_core, so you need to specify accessions_tab_file")
            sys.exit(1)
        is_core = True
        accessions_data = parse_accessions_tab_file(args.accessions_tab_file)

    xml2jsonl(
        args.xml_file,
        args.output_file,
        args.bulk_es,
        config.es_base_url,
        is_core,
        accessions_data,
        args.batch_size,
    )

    LOGGER.info("Finished converting BioProject XML %s to JSON-Lines", args.xml_file)


if __name__ == "__main__":
    main()
