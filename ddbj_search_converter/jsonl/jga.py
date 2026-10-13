"""JGA JSONL 生成モジュール。"""

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any, Literal

import httpx

from ddbj_search_converter.config import (
    JGA_BASE_DIR_NAME,
    JGA_BASE_PATH,
    JGA_DATE_REL_PATHS,
    JSONL_DIR_NAME,
    TODAY_STR,
    Config,
    get_config,
)
from ddbj_search_converter.dblink.db import AccessionType
from ddbj_search_converter.dblink.utils import load_jga_blacklist
from ddbj_search_converter.humandbs import HumandbsResponseError, get_humandbs_text
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.jsonl.distribution import make_jga_distribution
from ddbj_search_converter.jsonl.utils import (
    build_pubmed_url,
    build_search_entry_self_url,
    deduplicate_organizations,
    ensure_attribute_list,
    get_dbxref_map,
    is_valid_external_url,
    normalize_publication_dbtype,
    write_jsonl,
)
from ddbj_search_converter.logging.logger import log_debug, log_info, log_warn, run_logger
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.schema import (
    JGA,
    ExternalLink,
    Grant,
    Organism,
    Organization,
    Publication,
    PublicationDbType,
    Xref,
)
from ddbj_search_converter.xml_utils import parse_xml

IndexName = Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"]
INDEX_NAMES: list[IndexName] = ["jga-study", "jga-dataset", "jga-dac", "jga-policy"]

XML_KEYS: dict[IndexName, tuple[str, str]] = {
    "jga-study": ("STUDY_SET", "STUDY"),
    "jga-dataset": ("DATASETS", "DATASET"),
    "jga-dac": ("DAC_SET", "DAC"),
    "jga-policy": ("POLICY_SET", "POLICY"),
}

# JGA type から AccessionType へのマッピング
INDEX_TO_ACCESSION_TYPE: dict[IndexName, AccessionType] = {
    "jga-study": "jga-study",
    "jga-dataset": "jga-dataset",
    "jga-dac": "jga-dac",
    "jga-policy": "jga-policy",
}

JGA_ATTRIBUTE_PATHS: dict[IndexName, list[list[str]]] = {
    "jga-study": [["STUDY_ATTRIBUTES", "STUDY_ATTRIBUTE"]],
}


def load_jga_xml(xml_path: Path) -> dict[str, Any]:
    """JGA XML ファイルを読み込んでパースする。"""
    with xml_path.open("rb") as f:
        xml_bytes = f.read()
    xml_metadata: dict[str, Any] = parse_xml(xml_bytes)
    return xml_metadata


# === 日付 (humandbs API) ===

DateTriple = tuple[str | None, str | None, str | None]
DateMap = dict[str, DateTriple]

JGA_DATE_KEYS = ("dateCreated", "datePublished", "dateModified")
_CALENDAR_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}\Z")


def humandbs_jga_date_url(base_url: str, index_name: IndexName) -> str:
    return f"{base_url.rstrip('/')}/api/jga-date/{index_name}"


def _parse_calendar_date(value: Any, origin: str, line: str) -> str | None:
    """``YYYY-MM-DD`` の文字列はそのまま返し、null は None にする。

    Raises:
        HumandbsResponseError: 文字列でも null でもない、または暦日として読めない場合。
    """
    if value is None:
        return None
    if not isinstance(value, str) or _CALENDAR_DATE.match(value) is None:
        raise HumandbsResponseError(f"date is not YYYY-MM-DD from {origin}: {line[:200]}")
    try:
        date.fromisoformat(value)
    except ValueError as e:
        raise HumandbsResponseError(f"date is not a calendar date from {origin}: {line[:200]}") from e
    return value


def parse_humandbs_jga_date_ndjson(text: str, index_name: IndexName, origin: str) -> DateMap:
    """/api/jga-date/{index_name} の NDJSON から accession -> (dateCreated, datePublished, dateModified) を取り出す。

    1 行は ``{"identifier": ..., "type": ..., "dateCreated": ..., "datePublished": ..., "dateModified": ...}``。
    日付は ``YYYY-MM-DD`` か null。形式に合わない accession の行はスキップする。
    ``origin`` は応答の URL か保存したファイルのパスで、メッセージに使う。

    Raises:
        HumandbsResponseError: JSON として読めない行、必要なキーが無い行、``type`` が index_name と違う行、
            日付が ``YYYY-MM-DD`` でも null でもない行がある場合。
    """
    date_map: DateMap = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
            accession = entry["identifier"]
            entry_type = entry["type"]
            values = [entry[key] for key in JGA_DATE_KEYS]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            raise HumandbsResponseError(f"unexpected line from {origin}: {line[:200]}") from e
        if not isinstance(accession, str):
            raise HumandbsResponseError(f"non-string identifier from {origin}: {line[:200]}")
        if entry_type != index_name:
            raise HumandbsResponseError(f"type is not {index_name} from {origin}: {line[:200]}")
        date_created, date_published, date_modified = (_parse_calendar_date(value, origin, line) for value in values)
        if not is_valid_accession(accession, INDEX_TO_ACCESSION_TYPE[index_name]):
            log_debug(
                f"skipping invalid {index_name}: {accession}",
                accession=accession,
                debug_category=DebugCategory.INVALID_ACCESSION_ID,
                source="jga-humandbs",
                file=origin,
            )
            continue
        date_map[accession] = (date_created, date_published, date_modified)

    return date_map


def fetch_jga_date_map(base_url: str, index_name: IndexName) -> DateMap:
    """humandbs の /api/jga-date/{index_name} から日付を取得する。

    Raises:
        httpx.HTTPError: retry しても接続できない、または 2xx 以外の応答の場合。
        HumandbsResponseError: 応答が期待する NDJSON として読めない場合。
    """
    url = humandbs_jga_date_url(base_url, index_name)
    return parse_humandbs_jga_date_ndjson(get_humandbs_text(url), index_name, url)


def _jga_date_path(config: Config, index_name: IndexName) -> Path:
    return config.const_dir.joinpath(JGA_DATE_REL_PATHS[index_name])


def _write_jga_date_file(path: Path, text: str) -> None:
    """同じディレクトリの一時ファイルに書いてから置き換え、途中で止まっても前のファイルを残す。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def update_jga_date_file(config: Config, index_name: IndexName) -> None:
    """humandbs から index_name の日付を取得し、{const_dir}/jga/{index_name}.date.ndjson に応答の本文のまま保存する。

    取得に失敗したとき、または有効な行が 0 件のときは warn を出し、ファイルを変更しない。
    """
    path = _jga_date_path(config, index_name)
    url = humandbs_jga_date_url(config.humandbs_url, index_name)
    try:
        text = get_humandbs_text(url)
        date_map = parse_humandbs_jga_date_ndjson(text, index_name, url)
    except (httpx.HTTPError, HumandbsResponseError) as e:
        log_warn(f"failed to fetch {index_name} dates, using saved file: {e}", url=url, file=str(path))
        return
    if not date_map:
        log_warn(f"humandbs returned no {index_name} dates, using saved file", url=url, file=str(path))
        return

    _write_jga_date_file(path, text)
    log_info(f"saved {len(date_map)} {index_name} dates from humandbs", url=url, file=str(path))


def load_date_map(config: Config, index_name: IndexName) -> DateMap:
    """保存した humandbs の応答から accession -> (dateCreated, datePublished, dateModified) を読み込む。

    Raises:
        FileNotFoundError: ファイルが存在しない場合。
        HumandbsResponseError: ファイルが期待する NDJSON として読めない場合。
    """
    path = _jga_date_path(config, index_name)
    if not path.exists():
        raise FileNotFoundError(f"jga date file not found: {path}")
    date_map = parse_humandbs_jga_date_ndjson(path.read_text(encoding="utf-8"), index_name, str(path))
    log_info(f"loaded {len(date_map)} {index_name} dates from humandbs file", file=str(path))

    return date_map


def extract_title(entry: dict[str, Any], index_name: IndexName) -> str | None:
    """JGA エントリからタイトルを抽出する。"""
    title: Any = None
    if index_name == "jga-study":
        title = (entry.get("DESCRIPTOR") or {}).get("STUDY_TITLE")
    elif index_name in ("jga-dataset", "jga-policy"):
        title = entry.get("TITLE")
    return str(title) if title is not None else None


def extract_description(entry: dict[str, Any], index_name: IndexName) -> str | None:
    """JGA エントリから説明を抽出する。"""
    description: Any = None
    if index_name == "jga-study":
        description = (entry.get("DESCRIPTOR") or {}).get("STUDY_ABSTRACT")
    elif index_name == "jga-dataset":
        description = entry.get("DESCRIPTION")
    elif index_name == "jga-policy":
        description = entry.get("POLICY_TEXT")
    return str(description) if description is not None else None


def parse_same_as(entry: dict[str, Any], index_name: IndexName, accession: str = "") -> list[Xref]:
    """JGA エントリから sameAs (SECONDARY_ID) を抽出する。"""
    xrefs: list[Xref] = []
    try:
        identifiers = entry.get("IDENTIFIERS")
        if identifiers is None:
            return []
        secondary_id = identifiers.get("SECONDARY_ID")
        if secondary_id is None:
            return []
        sid_list = secondary_id if isinstance(secondary_id, list) else [secondary_id]
        for sid in sid_list:
            if not sid or sid == accession:
                continue
            xrefs.append(
                Xref(
                    identifier=sid,
                    type=index_name,
                    url=build_search_entry_self_url(index_name, sid),
                )
            )
    except Exception as e:
        log_warn(f"failed to parse same_as: {e}", accession=accession)
    return xrefs


def parse_organization(entry: dict[str, Any], index_name: IndexName, accession: str = "") -> list[Organization]:
    """JGA エントリから Organization を抽出する。

    取得元:
    - 全 type: `@center_name`
    - jga-study: STUDY_ATTRIBUTES の TAG="Submitting organization" の VALUE
    - jga-dac: CONTACTS/CONTACT の @organisation (@name / @email は load しない)

    role / organizationType / department / url / abbreviation は None。
    dedupe は共通 util ``deduplicate_organizations`` に委譲する。
    """
    organizations: list[Organization] = []

    def _add(raw: Any) -> None:
        if not isinstance(raw, str):
            return
        stripped = raw.strip()
        if not stripped:
            return
        organizations.append(Organization(name=stripped))

    try:
        _add(entry.get("center_name"))

        if index_name == "jga-study":
            attrs = (entry.get("STUDY_ATTRIBUTES") or {}).get("STUDY_ATTRIBUTE")
            if isinstance(attrs, dict):
                attrs = [attrs]
            if isinstance(attrs, list):
                for attr in attrs:
                    if isinstance(attr, dict) and attr.get("TAG") == "Submitting organization":
                        _add(attr.get("VALUE"))

        elif index_name == "jga-dac":
            contacts = (entry.get("CONTACTS") or {}).get("CONTACT")
            if isinstance(contacts, dict):
                contacts = [contacts]
            if isinstance(contacts, list):
                for contact in contacts:
                    if isinstance(contact, dict):
                        _add(contact.get("organisation"))
    except Exception as e:
        log_warn(f"failed to parse organization: {e}", accession=accession)

    return deduplicate_organizations(organizations)


def parse_publications(entry: dict[str, Any], accession: str = "") -> list[Publication]:
    """jga-study エントリから Publication を抽出する。

    PUBLICATIONS/PUBLICATION (@id, DB_TYPE) を共通型 Publication に詰める。
    DB_TYPE は lower() 正規化後 "pubmed" のみ採用、未知値は None fallback。
    """
    publications: list[Publication] = []
    try:
        publication_obj = (entry.get("PUBLICATIONS") or {}).get("PUBLICATION")
        if publication_obj is None:
            return []
        pub_list = publication_obj if isinstance(publication_obj, list) else [publication_obj]
        for item in pub_list:
            if not isinstance(item, dict):
                continue

            raw_id = item.get("id")
            pub_id: str | None = None
            if isinstance(raw_id, str) and raw_id.strip():
                pub_id = raw_id.strip()

            raw_db = item.get("DB_TYPE")
            db_type: PublicationDbType | None = None
            if isinstance(raw_db, str):
                db_type = normalize_publication_dbtype(raw_db)

            url: str | None = None
            if db_type == "pubmed" and pub_id:
                url = build_pubmed_url(pub_id)

            publications.append(
                Publication(
                    id=pub_id,
                    dbType=db_type,
                    url=url,
                )
            )
    except Exception as e:
        log_warn(f"failed to parse publications: {e}", accession=accession)
    return publications


def parse_grants(entry: dict[str, Any], accession: str = "") -> list[Grant]:
    """jga-study エントリから Grant を抽出する。

    GRANTS/GRANT (@grant_id, TITLE, AGENCY[@abbr, text content]) を共通型 Grant に詰める。
    grant_id は空文字を None に倒す。AGENCY は共通型 Organization として構築し、str の
    場合は abbreviation=None、dict の場合は `@abbr` を abbreviation に詰める。
    role / organizationType / department / url は常に None (funding agency に該当する値がないため)。
    """
    grants: list[Grant] = []
    try:
        grant_obj = (entry.get("GRANTS") or {}).get("GRANT")
        if grant_obj is None:
            return []
        grant_list = grant_obj if isinstance(grant_obj, list) else [grant_obj]
        for item in grant_list:
            if not isinstance(item, dict):
                continue

            raw_id = item.get("grant_id")
            grant_id = raw_id if isinstance(raw_id, str) and raw_id.strip() else None

            raw_title = item.get("TITLE")
            title = raw_title if isinstance(raw_title, str) else None

            agency_obj = item.get("AGENCY")
            agencies: list[Organization] = []
            if isinstance(agency_obj, str):
                stripped = agency_obj.strip()
                if stripped:
                    agencies.append(Organization(name=stripped, abbreviation=None))
            elif isinstance(agency_obj, dict):
                content = agency_obj.get("content")
                if isinstance(content, str) and content.strip():
                    agencies.append(
                        Organization(
                            name=content.strip(),
                            abbreviation=agency_obj.get("abbr"),
                        )
                    )

            grants.append(Grant(id=grant_id, title=title, agency=agencies))
    except Exception as e:
        log_warn(f"failed to parse grants: {e}", accession=accession)
    return grants


_EXTERNAL_LINK_KEYS: dict[IndexName, tuple[str, str]] = {
    "jga-study": ("STUDY_LINKS", "STUDY_LINK"),
    "jga-dac": ("DAC_LINKS", "DAC_LINK"),
    "jga-policy": ("POLICY_LINKS", "POLICY_LINK"),
}


def parse_external_link(entry: dict[str, Any], index_name: IndexName, accession: str = "") -> list[ExternalLink]:
    """JGA エントリから ExternalLink を抽出する (type 別の *_LINKS/URL_LINK[LABEL, URL])。

    jga-dataset は URL_LINK を持たないので常に []。
    LABEL 欠損時は URL を label にフォールバック。
    """
    keys = _EXTERNAL_LINK_KEYS.get(index_name)
    if keys is None:
        return []
    parent_key, child_key = keys
    links: list[ExternalLink] = []
    try:
        links_obj = (entry.get(parent_key) or {}).get(child_key)
        if links_obj is None:
            return []
        link_list = links_obj if isinstance(links_obj, list) else [links_obj]
        for link in link_list:
            if not isinstance(link, dict):
                continue
            url_link = link.get("URL_LINK")
            if not isinstance(url_link, dict):
                continue
            url = url_link.get("URL")
            if not is_valid_external_url(url):
                continue
            raw_label = url_link.get("LABEL")
            label = raw_label if isinstance(raw_label, str) and raw_label.strip() else url
            links.append(ExternalLink(url=url, label=label))
    except Exception as e:
        log_warn(f"failed to parse external_link: {e}", accession=accession)
    return links


def extract_study_type(entry: dict[str, Any], accession: str = "") -> list[str]:
    """jga-study エントリから studyType を抽出する。

    STUDY_TYPES/STUDY_TYPE[@existing_study_type, @new_study_type] を list[str] に詰める。
    `existing != "Other"` → [existing]、`existing == "Other"` → [new_study_type or "Other"]。
    """
    study_types: list[str] = []
    try:
        st_obj = ((entry.get("DESCRIPTOR") or {}).get("STUDY_TYPES") or {}).get("STUDY_TYPE")
        if st_obj is None:
            return []
        st_list = st_obj if isinstance(st_obj, list) else [st_obj]
        for item in st_list:
            if not isinstance(item, dict):
                continue
            existing = item.get("existing_study_type")
            new = item.get("new_study_type")
            if not isinstance(existing, str) or not existing:
                continue
            if existing != "Other":
                study_types.append(existing)
            elif isinstance(new, str) and new.strip():
                study_types.append(new)
            else:
                study_types.append("Other")
    except Exception as e:
        log_warn(f"failed to extract study_type: {e}", accession=accession)
    return study_types


def extract_dataset_type(entry: dict[str, Any], accession: str = "") -> list[str]:
    """jga-dataset エントリから datasetType を抽出する (DATASET_TYPE テキスト)。"""
    dataset_types: list[str] = []
    try:
        dt_obj = entry.get("DATASET_TYPE")
        if dt_obj is None:
            return []
        dt_list = dt_obj if isinstance(dt_obj, list) else [dt_obj]
        dataset_types.extend(v for v in dt_list if isinstance(v, str) and v.strip())
    except Exception as e:
        log_warn(f"failed to extract dataset_type: {e}", accession=accession)
    return dataset_types


def parse_vendor(entry: dict[str, Any], accession: str = "") -> list[str]:
    """jga-study エントリから vendor を抽出する (STUDY_ATTRIBUTES TAG="Vendor" の VALUE)。"""
    vendors: list[str] = []
    try:
        attrs = (entry.get("STUDY_ATTRIBUTES") or {}).get("STUDY_ATTRIBUTE")
        if attrs is None:
            return []
        attr_list = attrs if isinstance(attrs, list) else [attrs]
        for attr in attr_list:
            if not isinstance(attr, dict) or attr.get("TAG") != "Vendor":
                continue
            value = attr.get("VALUE")
            if isinstance(value, str) and value.strip():
                vendors.append(value)
    except Exception as e:
        log_warn(f"failed to parse vendor: {e}", accession=accession)
    return vendors


def jga_entry_to_jga_instance(entry: dict[str, Any], index_name: IndexName) -> JGA:
    """JGA XML エントリを JGA インスタンスに変換する。"""
    accession: str = entry["accession"]
    ensure_attribute_list(entry, JGA_ATTRIBUTE_PATHS.get(index_name, []))

    if index_name in ("jga-study", "jga-dataset"):
        organism: Organism | None = Organism(identifier="9606", name="Homo sapiens")
    else:
        organism = None

    publications = parse_publications(entry, accession) if index_name == "jga-study" else []
    grants = parse_grants(entry, accession) if index_name == "jga-study" else []
    study_types = extract_study_type(entry, accession) if index_name == "jga-study" else []
    dataset_types = extract_dataset_type(entry, accession) if index_name == "jga-dataset" else []
    vendors = parse_vendor(entry, accession) if index_name == "jga-study" else []

    return JGA(
        identifier=accession,
        properties=entry,
        distribution=make_jga_distribution(index_name, accession),
        isPartOf="jga",
        type_=index_name,
        name=None,
        url=build_search_entry_self_url(index_name, accession),
        organism=organism,
        title=extract_title(entry, index_name),
        description=extract_description(entry, index_name),
        organization=parse_organization(entry, index_name, accession),
        publication=publications,
        grant=grants,
        externalLink=parse_external_link(entry, index_name, accession),
        studyType=study_types,
        datasetType=dataset_types,
        vendor=vendors,
        dbXrefs=[],  # 後で更新
        sameAs=parse_same_as(entry, index_name, accession),
        status="public",
        accessibility="controlled-access",
        dateCreated=None,  # 後で更新
        dateModified=None,  # 後で更新
        datePublished=None,  # 後で更新
    )


def generate_jga_jsonl(
    config: Config,
    index_name: IndexName,
    output_dir: Path,
    jga_base_path: Path,
    jga_blacklist: set[str],
    include_dbxrefs: bool = False,
) -> None:
    """単一の JGA タイプの JSONL ファイルを生成する。"""
    xml_path = jga_base_path.joinpath(f"{index_name}.xml")
    if not xml_path.exists():
        raise FileNotFoundError(f"XML file for {index_name} does not exist: {xml_path}")

    log_info(f"loading xml file: {xml_path}")
    xml_metadata = load_jga_xml(xml_path)

    # XML からエントリを抽出
    root_key, entry_key = XML_KEYS[index_name]
    try:
        entries = xml_metadata[root_key][entry_key]
        if index_name == "jga-dac" and not isinstance(entries, list):
            entries = [entries]  # DAC は単一エントリなのでリストにラップ
        if not isinstance(entries, list):
            raise ValueError(f"Expected a list for {index_name}, but got: {type(entries)}")
    except Exception as e:
        raise ValueError(f"Failed to parse XML for {index_name}: {e}") from e

    log_info(f"processing {len(entries)} entries from xml file: {xml_path}")

    # エントリを JGA インスタンスに変換
    jga_instances: dict[str, JGA] = {}
    skipped_count = 0
    for entry in entries:
        jga_instance = jga_entry_to_jga_instance(entry, index_name)

        # blacklist チェック
        if jga_instance.identifier in jga_blacklist:
            skipped_count += 1
            continue

        jga_instances[jga_instance.identifier] = jga_instance

    if skipped_count > 0:
        log_info(f"skipped {skipped_count} entries by blacklist")

    accessions = list(jga_instances.keys())

    # dbXrefs を取得して更新
    if include_dbxrefs:
        dbxref_map = get_dbxref_map(config, INDEX_TO_ACCESSION_TYPE[index_name], accessions)
        for accession, xrefs in dbxref_map.items():
            jga_instances[accession].dbXrefs = xrefs

    # 日付を取得して更新
    update_jga_date_file(config, index_name)
    date_map = load_date_map(config, index_name)
    for accession, (date_created, date_published, date_modified) in date_map.items():
        if accession in jga_instances:
            jga_instance = jga_instances[accession]
            jga_instance.dateCreated = date_created
            jga_instance.datePublished = date_published
            jga_instance.dateModified = date_modified

    # JSONL ファイルに出力
    output_path = output_dir.joinpath(f"{index_name}.jsonl")
    write_jsonl(output_path, list(jga_instances.values()))
    log_info(f"wrote {len(jga_instances)} entries to jsonl file: {output_path}")


def parse_args(args: list[str]) -> tuple[Config, Path, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(description="Generate JGA JSONL files from JGA XML files.")
    parser.add_argument(
        "--include-dbxrefs",
        help="Include dbXrefs in JSONL output.",
        action="store_true",
    )

    parsed = parser.parse_args(args)

    config = get_config()
    output_dir = config.result_dir / JGA_BASE_DIR_NAME / JSONL_DIR_NAME / TODAY_STR

    return config, output_dir, parsed.include_dbxrefs


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, include_dbxrefs = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_jga_jsonl", config=config):
        log_debug(f"config: {config.model_dump_json(indent=2)}")
        log_debug(f"output directory: {output_dir}")
        log_debug(f"jga base path: {JGA_BASE_PATH}")
        log_debug(f"include dbxrefs: {include_dbxrefs}")

        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"output directory: {output_dir}")

        jga_blacklist = load_jga_blacklist(config)

        for index_name in INDEX_NAMES:
            generate_jga_jsonl(config, index_name, output_dir, JGA_BASE_PATH, jga_blacklist, include_dbxrefs)


if __name__ == "__main__":
    main()
