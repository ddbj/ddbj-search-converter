"""GEA / MetaboBank が共有する IDF (Investigation Description Format) parse 共通 utility。

MAGE-TAB IDF は tab 区切りのテキストで、1 列目が tag、2 列目以降が values。
Phase A §3.5.3 / §3.6.3 で確定した方針に従い、以下の utility を提供する:

- `parse_idf`: IDF を tag -> values dict に展開
- `parse_submitter_affiliations`: Person Affiliation から共通型 Organization list を構築 (案 α)
- `parse_pubmed_doi_publications`: PubMed ID / Publication DOI から共通型 Publication list を構築 (案 a)
"""

from pathlib import Path

from ddbj_search_converter.schema import Organization, Publication


def parse_idf(path: Path) -> dict[str, list[str]]:
    """IDF ファイルを tag -> values の dict に展開する。

    - tab 区切り、1 列目を tag、2 列目以降を values として扱う。
    - 完全な空行および空の tag は skip。
    - **末尾の空 value は除去**、**途中の空 value は保持**（Protocol Parameters 等で
      Person 系と index を揃える必要があるため）。
    - 同一 tag が複数行現れる場合は後勝ち（現実の IDF では発生しない想定）。
    """
    result: dict[str, list[str]] = {}
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\r\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            tag = parts[0].strip()
            if not tag:
                continue
            values = [v.strip() for v in parts[1:]]
            while values and not values[-1]:
                values.pop()
            result[tag] = values
    return result


def parse_submitter_affiliations(idf: dict[str, list[str]]) -> list[Organization]:
    """IDF の Person Affiliation から Organization list を構築する (案 α)。

    Phase A §4.6.1 / Q20 で GEA 688/688、MetaboBank 110/110 件の Person Roles が
    全件 "submitter" 単一値であることを確認済のため、role は "submitter" を固定する。
    空値は skip し、unique 化する。
    """
    affiliations = idf.get("Person Affiliation", [])
    seen: set[str] = set()
    organizations: list[Organization] = []
    for affiliation in affiliations:
        name = affiliation.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        organizations.append(Organization(name=name, role="submitter"))
    return organizations


def parse_pubmed_doi_publications(idf: dict[str, list[str]]) -> list[Publication]:
    """IDF の PubMed ID と Publication DOI から Publication list を構築する (案 a)。

    Phase A §3.5.3 / §3.6.3 の方針により、PubMed ID と Publication DOI は
    同一 index で対応させず、**別 entry として list に詰める**。
    """
    publications: list[Publication] = []
    for pmid in idf.get("PubMed ID", []):
        pmid_value = pmid.strip()
        if not pmid_value:
            continue
        publications.append(
            Publication(
                id=pmid_value,
                dbType="ePubmed",
                url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid_value}/",
            )
        )
    for doi in idf.get("Publication DOI", []):
        doi_value = doi.strip()
        if not doi_value:
            continue
        publications.append(
            Publication(
                id=doi_value,
                dbType="eDOI",
                url=f"https://doi.org/{doi_value}",
            )
        )
    return publications
