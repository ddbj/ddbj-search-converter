"""GEA / MetaboBank が共有する IDF (Investigation Description Format) parse 共通 utility。

MAGE-TAB IDF は tab 区切りのテキストで、1 列目が tag、2 列目以降が values。

- `parse_idf`: IDF を tag -> values dict に展開
- `parse_submitter_affiliations`: Person Affiliation から共通型 Organization list を構築
- `parse_pubmed_doi_publications`: PubMed ID / Publication DOI から共通型 Publication list を構築
"""

import csv
import re
from pathlib import Path

from ddbj_search_converter.schema import Organization, Publication

_DOI_PREFIX_RE = re.compile(
    r"^\s*(?:DOI\s*:\s*|https?://(?:dx\.)?doi\.org/|(?:dx\.)?doi\.org/)",
    re.IGNORECASE,
)


def _normalize_doi_value(raw: str) -> str:
    """IDF の `Publication DOI` 値の先頭プレフィックスを strip する。

    対応 prefix (case-insensitive):

    - `DOI:` / `doi:` (前後の空白を含む)
    - `https?://(dx\\.)?doi\\.org/` (DOI resolver URL)
    - `(dx\\.)?doi\\.org/` (protocol なしの doi.org URL)

    SSRN URL 等の DOI 外 URL (`http://ssrn.com/...`) は match せず生値のまま保持される。
    末尾句読点タイポ (`10.xxx.`) は元データ保持方針で strip 対象外。
    """
    return _DOI_PREFIX_RE.sub("", raw).strip()


def _build_doi_url(doi_id: str) -> str:
    """DOI id から `Publication.url` を構築する。

    - `http://` / `https://` で始まる値 (SSRN URL 等 DOI 以外の URL が
      `Publication DOI` 列に誤入力されているケース) は URL 自体を返し、
      `https://doi.org/` で二重 wrap しない。
    - それ以外 (DOI `10.xxx/...` 形式や判定不能な生文字列) は
      `https://doi.org/{doi_id}` を返す。
    """
    if doi_id.startswith(("http://", "https://")):
        return doi_id
    return f"https://doi.org/{doi_id}"


def parse_idf(path: Path) -> dict[str, list[str]]:
    """IDF ファイルを tag -> values の dict に展開する。

    - tab 区切り、1 列目を tag、2 列目以降を values として扱う。
    - 完全な空行および空の tag は skip。
    - **末尾の空 value は除去**、**途中の空 value は保持**（Protocol Parameters 等で
      Person 系と index を揃える必要があるため）。
    - 同一 tag が複数行現れる場合は後勝ち（現実の IDF では発生しない想定）。
    - **MAGE-TAB 仕様** (Tab2MAGE IDF notes) に則り、double quote で囲まれた値は
      tab / newline をリテラル保持する (Python 標準 `csv.reader(quotechar='"')`)。
    """
    result: dict[str, list[str]] = {}
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quotechar='"')
        for row in reader:
            if not row:
                continue
            tag = row[0].strip()
            if not tag:
                continue
            values = [v.strip() for v in row[1:]]
            while values and not values[-1]:
                values.pop()
            result[tag] = values
    return result


def parse_submitter_affiliations(idf: dict[str, list[str]]) -> list[Organization]:
    """IDF の Person Affiliation から Organization list を構築する。

    GEA / MetaboBank IDF の Person Roles は全件 "submitter" 単一値のため、role は
    "submitter" を固定する。空値は skip し、unique 化する。
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
    """IDF の PubMed ID と Publication DOI から Publication list を構築する。

    PubMed ID と Publication DOI は同一 index で対応させず、別 entry として list に詰める。
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
        doi_value = _normalize_doi_value(doi)
        if not doi_value:
            continue
        publications.append(
            Publication(
                id=doi_value,
                dbType="eDOI",
                url=_build_doi_url(doi_value),
            )
        )
    return publications
