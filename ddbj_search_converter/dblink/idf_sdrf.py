"""
IDF/SDRF ファイルのパース共通ユーティリティ。
GEA と MetaboBank で使用する。

抽出対象:
- IDF: ``Comment[BioProject]`` / ``Comment[Related study]``
- SDRF: ``Comment[BioSample]`` / ``Comment[SRA_RUN]`` / ``Comment[SRA_EXPERIMENT]``

``Comment[Related study]`` の値は ``_classify_related_study()`` で分類し、
``JGA:JGAS*`` は ``jga-study``、``NBDC:hum*`` は ``humandbs`` として dblink 経由で dbXrefs に流す。
``Metabolonote:SE*`` / ``RPMM:RPMM*`` / ``Metabolights:MTBLS*`` 等の他 prefix は silent skip。
"""

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


@dataclass
class IdfSdrfResult:
    """IDF/SDRF ディレクトリから抽出した関連先 ID 群。"""

    entry_id: str
    bioproject: str | None = None
    related_studies: list[str] = field(default_factory=list)
    biosamples: set[str] = field(default_factory=set)
    sra_runs: set[str] = field(default_factory=set)
    sra_experiments: set[str] = field(default_factory=set)


def parse_idf_file(idf_path: Path) -> tuple[str | None, list[str]]:
    """IDF ファイルから BioProject ID と Related study 値を抽出する。

    MAGE-TAB 仕様に従い ``csv.reader(quotechar='"')`` で quote 囲み値を正しく扱う
    (``idf_common.parse_idf`` と一貫)。

    Returns:
        ``(bioproject, related_studies)``:
        - ``bioproject``: 最初に見つかった ``Comment[BioProject]`` の最初の非空値 (無ければ ``None``)
        - ``related_studies``: 全 ``Comment[Related study]`` 行の tab-separated 値を strip して
          非空のみ先頭から順に列挙した list (prefix は生値のまま、classify は caller 側)
    """
    bioproject: str | None = None
    related_studies: list[str] = []

    with idf_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quotechar='"')
        for row in reader:
            if not row:
                continue
            tag = row[0].strip()
            values = [v.strip() for v in row[1:]]

            if tag == "Comment[BioProject]" and bioproject is None:
                for v in values:
                    if v:
                        bioproject = v
                        break
            elif tag == "Comment[Related study]":
                related_studies.extend(v for v in values if v)

    return bioproject, related_studies


def parse_sdrf_file(sdrf_path: Path) -> dict[str, set[str]]:
    """SDRF ファイルから BioSample / SRA_RUN / SRA_EXPERIMENT の値を抽出する。

    MAGE-TAB 仕様に従い ``csv.reader(quotechar='"')`` で quote 囲み値を正しく扱う
    (``idf_common.parse_idf`` と一貫)。列名は strict case match
    (``Comment[BioSample]`` / ``Comment[SRA_RUN]`` / ``Comment[SRA_EXPERIMENT]``)。
    列が存在しない場合は対応 key の value は空 set。空 cell / 空白のみ cell は skip。

    Returns:
        ``{"biosample": set, "sra_run": set, "sra_experiment": set}``
    """
    result: dict[str, set[str]] = {
        "biosample": set(),
        "sra_run": set(),
        "sra_experiment": set(),
    }

    column_map: dict[str, str] = {
        "Comment[BioSample]": "biosample",
        "Comment[SRA_RUN]": "sra_run",
        "Comment[SRA_EXPERIMENT]": "sra_experiment",
    }

    with sdrf_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quotechar='"')
        try:
            header = next(reader)
        except StopIteration:
            return result

        # 列名 → 列 index の map を構築 (該当列が無ければ entry なし)
        indexes: list[tuple[int, str]] = []
        for idx, col in enumerate(header):
            key = column_map.get(col)
            if key is not None:
                indexes.append((idx, key))

        if not indexes:
            return result

        for row in reader:
            if not row:
                continue
            for idx, key in indexes:
                if idx < len(row):
                    value = row[idx].strip()
                    if value:
                        result[key].add(value)

    return result


def _classify_related_study(raw: str) -> tuple[Literal["jga-study", "humandbs"], str] | None:
    """``Comment[Related study]`` の値を ``(XrefType, accession)`` に分類する。

    - ``JGA:JGAS*`` → ``("jga-study", "JGAS*")``
    - ``NBDC:hum*`` → ``("humandbs", "hum*")``
    - それ以外 (``Metabolonote:`` / ``RPMM:`` / ``Metabolights:`` / 空 / 不明 prefix / コロンなし) → ``None``

    prefix 判定は case-insensitive + 全体 strip (staging 実データは全て strict case だが防御実装)。
    戻り値の accession part は strip 済みの原 case。``is_valid_accession()`` は caller 側で行う。
    """
    stripped = raw.strip()
    if not stripped or ":" not in stripped:
        return None

    prefix, _, tail = stripped.partition(":")
    accession = tail.strip()
    if not accession:
        return None

    prefix_lower = prefix.strip().lower()
    if prefix_lower == "jga":
        return ("jga-study", accession)
    if prefix_lower == "nbdc":
        return ("humandbs", accession)
    return None


def process_idf_sdrf_dir(dir_path: Path) -> IdfSdrfResult:
    """IDF/SDRF を含むディレクトリを処理して ``IdfSdrfResult`` を返す。

    IDF / SDRF ファイルが存在しない場合は対応フィールドを空のままにして返す (raise しない)。

    Args:
        dir_path: IDF/SDRF ファイルを含むディレクトリ (``entry_id`` はディレクトリ名)

    Returns:
        IdfSdrfResult: ``entry_id`` / ``bioproject`` / ``related_studies`` /
        ``biosamples`` / ``sra_runs`` / ``sra_experiments`` を格納
    """
    result = IdfSdrfResult(entry_id=dir_path.name)

    idf_files = list(dir_path.glob("*.idf.txt"))
    if idf_files:
        bioproject, related_studies = parse_idf_file(idf_files[0])
        result.bioproject = bioproject
        result.related_studies = related_studies

    sdrf_files = list(dir_path.glob("*.sdrf.txt"))
    if sdrf_files:
        sdrf_values = parse_sdrf_file(sdrf_files[0])
        result.biosamples = sdrf_values["biosample"]
        result.sra_runs = sdrf_values["sra_run"]
        result.sra_experiments = sdrf_values["sra_experiment"]

    return result
