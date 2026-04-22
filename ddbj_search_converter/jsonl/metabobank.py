"""MetaboBank JSONL 生成モジュール。"""

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path

from ddbj_search_converter.config import (
    JSONL_DIR_NAME,
    METABOBANK_BASE_DIR_NAME,
    METABOBANK_BASE_PATH,
    SEARCH_BASE_URL,
    TODAY_STR,
    Config,
    get_config,
)
from ddbj_search_converter.jsonl.distribution import make_metabobank_distribution
from ddbj_search_converter.jsonl.idf_common import (
    parse_idf,
    parse_pubmed_doi_publications,
    parse_submitter_affiliations,
)
from ddbj_search_converter.jsonl.utils import get_dbxref_map, write_jsonl
from ddbj_search_converter.logging.logger import log_debug, log_info, log_warn, run_logger
from ddbj_search_converter.schema import MetaboBank, Xref


def iterate_metabobank_idf_files(base_path: Path) -> Iterator[tuple[str, Path]]:
    """MetaboBank IDF ファイルを iterate する。

    MTBKS* 直下の 1 階層構造。{accession}.idf.txt が無いディレクトリは skip + log_warn
    (S4 調査で 157 件中 47 件が IDF 無しで、これは除外対象)。
    """
    if not base_path.exists():
        log_warn(f"metabobank base path does not exist: {base_path}")
        return

    for mtb_dir in sorted(base_path.iterdir()):
        if not mtb_dir.is_dir() or not mtb_dir.name.startswith("MTBKS"):
            continue
        accession = mtb_dir.name
        idf_path = mtb_dir / f"{accession}.idf.txt"
        if not idf_path.exists():
            log_warn("missing idf file", accession=accession, file=str(idf_path))
            continue
        yield accession, idf_path


def _first_value(idf: dict[str, list[str]], tag: str) -> str | None:
    """IDF から tag の最初の非空値を返す。空白のみは None 扱い。"""
    for value in idf.get(tag, []):
        stripped = value.strip() if isinstance(value, str) else ""
        if stripped:
            return stripped
    return None


def _non_empty_list(idf: dict[str, list[str]], tag: str) -> list[str]:
    """IDF から tag の全値を非空 + strip して list 化する。"""
    return [v.strip() for v in idf.get(tag, []) if isinstance(v, str) and v.strip()]


def extract_title(idf: dict[str, list[str]]) -> str | None:
    """MetaboBank IDF からタイトルを抽出する (Study Title 最初値)。"""
    return _first_value(idf, "Study Title")


def extract_description(idf: dict[str, list[str]]) -> str | None:
    """MetaboBank IDF から description を抽出する (Study Description 最初値)。"""
    return _first_value(idf, "Study Description")


def extract_study_type(idf: dict[str, list[str]]) -> list[str]:
    """MetaboBank IDF から studyType を抽出する (Comment[Study type] の非空値 list)。"""
    return _non_empty_list(idf, "Comment[Study type]")


def extract_experiment_type(idf: dict[str, list[str]]) -> list[str]:
    """MetaboBank IDF から experimentType を抽出する (Comment[Experiment type] の非空値 list)。"""
    return _non_empty_list(idf, "Comment[Experiment type]")


def extract_submission_type(idf: dict[str, list[str]]) -> list[str]:
    """MetaboBank IDF から submissionType を抽出する (Comment[Submission type] の非空値 list)。"""
    return _non_empty_list(idf, "Comment[Submission type]")


def extract_dates(idf: dict[str, list[str]]) -> tuple[str | None, str | None, str | None]:
    """MetaboBank IDF から dates を抽出する。

    dateCreated = Comment[Submission Date] 最初値 (全 110 件埋まる MetaboBank 固有タグ)。
    dateModified = Comment[Last Update Date] 最初値。
    datePublished = Public Release Date 最初値。
    IDF の日付は "YYYY-MM-DD" 形式で、ES の date field がそのまま解釈する。
    """
    return (
        _first_value(idf, "Comment[Submission Date]"),
        _first_value(idf, "Comment[Last Update Date]"),
        _first_value(idf, "Public Release Date"),
    )


def create_metabobank_entry(
    accession: str,
    idf: dict[str, list[str]],
    dbxrefs: list[Xref] | None = None,
) -> MetaboBank:
    """IDF 辞書から MetaboBank インスタンスを構築する。"""
    date_created, date_modified, date_published = extract_dates(idf)
    return MetaboBank(
        identifier=accession,
        properties=idf,
        distribution=make_metabobank_distribution(accession),
        isPartOf="metabobank",
        type="metabobank",
        name=None,
        url=f"{SEARCH_BASE_URL}/search/entry/metabobank/{accession}",
        organism=None,
        title=extract_title(idf),
        description=extract_description(idf),
        organization=parse_submitter_affiliations(idf),
        publication=parse_pubmed_doi_publications(idf),
        studyType=extract_study_type(idf),
        experimentType=extract_experiment_type(idf),
        submissionType=extract_submission_type(idf),
        dbXrefs=dbxrefs or [],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def generate_metabobank_jsonl(
    config: Config,
    output_dir: Path,
    metabobank_base_path: Path,
    include_dbxrefs: bool = False,
) -> None:
    """MetaboBank の JSONL ファイルを生成する。"""
    entries: dict[str, MetaboBank] = {}

    for accession, idf_path in iterate_metabobank_idf_files(metabobank_base_path):
        log_debug(f"parsing idf file: {idf_path}", accession=accession)
        try:
            idf = parse_idf(idf_path)
        except Exception as e:
            log_warn(f"failed to parse idf file: {e}", accession=accession, file=str(idf_path))
            continue
        entries[accession] = create_metabobank_entry(accession, idf)

    log_info(f"processed {len(entries)} metabobank entries from {metabobank_base_path}")

    if include_dbxrefs:
        accessions = list(entries.keys())
        dbxref_map = get_dbxref_map(config, "metabobank", accessions)
        for accession, xrefs in dbxref_map.items():
            entries[accession].dbXrefs = xrefs

    output_path = output_dir / "metabobank.jsonl"
    write_jsonl(output_path, list(entries.values()))
    log_info(f"wrote {len(entries)} entries to jsonl file: {output_path}")


def parse_args(args: list[str]) -> tuple[Config, Path, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(description="Generate MetaboBank JSONL files from MetaboBank IDF files.")
    parser.add_argument(
        "--include-dbxrefs",
        help="Include dbXrefs in JSONL output.",
        action="store_true",
    )
    parsed = parser.parse_args(args)

    config = get_config()
    output_dir = config.result_dir / METABOBANK_BASE_DIR_NAME / JSONL_DIR_NAME / TODAY_STR
    return config, output_dir, parsed.include_dbxrefs


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, include_dbxrefs = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_metabobank_jsonl", config=config):
        log_debug(f"config: {config.model_dump_json(indent=2)}")
        log_debug(f"output directory: {output_dir}")
        log_debug(f"metabobank base path: {METABOBANK_BASE_PATH}")
        log_debug(f"include dbxrefs: {include_dbxrefs}")

        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"output directory: {output_dir}")

        generate_metabobank_jsonl(config, output_dir, METABOBANK_BASE_PATH, include_dbxrefs)


if __name__ == "__main__":
    main()
