"""GEA JSONL 生成モジュール。"""

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path

from ddbj_search_converter.config import (
    GEA_BASE_DIR_NAME,
    GEA_BASE_PATH,
    JSONL_DIR_NAME,
    SEARCH_BASE_URL,
    TODAY_STR,
    Config,
    get_config,
)
from ddbj_search_converter.jsonl.distribution import make_gea_distribution
from ddbj_search_converter.jsonl.idf_common import (
    parse_idf,
    parse_pubmed_doi_publications,
    parse_submitter_affiliations,
)
from ddbj_search_converter.jsonl.utils import get_dbxref_map, write_jsonl
from ddbj_search_converter.logging.logger import log_debug, log_info, log_warn, run_logger
from ddbj_search_converter.schema import GEA, Xref


def iterate_gea_idf_files(base_path: Path) -> Iterator[tuple[str, Path]]:
    """GEA IDF ファイルを iterate する。

    E-GEAD-{N000}/E-GEAD-{NNNN}/{accession}.idf.txt の 2 階層走査。
    IDF ファイルが無いディレクトリは skip + log_warn。
    """
    if not base_path.exists():
        log_warn(f"gea base path does not exist: {base_path}")
        return

    for prefix_dir in sorted(base_path.iterdir()):
        if not prefix_dir.is_dir() or not prefix_dir.name.startswith("E-GEAD-"):
            continue
        for gea_dir in sorted(prefix_dir.iterdir()):
            if not gea_dir.is_dir() or not gea_dir.name.startswith("E-GEAD-"):
                continue
            accession = gea_dir.name
            idf_path = gea_dir / f"{accession}.idf.txt"
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


def extract_title(idf: dict[str, list[str]]) -> str | None:
    """GEA IDF からタイトルを抽出する (Investigation Title 最初値)。"""
    return _first_value(idf, "Investigation Title")


def extract_description(idf: dict[str, list[str]]) -> str | None:
    """GEA IDF から description を抽出する (Experiment Description 最初値)。"""
    return _first_value(idf, "Experiment Description")


def extract_experiment_type(idf: dict[str, list[str]]) -> list[str]:
    """GEA IDF から experimentType を抽出する (Comment[AEExperimentType] の非空値 list)。"""
    return [v.strip() for v in idf.get("Comment[AEExperimentType]", []) if isinstance(v, str) and v.strip()]


def extract_dates(idf: dict[str, list[str]]) -> tuple[None, str | None, str | None]:
    """GEA IDF から dates を抽出する。

    GEA IDF には submission date 相当のタグが無いため dateCreated は常に None。
    dateModified = Comment[Last Update Date] 最初値。
    datePublished = Public Release Date 最初値。
    IDF の日付は "YYYY-MM-DD" 形式で、ES の date field がそのまま解釈する。
    """
    return (
        None,
        _first_value(idf, "Comment[Last Update Date]"),
        _first_value(idf, "Public Release Date"),
    )


def create_gea_entry(
    accession: str,
    idf: dict[str, list[str]],
    dbxrefs: list[Xref] | None = None,
) -> GEA:
    """IDF 辞書から GEA インスタンスを構築する。"""
    date_created, date_modified, date_published = extract_dates(idf)
    return GEA(
        identifier=accession,
        properties=idf,
        distribution=make_gea_distribution(accession),
        isPartOf="gea",
        type="gea",
        name=None,
        url=f"{SEARCH_BASE_URL}/search/entry/gea/{accession}",
        organism=None,
        title=extract_title(idf),
        description=extract_description(idf),
        organization=parse_submitter_affiliations(idf),
        publication=parse_pubmed_doi_publications(idf),
        experimentType=extract_experiment_type(idf),
        dbXrefs=dbxrefs or [],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=date_created,
        dateModified=date_modified,
        datePublished=date_published,
    )


def generate_gea_jsonl(
    config: Config,
    output_dir: Path,
    gea_base_path: Path,
    include_dbxrefs: bool = False,
) -> None:
    """GEA の JSONL ファイルを生成する。"""
    gea_instances: dict[str, GEA] = {}

    for accession, idf_path in iterate_gea_idf_files(gea_base_path):
        log_debug(f"parsing idf file: {idf_path}", accession=accession)
        try:
            idf = parse_idf(idf_path)
        except Exception as e:
            log_warn(f"failed to parse idf file: {e}", accession=accession, file=str(idf_path))
            continue
        gea_instances[accession] = create_gea_entry(accession, idf)

    log_info(f"processed {len(gea_instances)} gea entries from {gea_base_path}")

    if include_dbxrefs:
        accessions = list(gea_instances.keys())
        dbxref_map = get_dbxref_map(config, "gea", accessions)
        for accession, xrefs in dbxref_map.items():
            gea_instances[accession].dbXrefs = xrefs

    output_path = output_dir / "gea.jsonl"
    write_jsonl(output_path, list(gea_instances.values()))
    log_info(f"wrote {len(gea_instances)} entries to jsonl file: {output_path}")


def parse_args(args: list[str]) -> tuple[Config, Path, bool]:
    """コマンドライン引数をパースする。"""
    parser = argparse.ArgumentParser(description="Generate GEA JSONL files from GEA IDF files.")
    parser.add_argument(
        "--include-dbxrefs",
        help="Include dbXrefs in JSONL output.",
        action="store_true",
    )
    parsed = parser.parse_args(args)

    config = get_config()
    output_dir = config.result_dir / GEA_BASE_DIR_NAME / JSONL_DIR_NAME / TODAY_STR
    return config, output_dir, parsed.include_dbxrefs


def main() -> None:
    """CLI エントリポイント。"""
    config, output_dir, include_dbxrefs = parse_args(sys.argv[1:])

    with run_logger(run_name="generate_gea_jsonl", config=config):
        log_debug(f"config: {config.model_dump_json(indent=2)}")
        log_debug(f"output directory: {output_dir}")
        log_debug(f"gea base path: {GEA_BASE_PATH}")
        log_debug(f"include dbxrefs: {include_dbxrefs}")

        output_dir.mkdir(parents=True, exist_ok=True)
        log_info(f"output directory: {output_dir}")

        generate_gea_jsonl(config, output_dir, GEA_BASE_PATH, include_dbxrefs)


if __name__ == "__main__":
    main()
