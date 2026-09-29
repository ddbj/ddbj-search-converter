"""BioProject / BioSample の分割 XML と、JSONL 生成が前提にする date cache を作る。

分割 XML は ``iterate_xml_element`` が行単位で要素を切り出すので、開始タグと
終了タグをそれぞれ独立した行に置く。
"""

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape, quoteattr

from ddbj_search_converter.config import Config
from ddbj_search_converter.date_cache.db import (
    DateRow,
    DateTable,
    finalize_date_cache_db,
    init_date_cache_db,
    load_dates,
    set_cache_meta,
    write_dates_tsv,
)


def ddbj_bp_package(accession: str, *, title: str = "title") -> str:
    return (
        "<Package>\n"
        "  <Project>\n"
        "    <Project>\n"
        "      <ProjectID>\n"
        f'        <ArchiveID accession={quoteattr(accession)} archive="DDBJ" />\n'
        "      </ProjectID>\n"
        "      <ProjectDescr>\n"
        f"        <Title>{escape(title)}</Title>\n"
        "      </ProjectDescr>\n"
        "    </Project>\n"
        "  </Project>\n"
        "</Package>\n"
    )


def ncbi_bp_package(
    accession: str,
    *,
    archive: str = "NCBI",
    title: str = "title",
    submitted: str = "2020-01-01",
    last_update: str | None = None,
    release_date: str | None = None,
) -> str:
    release = ""
    if release_date is not None:
        release = f"        <ProjectReleaseDate>{escape(release_date)}</ProjectReleaseDate>\n"
    last_update_attr = "" if last_update is None else f" last_update={quoteattr(last_update)}"
    return (
        "<Package>\n"
        "  <Project>\n"
        "    <Project>\n"
        "      <ProjectID>\n"
        f'        <ArchiveID accession={quoteattr(accession)} archive={quoteattr(archive)} id="1"/>\n'
        "      </ProjectID>\n"
        "      <ProjectDescr>\n"
        f"        <Title>{escape(title)}</Title>\n"
        f"{release}"
        "      </ProjectDescr>\n"
        "    </Project>\n"
        f"    <Submission{last_update_attr} submitted={quoteattr(submitted)}>\n"
        "      <Description>\n"
        "        <Access>public</Access>\n"
        "      </Description>\n"
        "    </Submission>\n"
        "  </Project>\n"
        "</Package>\n"
    )


def bp_xml(packages: Iterable[str]) -> str:
    return '<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>\n' + "".join(packages) + "</PackageSet>\n"


def ddbj_bs_sample(accession: str, *, title: str = "title") -> str:
    return (
        '<BioSample last_update="2020-01-01T00:00:00.000+09:00" access="public">\n'
        "  <Ids>\n"
        f'    <Id namespace="BioSample" is_primary="1">{escape(accession)}</Id>\n'
        "  </Ids>\n"
        "  <Description>\n"
        f"    <Title>{escape(title)}</Title>\n"
        "  </Description>\n"
        "</BioSample>\n"
    )


def ncbi_bs_sample(
    accession: str,
    *,
    title: str = "title",
    submission_date: str = "2020-01-01T00:00:00.000",
    last_update: str | None = None,
    publication_date: str = "2020-01-01T00:00:00.000",
) -> str:
    last_update_attr = "" if last_update is None else f" last_update={quoteattr(last_update)}"
    return (
        f"<BioSample submission_date={quoteattr(submission_date)}{last_update_attr}"
        f' publication_date={quoteattr(publication_date)} access="public" id="1" accession={quoteattr(accession)}>\n'
        "  <Ids>\n"
        f'    <Id db="BioSample" is_primary="1">{escape(accession)}</Id>\n'
        "  </Ids>\n"
        "  <Description>\n"
        f"    <Title>{escape(title)}</Title>\n"
        "  </Description>\n"
        "</BioSample>\n"
    )


def bs_xml(samples: Iterable[str]) -> str:
    return '<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n' + "".join(samples) + "</BioSampleSet>\n"


def build_date_cache(
    config: Config,
    *,
    bp_rows: Iterable[DateRow] = (),
    bs_rows: Iterable[DateRow] = (),
) -> None:
    """全件ビルド済みの date cache を作る。logger の初期化後に呼ぶ。"""
    tables: tuple[tuple[DateTable, Iterable[DateRow]], ...] = (("bp_date", bp_rows), ("bs_date", bs_rows))
    init_date_cache_db(config, seed_from_final=False)
    for table, rows in tables:
        written = write_dates_tsv(config, table, rows)
        load_dates(config, table, written, replace_existing=False)
        set_cache_meta(config, table, full_built_at="2026-01-01", watermark="2026-01-01")
    finalize_date_cache_db(config)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def read_jsonl_dir(output_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """出力ディレクトリの JSONL をファイル名ごとに読む。"""
    return {path.name: read_jsonl(path) for path in sorted(output_dir.glob("*.jsonl"))}
