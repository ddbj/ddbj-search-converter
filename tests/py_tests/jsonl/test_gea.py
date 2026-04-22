"""Tests for ddbj_search_converter.jsonl.gea module."""

import json
from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.gea import (
    _first_value,
    create_gea_entry,
    extract_dates,
    extract_description,
    extract_experiment_type,
    extract_title,
    generate_gea_jsonl,
    iterate_gea_idf_files,
)
from ddbj_search_converter.jsonl.idf_common import parse_idf
from ddbj_search_converter.logging.logger import run_logger


@pytest.fixture(autouse=True)
def _gea_logger_ctx(tmp_path: Path) -> Generator[None, None, None]:
    """Initialize logger context so `log_warn` / `log_debug` can be called in helpers."""
    config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
    with run_logger(run_name="test_gea", config=config):
        yield


GEA_FIXTURE_BASE = (
    Path(__file__).parent.parent.parent / "fixtures" / "usr" / "local" / "resources" / "gea" / "experiment"
)
E_GEAD_1005_IDF = GEA_FIXTURE_BASE / "E-GEAD-1000" / "E-GEAD-1005" / "E-GEAD-1005.idf.txt"


class TestIterateGeaIdfFiles:
    """iterate_gea_idf_files: 2 階層走査で (accession, idf_path) を yield する。"""

    def test_yields_all_fixture_entries(self) -> None:
        pairs = list(iterate_gea_idf_files(GEA_FIXTURE_BASE))
        accessions = sorted(p[0] for p in pairs)
        assert accessions == [
            "E-GEAD-1005",
            "E-GEAD-1017",
            "E-GEAD-1037",
            "E-GEAD-1039",
            "E-GEAD-1043",
            "E-GEAD-1044",
            "E-GEAD-1047",
            "E-GEAD-1057",
            "E-GEAD-1060",
            "E-GEAD-1096",
        ]

    def test_yielded_paths_point_to_existing_idf_files(self) -> None:
        for accession, idf_path in iterate_gea_idf_files(GEA_FIXTURE_BASE):
            assert idf_path.exists()
            assert idf_path.name == f"{accession}.idf.txt"

    def test_missing_base_path_yields_nothing(self, tmp_path: Path) -> None:
        missing = tmp_path / "does-not-exist"
        assert list(iterate_gea_idf_files(missing)) == []

    def test_dir_without_idf_is_skipped(self, tmp_path: Path) -> None:
        """{accession}.idf.txt が無いディレクトリは skip + log_warn で除外される。"""
        prefix = tmp_path / "E-GEAD-1000"
        gea_dir = prefix / "E-GEAD-9999"
        gea_dir.mkdir(parents=True)
        assert list(iterate_gea_idf_files(tmp_path)) == []

    def test_non_gea_dir_ignored(self, tmp_path: Path) -> None:
        """E-GEAD- プレフィックスではないディレクトリは走査対象外。"""
        other = tmp_path / "README"
        other.mkdir()
        assert list(iterate_gea_idf_files(tmp_path)) == []


class TestFirstValue:
    """_first_value: tag の最初の非空値を返す。"""

    def test_single_value(self) -> None:
        assert _first_value({"T": ["foo"]}, "T") == "foo"

    def test_strips_whitespace(self) -> None:
        assert _first_value({"T": ["  bar  "]}, "T") == "bar"

    def test_skips_empty_head(self) -> None:
        assert _first_value({"T": ["", "  ", "third"]}, "T") == "third"

    def test_missing_tag_returns_none(self) -> None:
        assert _first_value({}, "T") is None

    def test_all_empty_returns_none(self) -> None:
        assert _first_value({"T": ["", "   "]}, "T") is None

    def test_non_str_values_skipped(self) -> None:
        assert _first_value({"T": [None, "ok"]}, "T") == "ok"  # type: ignore[list-item]


class TestExtractTitle:
    def test_standard(self) -> None:
        assert extract_title({"Investigation Title": ["Hello"]}) == "Hello"

    def test_missing(self) -> None:
        assert extract_title({}) is None

    def test_empty_values(self) -> None:
        assert extract_title({"Investigation Title": [""]}) is None

    def test_fixture_e_gead_1005(self) -> None:
        idf = parse_idf(E_GEAD_1005_IDF)
        assert extract_title(idf) == "RT2ProfilerTM PCR Array-Human common cytokines (CBX140)"


class TestExtractDescription:
    def test_standard(self) -> None:
        assert extract_description({"Experiment Description": ["abc"]}) == "abc"

    def test_missing(self) -> None:
        assert extract_description({}) is None


class TestExtractExperimentType:
    def test_empty(self) -> None:
        assert extract_experiment_type({}) == []

    def test_single(self) -> None:
        assert extract_experiment_type({"Comment[AEExperimentType]": ["transcription profiling by array"]}) == [
            "transcription profiling by array"
        ]

    def test_multiple(self) -> None:
        idf = {"Comment[AEExperimentType]": ["RNA-seq of coding RNA", "ChIP-seq"]}
        assert extract_experiment_type(idf) == ["RNA-seq of coding RNA", "ChIP-seq"]

    def test_skips_empty(self) -> None:
        idf = {"Comment[AEExperimentType]": ["", "  ", "ATAC-seq"]}
        assert extract_experiment_type(idf) == ["ATAC-seq"]

    def test_strips_whitespace(self) -> None:
        idf = {"Comment[AEExperimentType]": ["  ChIP-seq  "]}
        assert extract_experiment_type(idf) == ["ChIP-seq"]

    def test_fixture_e_gead_1005(self) -> None:
        idf = parse_idf(E_GEAD_1005_IDF)
        assert extract_experiment_type(idf) == ["transcription profiling by array"]


class TestExtractDates:
    def test_date_created_always_none(self) -> None:
        idf = {
            "Comment[Submission Date]": ["2024-01-01"],
            "Comment[Last Update Date]": ["2024-02-01"],
            "Public Release Date": ["2024-03-01"],
        }
        assert extract_dates(idf)[0] is None

    def test_date_modified_from_last_update(self) -> None:
        idf = {"Comment[Last Update Date]": ["2024-02-01"]}
        _, modified, _ = extract_dates(idf)
        assert modified == "2024-02-01"

    def test_date_published_from_public_release(self) -> None:
        idf = {"Public Release Date": ["2024-03-01"]}
        _, _, published = extract_dates(idf)
        assert published == "2024-03-01"

    def test_all_missing(self) -> None:
        assert extract_dates({}) == (None, None, None)

    def test_fixture_e_gead_1005(self) -> None:
        idf = parse_idf(E_GEAD_1005_IDF)
        created, modified, published = extract_dates(idf)
        assert created is None
        assert modified == "2025-01-31"
        assert published == "2025-01-31"


class TestCreateGeaEntry:
    """create_gea_entry: E-GEAD-1005 fixture で end-to-end 構築。"""

    def test_e_gead_1005(self) -> None:
        idf = parse_idf(E_GEAD_1005_IDF)
        gea = create_gea_entry("E-GEAD-1005", idf)

        assert gea.identifier == "E-GEAD-1005"
        assert gea.isPartOf == "gea"
        assert gea.type_ == "gea"
        assert gea.name is None
        assert gea.organism is None
        assert gea.title == "RT2ProfilerTM PCR Array-Human common cytokines (CBX140)"
        assert gea.description is not None
        assert gea.status == "public"
        assert gea.accessibility == "public-access"
        assert gea.url == "https://ddbj.nig.ac.jp/search/entry/gea/E-GEAD-1005"
        assert gea.dateCreated is None
        assert gea.dateModified == "2025-01-31"
        assert gea.datePublished == "2025-01-31"

        assert [o.name for o in gea.organization] == ["Kyushu University"]
        assert all(o.role == "submitter" for o in gea.organization)

        assert len(gea.publication) == 1
        assert gea.publication[0].id_ == "21187441"
        assert gea.publication[0].dbType == "ePubmed"
        assert gea.publication[0].url == "https://pubmed.ncbi.nlm.nih.gov/21187441/"

        assert gea.experimentType == ["transcription profiling by array"]

        # distribution は JSON + JSON-LD の 2 件
        assert len(gea.distribution) == 2
        assert gea.distribution[0].encodingFormat == "JSON"
        assert gea.distribution[1].encodingFormat == "JSON-LD"

        # properties には IDF 全 tag が保持される
        assert gea.properties["Investigation Title"] == ["RT2ProfilerTM PCR Array-Human common cytokines (CBX140)"]

    def test_dbxrefs_default_empty(self) -> None:
        idf = parse_idf(E_GEAD_1005_IDF)
        gea = create_gea_entry("E-GEAD-1005", idf)
        assert gea.dbXrefs == []

    def test_dbxrefs_explicit(self) -> None:
        from ddbj_search_converter.schema import Xref

        idf = parse_idf(E_GEAD_1005_IDF)
        xref = Xref(
            identifier="PRJDB1",
            type="bioproject",
            url="https://ddbj.nig.ac.jp/search/entry/bioproject/PRJDB1",
        )
        gea = create_gea_entry("E-GEAD-1005", idf, dbxrefs=[xref])
        assert len(gea.dbXrefs) == 1
        assert gea.dbXrefs[0].identifier == "PRJDB1"


class TestGenerateGeaJsonlE2E:
    """generate_gea_jsonl: fixture を走らせて JSONL 出力を検証 (dblink 無し)。"""

    def test_writes_all_fixture_entries(self, tmp_path: Path) -> None:
        config = Config()
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        generate_gea_jsonl(config, output_dir, GEA_FIXTURE_BASE, include_dbxrefs=False)

        output_path = output_dir / "gea.jsonl"
        assert output_path.exists()

        with output_path.open(encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]

        assert len(lines) == 10
        accessions = sorted(entry["identifier"] for entry in lines)
        assert accessions[0] == "E-GEAD-1005"
        assert all(entry["isPartOf"] == "gea" for entry in lines)
        assert all(entry["type"] == "gea" for entry in lines)
        assert all(entry["status"] == "public" for entry in lines)
        assert all(entry["accessibility"] == "public-access" for entry in lines)
        # 各 entry が dbXrefs=[] で出る (include_dbxrefs=False)
        assert all(entry["dbXrefs"] == [] for entry in lines)
