"""Tests for ddbj_search_converter.jsonl.metabobank module."""

import json
from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.idf_common import parse_idf
from ddbj_search_converter.jsonl.metabobank import (
    _first_value,
    _non_empty_list,
    create_metabobank_entry,
    extract_dates,
    extract_description,
    extract_experiment_type,
    extract_study_type,
    extract_submission_type,
    extract_title,
    generate_metabobank_jsonl,
    iterate_metabobank_idf_files,
)
from ddbj_search_converter.logging.logger import run_logger


@pytest.fixture(autouse=True)
def _metabobank_logger_ctx(tmp_path: Path) -> Generator[None, None, None]:
    """Initialize logger context for helpers that call log_warn / log_debug."""
    config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
    with run_logger(run_name="test_metabobank", config=config):
        yield


METABOBANK_FIXTURE_BASE = (
    Path(__file__).parent.parent.parent / "fixtures" / "usr" / "local" / "shared_data" / "metabobank" / "study"
)
MTBKS102_IDF = METABOBANK_FIXTURE_BASE / "MTBKS102" / "MTBKS102.idf.txt"


class TestIterateMetabobankIdfFiles:
    """iterate_metabobank_idf_files: 1 階層走査で (accession, idf_path) を yield する。"""

    def test_yields_all_fixture_entries(self) -> None:
        pairs = list(iterate_metabobank_idf_files(METABOBANK_FIXTURE_BASE))
        accessions = sorted(p[0] for p in pairs)
        assert accessions == [
            "MTBKS102",
            "MTBKS103",
            "MTBKS208",
            "MTBKS232",
            "MTBKS238",
            "MTBKS241",
            "MTBKS264",
            "MTBKS70",
            "MTBKS71",
            "MTBKS85",
        ]

    def test_yielded_paths_point_to_existing_idf_files(self) -> None:
        for accession, idf_path in iterate_metabobank_idf_files(METABOBANK_FIXTURE_BASE):
            assert idf_path.exists()
            assert idf_path.name == f"{accession}.idf.txt"

    def test_missing_base_path_yields_nothing(self, tmp_path: Path) -> None:
        missing = tmp_path / "does-not-exist"
        assert list(iterate_metabobank_idf_files(missing)) == []

    def test_dir_without_idf_is_skipped(self, tmp_path: Path) -> None:
        """{accession}.idf.txt が無いディレクトリは skip + log_warn (S4 の 47 件ケース)。"""
        mtb_dir = tmp_path / "MTBKS999"
        mtb_dir.mkdir()
        assert list(iterate_metabobank_idf_files(tmp_path)) == []

    def test_non_mtbks_dir_ignored(self, tmp_path: Path) -> None:
        """MTBKS プレフィックスではないディレクトリは走査対象外。"""
        other = tmp_path / "README"
        other.mkdir()
        assert list(iterate_metabobank_idf_files(tmp_path)) == []


class TestFirstValue:
    def test_single_value(self) -> None:
        assert _first_value({"T": ["foo"]}, "T") == "foo"

    def test_strips_whitespace(self) -> None:
        assert _first_value({"T": ["  bar  "]}, "T") == "bar"

    def test_skips_empty_head(self) -> None:
        assert _first_value({"T": ["", "  ", "third"]}, "T") == "third"

    def test_missing_tag_returns_none(self) -> None:
        assert _first_value({}, "T") is None


class TestNonEmptyList:
    def test_empty_tag(self) -> None:
        assert _non_empty_list({}, "T") == []

    def test_strips_and_skips_empty(self) -> None:
        assert _non_empty_list({"T": ["  a  ", "", "b"]}, "T") == ["a", "b"]


class TestExtractTitle:
    def test_standard(self) -> None:
        assert extract_title({"Study Title": ["Hello"]}) == "Hello"

    def test_does_not_use_gea_tag(self) -> None:
        """GEA の Investigation Title は MetaboBank では拾わない (別タグ)。"""
        assert extract_title({"Investigation Title": ["Wrong"]}) is None

    def test_missing(self) -> None:
        assert extract_title({}) is None

    def test_fixture_mtbks102(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        assert extract_title(idf) == "Arabidopsis thaliana leaf metabolite analysis"


class TestExtractDescription:
    def test_standard(self) -> None:
        assert extract_description({"Study Description": ["abc"]}) == "abc"

    def test_does_not_use_gea_tag(self) -> None:
        """GEA の Experiment Description は MetaboBank では拾わない。"""
        assert extract_description({"Experiment Description": ["Wrong"]}) is None


class TestExtractStudyType:
    def test_empty(self) -> None:
        assert extract_study_type({}) == []

    def test_single(self) -> None:
        assert extract_study_type({"Comment[Study type]": ["untargeted metabolite profiling"]}) == [
            "untargeted metabolite profiling"
        ]

    def test_multiple(self) -> None:
        assert extract_study_type({"Comment[Study type]": ["a", "b"]}) == ["a", "b"]

    def test_fixture_mtbks102(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        assert extract_study_type(idf) == ["untargeted metabolite profiling"]


class TestExtractExperimentType:
    def test_empty(self) -> None:
        assert extract_experiment_type({}) == []

    def test_multiple(self) -> None:
        idf = {"Comment[Experiment type]": ["liquid chromatography-mass spectrometry", "TOF MS"]}
        assert extract_experiment_type(idf) == ["liquid chromatography-mass spectrometry", "TOF MS"]

    def test_fixture_mtbks102(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        assert extract_experiment_type(idf) == [
            "liquid chromatography-mass spectrometry",
            "fourier transform ion cyclotron resonance mass spectrometry",
        ]


class TestExtractSubmissionType:
    def test_empty(self) -> None:
        assert extract_submission_type({}) == []

    def test_single(self) -> None:
        assert extract_submission_type({"Comment[Submission type]": ["LC-DAD-MS"]}) == ["LC-DAD-MS"]

    def test_fixture_mtbks102(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        assert extract_submission_type(idf) == ["LC-DAD-MS"]


class TestExtractDates:
    def test_date_created_from_submission_date(self) -> None:
        created, _, _ = extract_dates({"Comment[Submission Date]": ["2022-05-22"]})
        assert created == "2022-05-22"

    def test_date_modified_from_last_update(self) -> None:
        _, modified, _ = extract_dates({"Comment[Last Update Date]": ["2022-06-01"]})
        assert modified == "2022-06-01"

    def test_date_published_from_public_release(self) -> None:
        _, _, published = extract_dates({"Public Release Date": ["2022-07-01"]})
        assert published == "2022-07-01"

    def test_all_missing(self) -> None:
        assert extract_dates({}) == (None, None, None)

    def test_fixture_mtbks102(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        created, modified, published = extract_dates(idf)
        assert created == "2022-05-22"
        assert modified is not None
        assert published is not None


class TestCreateMetabobankEntry:
    """create_metabobank_entry: MTBKS102 fixture で end-to-end 構築。"""

    def test_mtbks102(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        mtb = create_metabobank_entry("MTBKS102", idf)

        assert mtb.identifier == "MTBKS102"
        assert mtb.isPartOf == "metabobank"
        assert mtb.type_ == "metabobank"
        assert mtb.name is None
        assert mtb.organism is None
        assert mtb.title == "Arabidopsis thaliana leaf metabolite analysis"
        assert mtb.description is not None
        assert mtb.status == "public"
        assert mtb.accessibility == "public-access"
        assert mtb.url == "https://ddbj.nig.ac.jp/search/entry/metabobank/MTBKS102"
        assert mtb.dateCreated == "2022-05-22"

        assert [o.name for o in mtb.organization] == ["Kazusa DNA Research Institute"]
        assert all(o.role == "submitter" for o in mtb.organization)

        # MTBKS102 fixture は PubMed ID が無い (test_idf_common.py で確認済)
        assert mtb.publication == []

        assert mtb.studyType == ["untargeted metabolite profiling"]
        assert len(mtb.experimentType) == 2
        assert mtb.experimentType[0] == "liquid chromatography-mass spectrometry"
        assert mtb.submissionType == ["LC-DAD-MS"]

        # distribution は JSON + JSON-LD の 2 件
        assert len(mtb.distribution) == 2
        assert mtb.distribution[0].encodingFormat == "JSON"
        assert mtb.distribution[1].encodingFormat == "JSON-LD"

    def test_dbxrefs_default_empty(self) -> None:
        idf = parse_idf(MTBKS102_IDF)
        mtb = create_metabobank_entry("MTBKS102", idf)
        assert mtb.dbXrefs == []

    def test_dbxrefs_explicit(self) -> None:
        from ddbj_search_converter.schema import Xref

        idf = parse_idf(MTBKS102_IDF)
        xref = Xref(
            identifier="PRJDB1",
            type="bioproject",
            url="https://ddbj.nig.ac.jp/search/entry/bioproject/PRJDB1",
        )
        mtb = create_metabobank_entry("MTBKS102", idf, dbxrefs=[xref])
        assert len(mtb.dbXrefs) == 1
        assert mtb.dbXrefs[0].identifier == "PRJDB1"


class TestGenerateMetabobankJsonlE2E:
    """generate_metabobank_jsonl: fixture を走らせて JSONL 出力を検証 (dblink 無し)。"""

    def test_writes_all_fixture_entries(self, tmp_path: Path) -> None:
        config = Config()
        output_dir = tmp_path / "out"
        output_dir.mkdir()

        generate_metabobank_jsonl(config, output_dir, METABOBANK_FIXTURE_BASE, include_dbxrefs=False)

        output_path = output_dir / "metabobank.jsonl"
        assert output_path.exists()

        with output_path.open(encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]

        assert len(lines) == 10
        assert all(entry["isPartOf"] == "metabobank" for entry in lines)
        assert all(entry["type"] == "metabobank" for entry in lines)
        assert all(entry["status"] == "public" for entry in lines)
        assert all(entry["accessibility"] == "public-access" for entry in lines)
        assert all(entry["dbXrefs"] == [] for entry in lines)
        # dateCreated が Comment[Submission Date] 由来で、fixture 10 件全て埋まる
        assert all(entry["dateCreated"] is not None for entry in lines)
