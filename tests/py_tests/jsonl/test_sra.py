"""Tests for ddbj_search_converter.jsonl.sra module."""
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from ddbj_search_converter.dblink.utils import load_sra_blacklist
from ddbj_search_converter.jsonl.sra import (SRA_TYPE_MAP,
                                             _normalize_accessibility,
                                             _normalize_status,
                                             create_sra_entry, parse_analysis,
                                             parse_args, parse_experiment,
                                             parse_run, parse_sample,
                                             parse_study, parse_submission)
from ddbj_search_converter.jsonl.utils import write_jsonl
from ddbj_search_converter.logging.logger import run_logger
from ddbj_search_converter.schema import SRA, Organism
from ddbj_search_converter.sra.tar_reader import SraXmlType

# === Fixture discovery ===

FIXTURE_BASE = Path(__file__).resolve().parent.parent.parent / "fixtures" / "usr" / "local" / "resources" / "dra" / "fastq"

_XML_TYPE_SUFFIXES: Dict[str, str] = {
    "submission": ".submission.xml",
    "study": ".study.xml",
    "experiment": ".experiment.xml",
    "run": ".run.xml",
    "sample": ".sample.xml",
    "analysis": ".analysis.xml",
}


def _discover_fixture_xmls(xml_type: str) -> List[Tuple[str, Path]]:
    """指定された XML タイプの全フィクスチャファイルを探索する。"""
    suffix = _XML_TYPE_SUFFIXES[xml_type]
    results: List[Tuple[str, Path]] = []
    if not FIXTURE_BASE.exists():
        return results
    for xml_path in sorted(FIXTURE_BASE.rglob(f"*{suffix}")):
        submission_id = xml_path.name.replace(suffix, "")
        results.append((submission_id, xml_path))
    return results


# === Parse function map ===

_PARSE_FNS: Dict[str, Any] = {
    "submission": parse_submission,
    "study": parse_study,
    "experiment": parse_experiment,
    "run": parse_run,
    "sample": parse_sample,
    "analysis": parse_analysis,
}


def _parse_fixture(xml_type: str, xml_path: Path) -> List[Dict[str, Any]]:
    """フィクスチャ XML をパースして結果リストを返す。"""
    xml_bytes = xml_path.read_bytes()
    submission_id = xml_path.stem.split(".")[0]
    parse_fn = _PARSE_FNS[xml_type]

    if xml_type == "submission":
        result = parse_fn(xml_bytes, submission_id)
        return [result] if result and result.get("accession") else []
    return parse_fn(xml_bytes, submission_id)


# === Normalize tests ===


class TestNormalizeStatus:
    """Tests for _normalize_status function."""

    def test_returns_live_for_none(self) -> None:
        """None の場合は live を返す。"""
        assert _normalize_status(None) == "live"

    def test_returns_live_for_public(self) -> None:
        """public の場合は live を返す (DRA 互換)。"""
        assert _normalize_status("public") == "live"

    def test_returns_live_for_live(self) -> None:
        """live の場合は live を返す。"""
        assert _normalize_status("live") == "live"

    def test_returns_suppressed(self) -> None:
        """suppressed の場合は suppressed を返す。"""
        assert _normalize_status("suppressed") == "suppressed"

    def test_returns_withdrawn_for_replaced(self) -> None:
        """replaced の場合は withdrawn を返す (旧値互換)。"""
        assert _normalize_status("replaced") == "withdrawn"

    def test_returns_withdrawn_for_killed(self) -> None:
        """killed の場合は withdrawn を返す (旧値互換)。"""
        assert _normalize_status("killed") == "withdrawn"

    def test_returns_withdrawn_for_withdrawn(self) -> None:
        """withdrawn の場合は withdrawn を返す。"""
        assert _normalize_status("withdrawn") == "withdrawn"

    def test_returns_unpublished(self) -> None:
        """unpublished の場合は unpublished を返す。"""
        assert _normalize_status("unpublished") == "unpublished"

    def test_returns_live_for_unknown(self) -> None:
        """不明なステータスの場合は live を返す。"""
        assert _normalize_status("unknown") == "live"

    def test_case_insensitive(self) -> None:
        """大文字小文字を区別しない。"""
        assert _normalize_status("PUBLIC") == "live"
        assert _normalize_status("Suppressed") == "suppressed"


class TestNormalizeAccessibility:
    """Tests for _normalize_accessibility function."""

    def test_returns_public_access_for_none(self) -> None:
        """None の場合は public-access を返す。"""
        assert _normalize_accessibility(None) == "public-access"

    def test_returns_public_access(self) -> None:
        """public の場合は public-access を返す。"""
        assert _normalize_accessibility("public") == "public-access"

    def test_returns_controlled_access(self) -> None:
        """controlled-access の場合は controlled-access を返す。"""
        assert _normalize_accessibility("controlled-access") == "controlled-access"

    def test_returns_controlled_access_for_controlled(self) -> None:
        """controlled の場合は controlled-access を返す (BioSample 互換)。"""
        assert _normalize_accessibility("controlled") == "controlled-access"

    def test_returns_controlled_access_for_underscore(self) -> None:
        """controlled_access の場合は controlled-access を返す (アンダースコア互換)。"""
        assert _normalize_accessibility("controlled_access") == "controlled-access"

    def test_returns_public_access_for_unknown(self) -> None:
        """不明な accessibility の場合は public-access を返す。"""
        assert _normalize_accessibility("unknown") == "public-access"


class TestLoadSraBlacklist:
    """Tests for load_sra_blacklist function."""

    def test_returns_empty_for_missing_file(self, test_config):  # type: ignore
        """ファイルがない場合は空セットを返す。"""
        with run_logger(config=test_config):
            result = load_sra_blacklist(test_config)
            assert result == set()

    def test_loads_blacklist(self, test_config):  # type: ignore
        """blacklist ファイルを読み込む。"""
        blacklist_dir = test_config.const_dir / "sra"
        blacklist_dir.mkdir(parents=True, exist_ok=True)
        blacklist_path = blacklist_dir / "blacklist.txt"
        blacklist_path.write_text("DRA000001\nDRA000002\n# comment\nDRA000003\n")

        with run_logger(config=test_config):
            result = load_sra_blacklist(test_config)
            assert result == {"DRA000001", "DRA000002", "DRA000003"}


# === Parse function unit tests ===


class TestParseSubmission:
    """Tests for parse_submission function."""

    def test_parses_dra_submission(self) -> None:
        """DRA submission XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<SUBMISSION accession="DRA000001" broker_name="DRA" center_name="KEIO" alias="DRA000001" lab_name="Bioinformatics Lab." submission_comment="Test comment" submission_date="2009-05-14T23:16:00+09:00">
    <TITLE>Test Title</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml, accession="DRA000001")

        assert result is not None
        assert result["accession"] == "DRA000001"
        assert result["title"] == "Test Title"
        assert result["description"] == "Test comment"
        assert result["submission_date"] == "2009-05-14T23:16:00+09:00"

    def test_returns_none_for_missing_submission(self) -> None:
        """SUBMISSION 要素がない場合は None を返す。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ROOT>
    <OTHER/>
</ROOT>
"""
        result = parse_submission(xml, accession="test")
        assert result is None

    def test_empty_submission_comment_returns_none(self) -> None:
        """空の submission_comment は None を返す。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="DRA000001" submission_comment="">
    <TITLE>Test</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml, accession="DRA000001")
        assert result is not None
        assert result["description"] is None

    def test_submission_date_missing_returns_none(self) -> None:
        """submission_date 属性がない場合は None を返す。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="SRA000001">
    <TITLE>Test</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml, accession="SRA000001")
        assert result is not None
        assert result["submission_date"] is None


class TestParseStudy:
    """Tests for parse_study function."""

    def test_parses_dra_study(self) -> None:
        """DRA study XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<STUDY_SET>
    <STUDY accession="DRP000001" center_name="KEIO" alias="DRP000001">
        <DESCRIPTOR>
            <STUDY_TITLE>Whole genome sequencing</STUDY_TITLE>
            <STUDY_TYPE existing_study_type="Whole Genome Sequencing"/>
            <STUDY_ABSTRACT>Test abstract</STUDY_ABSTRACT>
        </DESCRIPTOR>
    </STUDY>
</STUDY_SET>
"""
        results = parse_study(xml, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRP000001"
        assert results[0]["title"] == "Whole genome sequencing"
        assert results[0]["description"] == "Test abstract"

    def test_parses_multiple_studies(self) -> None:
        """複数の study をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
    <STUDY accession="DRP000001" center_name="CENTER1"/>
    <STUDY accession="DRP000002" center_name="CENTER2"/>
</STUDY_SET>
"""
        results = parse_study(xml, accession="test")
        assert len(results) == 2

    def test_returns_empty_for_missing_study_set(self) -> None:
        """STUDY_SET 要素がない場合は空リストを返す。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ROOT>
    <OTHER/>
</ROOT>
"""
        results = parse_study(xml, accession="test")
        assert results == []

    def test_falls_back_to_study_description(self) -> None:
        """STUDY_ABSTRACT がない場合は STUDY_DESCRIPTION にフォールバックする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
    <STUDY accession="DRP000001">
        <DESCRIPTOR>
            <STUDY_TITLE>Title</STUDY_TITLE>
            <STUDY_DESCRIPTION>Fallback description</STUDY_DESCRIPTION>
        </DESCRIPTOR>
    </STUDY>
</STUDY_SET>
"""
        results = parse_study(xml, accession="test")
        assert results[0]["description"] == "Fallback description"


class TestParseExperiment:
    """Tests for parse_experiment function."""

    def test_parses_dra_experiment(self) -> None:
        """DRA experiment XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<EXPERIMENT_SET>
    <EXPERIMENT accession="DRX000001" center_name="KEIO" alias="DRX000001">
        <TITLE>B. subtilis genome sequencing</TITLE>
        <DESIGN>
            <DESIGN_DESCRIPTION>Random fragmentation</DESIGN_DESCRIPTION>
            <LIBRARY_DESCRIPTOR>
                <LIBRARY_STRATEGY>WGS</LIBRARY_STRATEGY>
                <LIBRARY_SOURCE>GENOMIC</LIBRARY_SOURCE>
                <LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
                <LIBRARY_LAYOUT>
                    <PAIRED/>
                </LIBRARY_LAYOUT>
            </LIBRARY_DESCRIPTOR>
        </DESIGN>
        <PLATFORM>
            <ILLUMINA>
                <INSTRUMENT_MODEL>Illumina Genome Analyzer II</INSTRUMENT_MODEL>
            </ILLUMINA>
        </PLATFORM>
    </EXPERIMENT>
</EXPERIMENT_SET>
"""
        results = parse_experiment(xml, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRX000001"
        assert results[0]["title"] == "B. subtilis genome sequencing"
        assert results[0]["description"] == "Random fragmentation"

    def test_empty_title_element(self) -> None:
        """空 TITLE 要素を正しく処理する (ERA パターン)。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<EXPERIMENT_SET>
    <EXPERIMENT accession="ERX000001">
        <TITLE/>
        <DESIGN>
            <DESIGN_DESCRIPTION/>
        </DESIGN>
    </EXPERIMENT>
</EXPERIMENT_SET>
"""
        results = parse_experiment(xml, accession="test")
        assert len(results) == 1
        assert results[0]["title"] is None
        assert results[0]["description"] is None


class TestParseRun:
    """Tests for parse_run function."""

    def test_parses_dra_run(self) -> None:
        """DRA run XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<RUN_SET>
    <RUN accession="DRR000001" center_name="KEIO" run_center="NIG" run_date="2008-09-13T01:27:27+09:00">
        <EXPERIMENT_REF accession="DRX000001"/>
    </RUN>
</RUN_SET>
"""
        results = parse_run(xml, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRR000001"
        assert results[0]["description"] is None


class TestParseSample:
    """Tests for parse_sample function."""

    def test_parses_dra_sample(self) -> None:
        """DRA sample XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<SAMPLE_SET>
    <SAMPLE accession="DRS000001" center_name="KEIO">
        <TITLE>Sample Title</TITLE>
        <SAMPLE_NAME>
            <TAXON_ID>645657</TAXON_ID>
            <SCIENTIFIC_NAME>Bacillus subtilis</SCIENTIFIC_NAME>
        </SAMPLE_NAME>
        <DESCRIPTION>Sample description</DESCRIPTION>
        <SAMPLE_ATTRIBUTES>
            <SAMPLE_ATTRIBUTE>
                <TAG>strain</TAG>
                <VALUE>BEST195</VALUE>
            </SAMPLE_ATTRIBUTE>
        </SAMPLE_ATTRIBUTES>
    </SAMPLE>
</SAMPLE_SET>
"""
        results = parse_sample(xml, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRS000001"
        assert results[0]["title"] == "Sample Title"
        assert results[0]["description"] == "Sample description"
        assert results[0]["organism"] is not None
        assert results[0]["organism"].identifier == "645657"
        assert results[0]["organism"].name == "Bacillus subtilis"


class TestParseAnalysis:
    """Tests for parse_analysis function."""

    def test_parses_dra_analysis(self) -> None:
        """DRA analysis XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ANALYSIS_SET>
    <ANALYSIS accession="DRZ000001" center_name="CENTER">
        <TITLE>Analysis Title</TITLE>
        <DESCRIPTION>Analysis description</DESCRIPTION>
        <ANALYSIS_TYPE>
            <REFERENCE_ALIGNMENT/>
        </ANALYSIS_TYPE>
    </ANALYSIS>
</ANALYSIS_SET>
"""
        results = parse_analysis(xml, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRZ000001"
        assert results[0]["title"] == "Analysis Title"
        assert results[0]["description"] == "Analysis description"


# === Create function tests ===


class TestCreateSraEntry:
    """Tests for create_sra_entry function."""

    def test_creates_submission(self) -> None:
        """SRA submission インスタンスを作成する。"""
        parsed: Dict[str, Any] = {
            "accession": "DRA000001",
            "title": "Test Submission",
            "description": "Test comment",
            "properties": {"SUBMISSION": {}},
        }

        result = create_sra_entry(
            "submission",
            parsed,
            status="live",
            accessibility="public-access",
            date_created="2009-05-14",
            date_modified="2014-05-12",
            date_published="2010-03-26",
        )

        assert result.identifier == "DRA000001"
        assert result.title == "Test Submission"
        assert result.description == "Test comment"
        assert result.status == "live"
        assert result.accessibility == "public-access"
        assert result.dateCreated == "2009-05-14"
        assert result.dateModified == "2014-05-12"
        assert result.datePublished == "2010-03-26"
        assert result.type_ == "sra-submission"
        assert result.isPartOf == "sra"

    def test_creates_study(self) -> None:
        """SRA study インスタンスを作成する。"""
        parsed: Dict[str, Any] = {
            "accession": "DRP000001",
            "title": "Study Title",
            "description": "Study description",
            "properties": {},
        }

        result = create_sra_entry(
            "study",
            parsed,
            status="live",
            accessibility="public-access",
            date_created=None,
            date_modified=None,
            date_published=None,
        )

        assert result.identifier == "DRP000001"
        assert result.title == "Study Title"
        assert result.type_ == "sra-study"

    def test_creates_sample_with_organism(self) -> None:
        """Sample のみ organism が設定される。"""
        parsed: Dict[str, Any] = {
            "accession": "DRS000001",
            "title": "Sample",
            "description": None,
            "organism": Organism(identifier="9606", name="Homo sapiens"),
            "properties": {},
        }

        result = create_sra_entry(
            "sample",
            parsed,
            status="live",
            accessibility="public-access",
            date_created=None,
            date_modified=None,
            date_published=None,
        )

        assert result.organism is not None
        assert result.organism.identifier == "9606"
        assert result.organism.name == "Homo sapiens"

    def test_non_sample_has_no_organism(self) -> None:
        """Sample 以外は organism が None。"""
        parsed: Dict[str, Any] = {
            "accession": "DRR000001",
            "title": "Run",
            "description": None,
            "properties": {},
        }

        result = create_sra_entry(
            "run",
            parsed,
            status="live",
            accessibility="public-access",
            date_created=None,
            date_modified=None,
            date_published=None,
        )

        assert result.organism is None

    def test_all_sra_types(self) -> None:
        """全 SRA タイプのエントリを作成できる。"""
        for sra_type, entry_type in SRA_TYPE_MAP.items():
            parsed: Dict[str, Any] = {
                "accession": f"TEST_{sra_type}",
                "title": None,
                "description": None,
                "properties": {},
            }
            result = create_sra_entry(
                sra_type,
                parsed,
                status="live",
                accessibility="public-access",
                date_created=None,
                date_modified=None,
                date_published=None,
            )
            assert result.type_ == entry_type
            assert result.identifier == f"TEST_{sra_type}"

    def test_accepts_iso8601_date_created(self) -> None:
        """ISO8601 形式の dateCreated を受け入れる（DRA の submission_date 形式）。"""
        parsed: Dict[str, Any] = {
            "accession": "DRA000072",
            "title": "Test",
            "description": None,
            "properties": {},
        }

        result = create_sra_entry(
            "submission",
            parsed,
            status="live",
            accessibility="public-access",
            date_created="2010-01-15T09:00:00+09:00",
            date_modified="2014-05-12",
            date_published="2010-03-26",
        )

        assert result.dateCreated == "2010-01-15T09:00:00+09:00"
        assert result.dateModified == "2014-05-12"
        assert result.datePublished == "2010-03-26"


class TestWriteJsonl:
    """Tests for write_jsonl function."""

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        """JSONL ファイルを書き込む。"""
        submission = SRA(
            identifier="DRA000001",
            properties={},
            distribution=[],
            isPartOf="sra",
            type="sra-submission",
            name=None,
            url="https://example.com",
            organism=None,
            title="Test",
            description=None,
            dbXrefs=[],
            sameAs=[],
            downloadUrl=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [submission])

        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "DRA000001" in content
        assert '"type":"sra-submission"' in content


class TestParseArgs:
    """Tests for parse_args function."""

    def test_default_args(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """デフォルト引数でパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, output_dir, parallel_num, full = parse_args([])
        assert config.result_dir == tmp_path
        assert "sra/jsonl" in str(output_dir)
        assert parallel_num == 8
        assert full is False

    def test_full_flag(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--full フラグをパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, output_dir, parallel_num, full = parse_args(["--full"])
        assert full is True

    def test_parallel_num(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--parallel-num オプションをパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, output_dir, parallel_num, full = parse_args(["--parallel-num", "16"])
        assert parallel_num == 16



# === Fixture-based parametrized tests ===


class TestFixtureSubmission:
    """Fixture-based tests for submission XML parsing."""

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("submission"),
        ids=[t[0] for t in _discover_fixture_xmls("submission")],
    )
    def test_parses_without_error(self, submission_id: str, xml_path: Path) -> None:
        """フィクスチャ XML がエラーなくパースできる。"""
        results = _parse_fixture("submission", xml_path)
        for entry in results:
            assert entry["accession"] is not None
            assert isinstance(entry["properties"], dict)
            assert entry["title"] is None or isinstance(entry["title"], str)
            assert entry["description"] is None or isinstance(entry["description"], str)

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("submission"),
        ids=[t[0] for t in _discover_fixture_xmls("submission")],
    )
    def test_creates_sra_instance(self, submission_id: str, xml_path: Path) -> None:
        """パース結果から SRA インスタンスを生成できる。"""
        results = _parse_fixture("submission", xml_path)
        for entry in results:
            sra = create_sra_entry(
                "submission", entry,
                status="live", accessibility="public-access",
                date_created=None, date_modified=None, date_published=None,
            )
            assert isinstance(sra, SRA)
            assert sra.type_ == "sra-submission"


class TestFixtureStudy:
    """Fixture-based tests for study XML parsing."""

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("study"),
        ids=[t[0] for t in _discover_fixture_xmls("study")],
    )
    def test_parses_without_error(self, submission_id: str, xml_path: Path) -> None:
        """フィクスチャ XML がエラーなくパースできる。"""
        results = _parse_fixture("study", xml_path)
        assert len(results) >= 1
        for entry in results:
            assert entry["accession"] is not None
            assert isinstance(entry["properties"], dict)
            assert entry["title"] is None or isinstance(entry["title"], str)
            assert entry["description"] is None or isinstance(entry["description"], str)

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("study"),
        ids=[t[0] for t in _discover_fixture_xmls("study")],
    )
    def test_creates_sra_instance(self, submission_id: str, xml_path: Path) -> None:
        """パース結果から SRA インスタンスを生成できる。"""
        results = _parse_fixture("study", xml_path)
        for entry in results:
            sra = create_sra_entry(
                "study", entry,
                status="live", accessibility="public-access",
                date_created=None, date_modified=None, date_published=None,
            )
            assert isinstance(sra, SRA)
            assert sra.type_ == "sra-study"


class TestFixtureExperiment:
    """Fixture-based tests for experiment XML parsing."""

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("experiment"),
        ids=[t[0] for t in _discover_fixture_xmls("experiment")],
    )
    def test_parses_without_error(self, submission_id: str, xml_path: Path) -> None:
        """フィクスチャ XML がエラーなくパースできる。"""
        results = _parse_fixture("experiment", xml_path)
        assert len(results) >= 1
        for entry in results:
            assert entry["accession"] is not None
            assert isinstance(entry["properties"], dict)
            assert entry["title"] is None or isinstance(entry["title"], str)
            assert entry["description"] is None or isinstance(entry["description"], str)

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("experiment"),
        ids=[t[0] for t in _discover_fixture_xmls("experiment")],
    )
    def test_creates_sra_instance(self, submission_id: str, xml_path: Path) -> None:
        """パース結果から SRA インスタンスを生成できる。"""
        results = _parse_fixture("experiment", xml_path)
        for entry in results:
            sra = create_sra_entry(
                "experiment", entry,
                status="live", accessibility="public-access",
                date_created=None, date_modified=None, date_published=None,
            )
            assert isinstance(sra, SRA)
            assert sra.type_ == "sra-experiment"


class TestFixtureRun:
    """Fixture-based tests for run XML parsing."""

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("run"),
        ids=[t[0] for t in _discover_fixture_xmls("run")],
    )
    def test_parses_without_error(self, submission_id: str, xml_path: Path) -> None:
        """フィクスチャ XML がエラーなくパースできる。"""
        results = _parse_fixture("run", xml_path)
        assert len(results) >= 1
        for entry in results:
            assert entry["accession"] is not None
            assert isinstance(entry["properties"], dict)
            assert entry["title"] is None or isinstance(entry["title"], str)
            assert entry["description"] is None

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("run"),
        ids=[t[0] for t in _discover_fixture_xmls("run")],
    )
    def test_creates_sra_instance(self, submission_id: str, xml_path: Path) -> None:
        """パース結果から SRA インスタンスを生成できる。"""
        results = _parse_fixture("run", xml_path)
        for entry in results:
            sra = create_sra_entry(
                "run", entry,
                status="live", accessibility="public-access",
                date_created=None, date_modified=None, date_published=None,
            )
            assert isinstance(sra, SRA)
            assert sra.type_ == "sra-run"


class TestFixtureSample:
    """Fixture-based tests for sample XML parsing."""

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("sample"),
        ids=[t[0] for t in _discover_fixture_xmls("sample")],
    )
    def test_parses_without_error(self, submission_id: str, xml_path: Path) -> None:
        """フィクスチャ XML がエラーなくパースできる。"""
        results = _parse_fixture("sample", xml_path)
        assert len(results) >= 1
        for entry in results:
            assert entry["accession"] is not None
            assert isinstance(entry["properties"], dict)
            assert entry["title"] is None or isinstance(entry["title"], str)
            assert entry["description"] is None or isinstance(entry["description"], str)
            assert entry["organism"] is None or isinstance(entry["organism"], Organism)

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("sample"),
        ids=[t[0] for t in _discover_fixture_xmls("sample")],
    )
    def test_creates_sra_instance(self, submission_id: str, xml_path: Path) -> None:
        """パース結果から SRA インスタンスを生成できる。"""
        results = _parse_fixture("sample", xml_path)
        for entry in results:
            sra = create_sra_entry(
                "sample", entry,
                status="live", accessibility="public-access",
                date_created=None, date_modified=None, date_published=None,
            )
            assert isinstance(sra, SRA)
            assert sra.type_ == "sra-sample"


class TestFixtureAnalysis:
    """Fixture-based tests for analysis XML parsing."""

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("analysis"),
        ids=[t[0] for t in _discover_fixture_xmls("analysis")],
    )
    def test_parses_without_error(self, submission_id: str, xml_path: Path) -> None:
        """フィクスチャ XML がエラーなくパースできる。"""
        results = _parse_fixture("analysis", xml_path)
        for entry in results:
            assert entry["accession"] is not None
            assert isinstance(entry["properties"], dict)
            assert entry["title"] is None or isinstance(entry["title"], str)
            assert entry["description"] is None or isinstance(entry["description"], str)

    @pytest.mark.parametrize(
        "submission_id,xml_path",
        _discover_fixture_xmls("analysis"),
        ids=[t[0] for t in _discover_fixture_xmls("analysis")],
    )
    def test_creates_sra_instance(self, submission_id: str, xml_path: Path) -> None:
        """パース結果から SRA インスタンスを生成できる。"""
        results = _parse_fixture("analysis", xml_path)
        for entry in results:
            sra = create_sra_entry(
                "analysis", entry,
                status="live", accessibility="public-access",
                date_created=None, date_modified=None, date_published=None,
            )
            assert isinstance(sra, SRA)
            assert sra.type_ == "sra-analysis"


# === Representative fixture concrete value tests ===


class TestDRA000072:
    """DRA000072 の具体値テスト (DRA 典型パターン)。"""

    def test_submission(self) -> None:
        xml_path = FIXTURE_BASE / "DRA000" / "DRA000072" / "DRA000072.submission.xml"
        result = parse_submission(xml_path.read_bytes(), "DRA000072")
        assert result is not None
        assert result["accession"] == "DRA000072"
        assert result["title"] == "DRA000072"
        assert result["description"] is None  # empty submission_comment
        assert result["submission_date"] == "2010-01-15T09:00:00+09:00"

    def test_study(self) -> None:
        xml_path = FIXTURE_BASE / "DRA000" / "DRA000072" / "DRA000072.study.xml"
        results = parse_study(xml_path.read_bytes(), "DRA000072")
        assert len(results) == 1
        assert results[0]["accession"] == "DRP000072"
        assert results[0]["title"] == "Whole genome analysis of Streptococcus salivarius"
        assert results[0]["description"] == "Whole genome analysis of Streptococcus salivarius"

    def test_experiment(self) -> None:
        xml_path = FIXTURE_BASE / "DRA000" / "DRA000072" / "DRA000072.experiment.xml"
        results = parse_experiment(xml_path.read_bytes(), "DRA000072")
        assert len(results) >= 1
        assert results[0]["accession"] is not None
        assert results[0]["title"] is None or isinstance(results[0]["title"], str)

    def test_sample(self) -> None:
        xml_path = FIXTURE_BASE / "DRA000" / "DRA000072" / "DRA000072.sample.xml"
        results = parse_sample(xml_path.read_bytes(), "DRA000072")
        assert len(results) >= 1
        assert results[0]["accession"] is not None
        assert results[0]["organism"] is None or isinstance(results[0]["organism"], Organism)

    def test_run(self) -> None:
        xml_path = FIXTURE_BASE / "DRA000" / "DRA000072" / "DRA000072.run.xml"
        results = parse_run(xml_path.read_bytes(), "DRA000072")
        assert len(results) >= 1
        assert results[0]["description"] is None

    def test_analysis(self) -> None:
        xml_path = FIXTURE_BASE / "DRA000" / "DRA000072" / "DRA000072.analysis.xml"
        results = parse_analysis(xml_path.read_bytes(), "DRA000072")
        # analysis は空の場合もある
        for entry in results:
            assert entry["accession"] is not None


class TestSRA000234:
    """SRA000234 の具体値テスト (NCBI 複数エントリパターン)。"""

    def test_submission(self) -> None:
        xml_path = FIXTURE_BASE / "SRA000" / "SRA000234" / "SRA000234.submission.xml"
        result = parse_submission(xml_path.read_bytes(), "SRA000234")
        assert result is not None
        assert result["accession"] == "SRA000234"
        assert result["title"] is None  # self-closing SUBMISSION
        assert result["description"] == "ftp submission manually prepared by shumwaym"

    def test_study(self) -> None:
        xml_path = FIXTURE_BASE / "SRA000" / "SRA000234" / "SRA000234.study.xml"
        results = parse_study(xml_path.read_bytes(), "SRA000234")
        assert len(results) == 1
        assert results[0]["accession"] == "SRP000105"
        assert "nucleosome" in results[0]["title"].lower()

    def test_experiment_multiple(self) -> None:
        xml_path = FIXTURE_BASE / "SRA000" / "SRA000234" / "SRA000234.experiment.xml"
        results = parse_experiment(xml_path.read_bytes(), "SRA000234")
        assert len(results) == 7
        accessions = [r["accession"] for r in results]
        assert "SRX000164" in accessions

    def test_sample_multiple(self) -> None:
        xml_path = FIXTURE_BASE / "SRA000" / "SRA000234" / "SRA000234.sample.xml"
        results = parse_sample(xml_path.read_bytes(), "SRA000234")
        assert len(results) == 7
        accessions = [r["accession"] for r in results]
        assert "SRS000331" in accessions


class TestERA000005:
    """ERA000005 の具体値テスト (ERA 空要素パターン)。"""

    def test_submission(self) -> None:
        xml_path = FIXTURE_BASE / "ERA000" / "ERA000005" / "ERA000005.submission.xml"
        result = parse_submission(xml_path.read_bytes(), "ERA000005")
        assert result is not None
        assert result["accession"] == "ERA000005"
        assert result["title"] is None  # no TITLE element
        assert result["description"] is None  # no submission_comment

    def test_study(self) -> None:
        xml_path = FIXTURE_BASE / "ERA000" / "ERA000005" / "ERA000005.study.xml"
        results = parse_study(xml_path.read_bytes(), "ERA000005")
        assert len(results) == 1
        assert results[0]["accession"] == "ERP000053"
        assert "diploid genome" in results[0]["title"].lower()

    def test_experiment_empty_title(self) -> None:
        xml_path = FIXTURE_BASE / "ERA000" / "ERA000005" / "ERA000005.experiment.xml"
        results = parse_experiment(xml_path.read_bytes(), "ERA000005")
        assert len(results) >= 1
        # ERA experiments often have <TITLE/> empty elements
        first = results[0]
        assert first["accession"] is not None
        # title can be None for empty <TITLE/> elements
        assert first["title"] is None or isinstance(first["title"], str)

    def test_sample(self) -> None:
        xml_path = FIXTURE_BASE / "ERA000" / "ERA000005" / "ERA000005.sample.xml"
        results = parse_sample(xml_path.read_bytes(), "ERA000005")
        assert len(results) >= 1
        assert results[0]["accession"] is not None
