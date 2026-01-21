"""Tests for ddbj_search_converter.jsonl.sra module."""
from pathlib import Path
from typing import Any, Dict

import pytest

from ddbj_search_converter.jsonl.sra import (
    _normalize_status,
    _normalize_visibility,
    create_analysis,
    create_experiment,
    create_run,
    create_sample,
    create_study,
    create_submission,
    load_sra_blacklist,
    parse_analysis,
    parse_args,
    parse_experiment,
    parse_run,
    parse_sample,
    parse_study,
    parse_submission,
    write_jsonl,
)
from ddbj_search_converter.schema import SraOrganism, SraSampleAttribute, SraSubmission


class TestNormalizeStatus:
    """Tests for _normalize_status function."""

    def test_returns_public_for_none(self) -> None:
        """None の場合は public を返す。"""
        assert _normalize_status(None) == "public"

    def test_returns_public_for_public(self) -> None:
        """public の場合は public を返す。"""
        assert _normalize_status("public") == "public"

    def test_returns_suppressed(self) -> None:
        """suppressed の場合は suppressed を返す。"""
        assert _normalize_status("suppressed") == "suppressed"

    def test_returns_replaced(self) -> None:
        """replaced の場合は replaced を返す。"""
        assert _normalize_status("replaced") == "replaced"

    def test_returns_killed(self) -> None:
        """killed の場合は killed を返す。"""
        assert _normalize_status("killed") == "killed"

    def test_returns_unpublished(self) -> None:
        """unpublished の場合は unpublished を返す。"""
        assert _normalize_status("unpublished") == "unpublished"

    def test_returns_public_for_unknown(self) -> None:
        """不明なステータスの場合は public を返す。"""
        assert _normalize_status("unknown") == "public"

    def test_case_insensitive(self) -> None:
        """大文字小文字を区別しない。"""
        assert _normalize_status("PUBLIC") == "public"
        assert _normalize_status("Suppressed") == "suppressed"


class TestNormalizeVisibility:
    """Tests for _normalize_visibility function."""

    def test_returns_public_for_none(self) -> None:
        """None の場合は public を返す。"""
        assert _normalize_visibility(None) == "public"

    def test_returns_public(self) -> None:
        """public の場合は public を返す。"""
        assert _normalize_visibility("public") == "public"

    def test_returns_controlled_access(self) -> None:
        """controlled-access の場合は controlled-access を返す。"""
        assert _normalize_visibility("controlled-access") == "controlled-access"

    def test_returns_public_for_unknown(self) -> None:
        """不明な visibility の場合は public を返す。"""
        assert _normalize_visibility("unknown") == "public"


class TestLoadSraBlacklist:
    """Tests for load_sra_blacklist function."""

    def test_returns_empty_for_missing_file(self, test_config):  # type: ignore
        """ファイルがない場合は空セットを返す。"""
        result = load_sra_blacklist(test_config)
        assert result == set()

    def test_loads_blacklist(self, test_config):  # type: ignore
        """blacklist ファイルを読み込む。"""
        blacklist_dir = test_config.const_dir / "sra"
        blacklist_dir.mkdir(parents=True, exist_ok=True)
        blacklist_path = blacklist_dir / "blacklist.txt"
        blacklist_path.write_text("DRA000001\nDRA000002\n# comment\nDRA000003\n")

        result = load_sra_blacklist(test_config)
        assert result == {"DRA000001", "DRA000002", "DRA000003"}


class TestParseSubmission:
    """Tests for parse_submission function."""

    def test_parses_dra_submission(self) -> None:
        """DRA submission XML をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<SUBMISSION accession="DRA000001" broker_name="DRA" center_name="KEIO" alias="DRA000001" lab_name="Bioinformatics Lab." submission_comment="Test comment" submission_date="2009-05-14T23:16:00+09:00">
    <TITLE>Test Title</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml, is_dra=True, accession="DRA000001")

        assert result is not None
        assert result["accession"] == "DRA000001"
        assert result["title"] == "Test Title"
        assert result["center_name"] == "KEIO"
        assert result["lab_name"] == "Bioinformatics Lab."
        assert result["submission_comment"] == "Test comment"

    def test_returns_none_for_missing_submission(self) -> None:
        """SUBMISSION 要素がない場合は None を返す。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ROOT>
    <OTHER/>
</ROOT>
"""
        result = parse_submission(xml, is_dra=True, accession="test")
        assert result is None


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
        results = parse_study(xml, is_dra=True, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRP000001"
        assert results[0]["title"] == "Whole genome sequencing"
        assert results[0]["description"] == "Test abstract"
        assert results[0]["study_type"] == "Whole Genome Sequencing"
        assert results[0]["center_name"] == "KEIO"

    def test_parses_multiple_studies(self) -> None:
        """複数の study をパースする。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
    <STUDY accession="DRP000001" center_name="CENTER1"/>
    <STUDY accession="DRP000002" center_name="CENTER2"/>
</STUDY_SET>
"""
        results = parse_study(xml, is_dra=True, accession="test")
        assert len(results) == 2

    def test_returns_empty_for_missing_study_set(self) -> None:
        """STUDY_SET 要素がない場合は空リストを返す。"""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ROOT>
    <OTHER/>
</ROOT>
"""
        results = parse_study(xml, is_dra=True, accession="test")
        assert results == []


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
        results = parse_experiment(xml, is_dra=True, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRX000001"
        assert results[0]["title"] == "B. subtilis genome sequencing"
        assert results[0]["description"] == "Random fragmentation"
        assert results[0]["instrument_model"] == "Illumina Genome Analyzer II"
        assert results[0]["library_strategy"] == "WGS"
        assert results[0]["library_source"] == "GENOMIC"
        assert results[0]["library_selection"] == "RANDOM"
        assert results[0]["library_layout"] == "PAIRED"


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
        results = parse_run(xml, is_dra=True, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRR000001"
        assert results[0]["run_date"] == "2008-09-13T01:27:27+09:00"
        assert results[0]["run_center"] == "NIG"
        assert results[0]["center_name"] == "KEIO"


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
        results = parse_sample(xml, is_dra=True, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRS000001"
        assert results[0]["title"] == "Sample Title"
        assert results[0]["description"] == "Sample description"
        assert results[0]["organism"] is not None
        assert results[0]["organism"].identifier == "645657"
        assert results[0]["organism"].name == "Bacillus subtilis"
        assert len(results[0]["attributes"]) == 1
        assert results[0]["attributes"][0].tag == "strain"
        assert results[0]["attributes"][0].value == "BEST195"


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
        results = parse_analysis(xml, is_dra=True, accession="DRA000001")

        assert len(results) == 1
        assert results[0]["accession"] == "DRZ000001"
        assert results[0]["title"] == "Analysis Title"
        assert results[0]["description"] == "Analysis description"
        assert results[0]["analysis_type"] == "REFERENCE_ALIGNMENT"


class TestCreateSubmission:
    """Tests for create_submission function."""

    def test_creates_submission(self) -> None:
        """SraSubmission インスタンスを作成する。"""
        parsed = {
            "accession": "DRA000001",
            "title": "Test Submission",
            "submission_comment": "Test comment",
            "center_name": "KEIO",
            "lab_name": "Lab",
            "properties": {"SUBMISSION": {}},
        }

        result = create_submission(
            parsed,
            status="public",
            visibility="public",
            date_created="2009-05-14",
            date_modified="2014-05-12",
            date_published="2010-03-26",
        )

        assert result.identifier == "DRA000001"
        assert result.title == "Test Submission"
        assert result.description == "Test comment"
        assert result.centerName == "KEIO"
        assert result.labName == "Lab"
        assert result.status == "public"
        assert result.visibility == "public"
        assert result.dateCreated == "2009-05-14"
        assert result.dateModified == "2014-05-12"
        assert result.datePublished == "2010-03-26"
        assert result.type_ == "sra-submission"
        assert result.isPartOf == "SRA"


class TestCreateStudy:
    """Tests for create_study function."""

    def test_creates_study(self) -> None:
        """SraStudy インスタンスを作成する。"""
        parsed = {
            "accession": "DRP000001",
            "title": "Study Title",
            "description": "Study description",
            "study_type": "Whole Genome Sequencing",
            "center_name": "KEIO",
            "properties": {},
        }

        result = create_study(
            parsed,
            status="public",
            visibility="public",
            date_created=None,
            date_modified=None,
            date_published=None,
        )

        assert result.identifier == "DRP000001"
        assert result.title == "Study Title"
        assert result.studyType == "Whole Genome Sequencing"
        assert result.type_ == "sra-study"


class TestWriteJsonl:
    """Tests for write_jsonl function."""

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        """JSONL ファイルを書き込む。"""
        submission = SraSubmission(
            identifier="DRA000001",
            properties={},
            distribution=[],
            isPartOf="SRA",
            type="sra-submission",
            name=None,
            url="https://example.com",
            title="Test",
            description=None,
            centerName="CENTER",
            labName=None,
            dbXref=[],
            sameAs=[],
            status="public",
            visibility="public",
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

    def test_result_dir(self, tmp_path: Path) -> None:
        """--result-dir オプションをパースする。"""
        result_dir = tmp_path / "custom_result"
        config, output_dir, parallel_num, full = parse_args(["--result-dir", str(result_dir)])
        assert config.result_dir == result_dir

    def test_date_option(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--date オプションをパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, output_dir, parallel_num, full = parse_args(["--date", "20260115"])
        assert "20260115" in str(output_dir)
