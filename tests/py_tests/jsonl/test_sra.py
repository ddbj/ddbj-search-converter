"""Tests for ddbj_search_converter.jsonl.sra module.

SRA モジュールは tar ファイル読み込みに強く依存するため、
ここではパース関数と正規化関数のユニットテストを中心に行う。
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.sra import (
    XML_TYPES,
    _normalize_accessibility,
    _normalize_status,
    _parse_analysis_type,
    _parse_library,
    _parse_organization_from_center_name,
    _parse_publications,
    _parse_submission_description,
    create_sra_entry,
    parse_analysis,
    parse_experiment,
    parse_study,
    parse_submission,
    process_submission_xml,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger
from ddbj_search_converter.schema import SRA, Organization, Publication
from ddbj_search_converter.sra.tar_reader import SraXmlType


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


class TestNormalizeStatus:
    """Tests for _normalize_status function."""

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            (None, "public"),
            ("live", "public"),
            ("Live", "public"),
            ("unpublished", "private"),
            ("suppressed", "suppressed"),
            ("withdrawn", "withdrawn"),
            ("public", "public"),
            ("replaced", "suppressed"),
            ("killed", "withdrawn"),
            ("LIVE", "public"),
            ("PUBLIC", "public"),
            ("unknown_status", "public"),
            ("", "public"),
        ],
    )
    def test_normalize_status(self, input_val: str | None, expected: str) -> None:
        result = _normalize_status(input_val)
        assert result == expected


class TestNormalizeAccessibility:
    """Tests for _normalize_accessibility function."""

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            (None, "public-access"),
            ("public", "public-access"),
            ("controlled", "controlled-access"),
            ("controlled-access", "controlled-access"),
            ("controlled_access", "controlled-access"),
            ("Public", "public-access"),
            ("CONTROLLED", "controlled-access"),
            ("unknown", "public-access"),
            ("", "public-access"),
        ],
    )
    def test_normalize_accessibility(self, input_val: str | None, expected: str) -> None:
        result = _normalize_accessibility(input_val)
        assert result == expected


class TestParseSubmission:
    """Tests for parse_submission function."""

    def test_valid_submission(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="SRA123456" submission_date="2024-01-01" submission_comment="A comment">
  <TITLE>Test Submission</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml_bytes, "SRA123456")
        assert result is not None
        assert result["accession"] == "SRA123456"
        assert result["title"] == "Test Submission"
        assert result["description"] == "A comment"

    def test_empty_submission(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="SRA123456">
</SUBMISSION>
"""
        result = parse_submission(xml_bytes, "SRA123456")
        assert result is not None
        assert result["title"] is None

    def test_invalid_xml(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            result = parse_submission(b"<invalid", "X001")
            assert result is None

    def test_no_submission_element(self) -> None:
        xml_bytes = b"<OTHER>content</OTHER>"
        result = parse_submission(xml_bytes, "X001")
        assert result is None


class TestParseStudy:
    """Tests for parse_study function."""

    def test_valid_study(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
  <STUDY accession="SRP123456" alias="my_study">
    <DESCRIPTOR>
      <STUDY_TITLE>My Study Title</STUDY_TITLE>
      <STUDY_ABSTRACT>My study abstract</STUDY_ABSTRACT>
    </DESCRIPTOR>
  </STUDY>
</STUDY_SET>
"""
        results = parse_study(xml_bytes, "SRA123456")
        assert len(results) == 1
        assert results[0]["accession"] == "SRP123456"
        assert results[0]["title"] == "My Study Title"
        assert results[0]["description"] == "My study abstract"

    def test_empty_study_set(self) -> None:
        xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
</STUDY_SET>
"""
        results = parse_study(xml_bytes, "SRA123456")
        assert results == []

    def test_invalid_xml(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            results = parse_study(b"<invalid", "X001")
            assert results == []


class TestEdgeCases:
    """Edge case tests."""

    def test_status_case_insensitive(self) -> None:
        """大文字小文字を問わない。"""
        assert _normalize_status("LIVE") == "public"
        assert _normalize_status("Suppressed") == "suppressed"
        assert _normalize_status("WITHDRAWN") == "withdrawn"

    def test_accessibility_underscore_handling(self) -> None:
        """アンダースコアがハイフンに変換される。"""
        assert _normalize_accessibility("controlled_access") == "controlled-access"
        assert _normalize_accessibility("CONTROLLED_ACCESS") == "controlled-access"


class TestProcessSubmissionXml:
    """Tests for process_submission_xml function."""

    _SUBMISSION_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<SUBMISSION accession="DRA000001" submission_date="2020-01-01">
  <TITLE>Test DRA Submission</TITLE>
</SUBMISSION>
"""

    _STUDY_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<STUDY_SET>
  <STUDY accession="DRP000001">
    <DESCRIPTOR>
      <STUDY_TITLE>Test Study</STUDY_TITLE>
    </DESCRIPTOR>
  </STUDY>
</STUDY_SET>
"""

    def test_dra_uses_received_from_accessions_tab(self) -> None:
        """DRA でも Accessions.tab の Received が dateCreated に使われる。"""
        accession_info: dict[str, tuple[str, str, str | None, str | None, str | None, str]] = {
            "DRA000001": ("live", "public", "2024-06-15", "2024-07-01", "2024-08-01", "submission"),
            "DRP000001": ("live", "public", "2024-06-15", "2024-07-01", "2024-08-01", "study"),
        }
        xml_cache: dict[SraXmlType, bytes | None] = {
            "submission": self._SUBMISSION_XML,
            "study": self._STUDY_XML,
            "experiment": None,
            "run": None,
            "sample": None,
            "analysis": None,
        }

        results = process_submission_xml(
            submission="DRA000001",
            blacklist=set(),
            accession_info=accession_info,
            xml_cache=xml_cache,
        )

        # submission の dateCreated が Accessions.tab の Received であること
        assert len(results["submission"]) == 1
        assert results["submission"][0].dateCreated == "2024-06-15"

        # study も同様に Accessions.tab の Received であること
        assert len(results["study"]) == 1
        assert results["study"][0].dateCreated == "2024-06-15"

    def test_received_none_results_in_date_created_none(self) -> None:
        """Received が None の場合、dateCreated が None になる。"""
        accession_info: dict[str, tuple[str, str, str | None, str | None, str | None, str]] = {
            "DRA000001": ("live", "public", None, None, None, "submission"),
            "DRP000001": ("live", "public", None, None, None, "study"),
        }
        xml_cache: dict[SraXmlType, bytes | None] = {
            "submission": self._SUBMISSION_XML,
            "study": self._STUDY_XML,
            "experiment": None,
            "run": None,
            "sample": None,
            "analysis": None,
        }

        results = process_submission_xml(
            submission="DRA000001",
            blacklist=set(),
            accession_info=accession_info,
            xml_cache=xml_cache,
        )

        assert len(results["submission"]) == 1
        assert results["submission"][0].dateCreated is None

        assert len(results["study"]) == 1
        assert results["study"][0].dateCreated is None


def _make_sra_entry(identifier: str, sra_type: SraXmlType = "study") -> SRA:
    """テスト用の SRA エントリを作成するヘルパー。"""
    parsed: dict[str, Any] = {
        "accession": identifier,
        "properties": {},
        "alias": None,
        "title": f"Title for {identifier}",
        "description": None,
    }
    return create_sra_entry(
        sra_type=sra_type,
        parsed=parsed,
        status="public",
        accessibility="public-access",
        date_created=None,
        date_modified=None,
        date_published=None,
    )


class TestBatchDedup:
    """_process_batch_worker の重複排除ロジックを検証する。

    実際の _process_batch_worker は tar 読み込みや DB 依存が大きいため、
    Step 3 の重複排除ロジック部分を単体で再現してテストする。
    """

    def test_dedup_removes_duplicates_within_same_type(self) -> None:
        """同一 xml_type 内で重複する identifier が排除される。"""
        entries = [
            _make_sra_entry("SRP000001", "study"),
            _make_sra_entry("SRP000002", "study"),
            _make_sra_entry("SRP000001", "study"),  # duplicate
            _make_sra_entry("SRP000003", "study"),
        ]

        batch_entries: list[SRA] = []
        seen_ids: set[str] = set()
        for entry in entries:
            if entry.identifier not in seen_ids:
                batch_entries.append(entry)
                seen_ids.add(entry.identifier)

        assert len(batch_entries) == 3
        ids = [e.identifier for e in batch_entries]
        assert ids == ["SRP000001", "SRP000002", "SRP000003"]

    def test_dedup_across_submissions(self) -> None:
        """異なる submission から生成された同一 identifier が排除される。"""
        sub1_results = [
            _make_sra_entry("SRP000001", "study"),
            _make_sra_entry("SRP000002", "study"),
        ]
        sub2_results = [
            _make_sra_entry("SRP000001", "study"),  # same as sub1
            _make_sra_entry("SRP000003", "study"),
        ]

        batch_entries: dict[SraXmlType, list[SRA]] = {t: [] for t in XML_TYPES}
        seen_ids: dict[SraXmlType, set[str]] = {t: set() for t in XML_TYPES}

        for results in [sub1_results, sub2_results]:
            for entry in results:
                if entry.identifier not in seen_ids["study"]:
                    batch_entries["study"].append(entry)
                    seen_ids["study"].add(entry.identifier)

        assert len(batch_entries["study"]) == 3
        ids = [e.identifier for e in batch_entries["study"]]
        assert ids == ["SRP000001", "SRP000002", "SRP000003"]

    def test_dedup_independent_across_types(self) -> None:
        """異なる xml_type 間では重複排除が独立して行われる。"""
        study_entries = [_make_sra_entry("SRP000001", "study")]
        sample_entries = [_make_sra_entry("SRS000001", "sample")]

        batch_entries: dict[SraXmlType, list[SRA]] = {t: [] for t in XML_TYPES}
        seen_ids: dict[SraXmlType, set[str]] = {t: set() for t in XML_TYPES}

        for entry in study_entries:
            if entry.identifier not in seen_ids["study"]:
                batch_entries["study"].append(entry)
                seen_ids["study"].add(entry.identifier)

        for entry in sample_entries:
            if entry.identifier not in seen_ids["sample"]:
                batch_entries["sample"].append(entry)
                seen_ids["sample"].add(entry.identifier)

        assert len(batch_entries["study"]) == 1
        assert len(batch_entries["sample"]) == 1

    def test_dedup_preserves_first_occurrence(self) -> None:
        """重複がある場合、最初の出現が保持される。"""
        entry1 = _make_sra_entry("SRP000001", "study")
        entry1_dup = _make_sra_entry("SRP000001", "study")

        batch_entries: list[SRA] = []
        seen_ids: set[str] = set()
        for entry in [entry1, entry1_dup]:
            if entry.identifier not in seen_ids:
                batch_entries.append(entry)
                seen_ids.add(entry.identifier)

        assert len(batch_entries) == 1
        assert batch_entries[0] is entry1

    def test_no_duplicates_all_kept(self) -> None:
        """重複がない場合、全エントリが保持される。"""
        entries = [
            _make_sra_entry("SRP000001", "study"),
            _make_sra_entry("SRP000002", "study"),
            _make_sra_entry("SRP000003", "study"),
        ]

        batch_entries: list[SRA] = []
        seen_ids: set[str] = set()
        for entry in entries:
            if entry.identifier not in seen_ids:
                batch_entries.append(entry)
                seen_ids.add(entry.identifier)

        assert len(batch_entries) == 3


class TestCreateSraEntryProperties:
    """create_sra_entry 実行後の properties 内 Attribute 正規化を検証する。"""

    def _call(self, sra_type: SraXmlType, properties: dict[str, Any]) -> SRA:
        parsed: dict[str, Any] = {
            "accession": "ACC000001",
            "properties": properties,
            "alias": None,
            "title": None,
            "description": None,
        }
        return create_sra_entry(
            sra_type=sra_type,
            parsed=parsed,
            status="public",
            accessibility="public-access",
            date_created=None,
            date_modified=None,
            date_published=None,
        )

    def test_study_single_attribute_becomes_list(self) -> None:
        props = {"STUDY_SET": {"STUDY": {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "k", "VALUE": "v"}}}}}
        sra = self._call("study", props)
        attrs = sra.properties["STUDY_SET"]["STUDY"]["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"]
        assert isinstance(attrs, list)
        assert attrs == [{"TAG": "k", "VALUE": "v"}]

    def test_study_multiple_attributes_stay_list(self) -> None:
        props = {"STUDY_SET": {"STUDY": {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": [{"TAG": "a"}, {"TAG": "b"}]}}}}
        sra = self._call("study", props)
        attrs = sra.properties["STUDY_SET"]["STUDY"]["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"]
        assert len(attrs) == 2

    def test_experiment_attribute_becomes_list(self) -> None:
        props = {"EXPERIMENT_SET": {"EXPERIMENT": {"EXPERIMENT_ATTRIBUTES": {"EXPERIMENT_ATTRIBUTE": {"TAG": "k"}}}}}
        sra = self._call("experiment", props)
        attrs = sra.properties["EXPERIMENT_SET"]["EXPERIMENT"]["EXPERIMENT_ATTRIBUTES"]["EXPERIMENT_ATTRIBUTE"]
        assert isinstance(attrs, list)

    def test_run_attribute_becomes_list(self) -> None:
        props = {"RUN_SET": {"RUN": {"RUN_ATTRIBUTES": {"RUN_ATTRIBUTE": {"TAG": "k"}}}}}
        sra = self._call("run", props)
        attrs = sra.properties["RUN_SET"]["RUN"]["RUN_ATTRIBUTES"]["RUN_ATTRIBUTE"]
        assert isinstance(attrs, list)

    def test_sample_attribute_becomes_list(self) -> None:
        props = {"SAMPLE_SET": {"SAMPLE": {"SAMPLE_ATTRIBUTES": {"SAMPLE_ATTRIBUTE": {"TAG": "k"}}}}}
        sra = self._call("sample", props)
        attrs = sra.properties["SAMPLE_SET"]["SAMPLE"]["SAMPLE_ATTRIBUTES"]["SAMPLE_ATTRIBUTE"]
        assert isinstance(attrs, list)

    def test_analysis_attribute_becomes_list(self) -> None:
        props = {"ANALYSIS_SET": {"ANALYSIS": {"ANALYSIS_ATTRIBUTES": {"ANALYSIS_ATTRIBUTE": {"TAG": "k"}}}}}
        sra = self._call("analysis", props)
        attrs = sra.properties["ANALYSIS_SET"]["ANALYSIS"]["ANALYSIS_ATTRIBUTES"]["ANALYSIS_ATTRIBUTE"]
        assert isinstance(attrs, list)

    def test_submission_no_op(self) -> None:
        """sra-submission は対象外。properties の構造が保持される。"""
        props = {"SUBMISSION": {"TITLE": "Test"}}
        sra = self._call("submission", props)
        assert isinstance(sra.properties["SUBMISSION"], dict)
        assert sra.properties["SUBMISSION"]["TITLE"] == "Test"

    def test_multiple_studies_each_normalized(self) -> None:
        """STUDY が list（複数 entry）の場合、各要素で正規化される。"""
        props = {
            "STUDY_SET": {
                "STUDY": [
                    {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "a"}}},
                    {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": [{"TAG": "b"}, {"TAG": "c"}]}},
                ]
            }
        }
        sra = self._call("study", props)
        studies = sra.properties["STUDY_SET"]["STUDY"]
        assert studies[0]["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"] == [{"TAG": "a"}]
        assert len(studies[1]["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"]) == 2

    def test_other_fields_not_wrapped(self) -> None:
        """DESCRIPTOR 等は配列化されない（回帰テスト）。"""
        props = {
            "STUDY_SET": {
                "STUDY": {
                    "DESCRIPTOR": {"STUDY_TITLE": "Title"},
                    "STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "k"}},
                }
            }
        }
        sra = self._call("study", props)
        study = sra.properties["STUDY_SET"]["STUDY"]
        assert isinstance(study["DESCRIPTOR"], dict)
        assert study["DESCRIPTOR"]["STUDY_TITLE"] == "Title"


class TestParseOrganizationFromCenterName:
    """_parse_organization_from_center_name (§3.3 SRA organization from @center_name)."""

    def test_center_name_present(self) -> None:
        entry = {"accession": "DRA000072", "center_name": "NIID"}
        orgs = _parse_organization_from_center_name(entry)
        assert orgs == [Organization(name="NIID")]
        assert orgs[0].role is None
        assert orgs[0].organizationType is None

    def test_center_name_trimmed(self) -> None:
        entry = {"center_name": "  NIID  "}
        orgs = _parse_organization_from_center_name(entry)
        assert orgs == [Organization(name="NIID")]

    def test_center_name_missing_returns_empty(self) -> None:
        assert _parse_organization_from_center_name({"accession": "X"}) == []

    def test_center_name_empty_string_returns_empty(self) -> None:
        assert _parse_organization_from_center_name({"center_name": ""}) == []
        assert _parse_organization_from_center_name({"center_name": "   "}) == []

    def test_non_dict_input_returns_empty(self) -> None:
        assert _parse_organization_from_center_name(None) == []
        assert _parse_organization_from_center_name([]) == []
        assert _parse_organization_from_center_name("NIID") == []


class TestL8XrefLinkDbNormalization:
    """§4.11 L8: XREF_LINK.DB の lower() 正規化 + pubmed のみ Publication 化。"""

    @pytest.mark.parametrize("db_value", ["pubmed", "PUBMED", "PubMed", "Pubmed", "  PubMed  "])
    def test_all_case_variants_normalized_to_pubmed(self, db_value: str) -> None:
        entry = {"STUDY_LINKS": {"STUDY_LINK": {"XREF_LINK": {"DB": db_value, "ID": "12345"}}}}
        pubs = _parse_publications(entry, "STUDY_LINKS", "STUDY_LINK")
        assert len(pubs) == 1
        assert pubs[0].id_ == "12345"
        assert pubs[0].dbType == "ePubmed"
        assert pubs[0].url == "https://pubmed.ncbi.nlm.nih.gov/12345/"

    def test_bioproject_db_not_included(self) -> None:
        """§4.9.4: bioproject は publication に詰めない (dbXrefs 系扱い)。"""
        entry = {"SAMPLE_LINKS": {"SAMPLE_LINK": {"XREF_LINK": {"DB": "bioproject", "ID": "PRJNA12345"}}}}
        assert _parse_publications(entry, "SAMPLE_LINKS", "SAMPLE_LINK") == []

    @pytest.mark.parametrize("db_value", ["gds", "geo", "omim", "nuccore", "ENA-STUDY", "biosample"])
    def test_other_dbs_not_included(self, db_value: str) -> None:
        entry = {"EXPERIMENT_LINKS": {"EXPERIMENT_LINK": {"XREF_LINK": {"DB": db_value, "ID": "X1"}}}}
        assert _parse_publications(entry, "EXPERIMENT_LINKS", "EXPERIMENT_LINK") == []

    def test_empty_db_or_id_skipped(self) -> None:
        """空の DB / 空の ID はすべて skip。"""
        for xref in [
            {"DB": "", "ID": ""},
            {"DB": "pubmed", "ID": ""},
            {"DB": "", "ID": "123"},
            {"DB": "   ", "ID": "123"},
        ]:
            entry = {"ANALYSIS_LINKS": {"ANALYSIS_LINK": {"XREF_LINK": xref}}}
            assert _parse_publications(entry, "ANALYSIS_LINKS", "ANALYSIS_LINK") == []

    def test_multiple_links_as_list(self) -> None:
        entry = {
            "STUDY_LINKS": {
                "STUDY_LINK": [
                    {"XREF_LINK": {"DB": "pubmed", "ID": "1"}},
                    {"XREF_LINK": {"DB": "PUBMED", "ID": "2"}},
                    {"XREF_LINK": {"DB": "gds", "ID": "3"}},
                ]
            }
        }
        pubs = _parse_publications(entry, "STUDY_LINKS", "STUDY_LINK")
        assert [p.id_ for p in pubs] == ["1", "2"]
        assert all(p.dbType == "ePubmed" for p in pubs)

    def test_no_links_element_returns_empty(self) -> None:
        assert _parse_publications({"accession": "X"}, "STUDY_LINKS", "STUDY_LINK") == []
        assert _parse_publications(None, "STUDY_LINKS", "STUDY_LINK") == []
        assert _parse_publications({"STUDY_LINKS": None}, "STUDY_LINKS", "STUDY_LINK") == []


class TestParseLibrary:
    """_parse_library (§3.3 experiment 専用)."""

    def test_typical_illumina_paired(self) -> None:
        exp = {
            "DESIGN": {
                "LIBRARY_DESCRIPTOR": {
                    "LIBRARY_STRATEGY": "WGS",
                    "LIBRARY_SOURCE": "GENOMIC",
                    "LIBRARY_SELECTION": "RANDOM",
                    "LIBRARY_LAYOUT": {"PAIRED": {}},
                }
            },
            "PLATFORM": {"ILLUMINA": {"INSTRUMENT_MODEL": "Illumina NovaSeq 6000"}},
        }
        result = _parse_library(exp)
        assert result["libraryStrategy"] == ["WGS"]
        assert result["librarySource"] == ["GENOMIC"]
        assert result["librarySelection"] == ["RANDOM"]
        assert result["libraryLayout"] == "PAIRED"
        assert result["platform"] == "ILLUMINA"
        assert result["instrumentModel"] == ["Illumina NovaSeq 6000"]

    def test_single_layout(self) -> None:
        exp = {"DESIGN": {"LIBRARY_DESCRIPTOR": {"LIBRARY_LAYOUT": {"SINGLE": None}}}}
        assert _parse_library(exp)["libraryLayout"] == "SINGLE"

    def test_library_source_invalid_value_falls_back_to_empty_list(self) -> None:
        """Literal safeguard: 想定外値は空 list fallback (§4.9.2 9 値 controlled)。"""
        exp = {"DESIGN": {"LIBRARY_DESCRIPTOR": {"LIBRARY_SOURCE": "NOVEL_SOURCE_NOT_IN_LITERAL"}}}
        assert _parse_library(exp)["librarySource"] == []

    def test_library_layout_multi_fallbacks_to_none(self) -> None:
        """複数キー (PAIRED + SINGLE 同居) は None fallback (§4.9.1 実データ 0 件だが防御)。"""
        exp = {"DESIGN": {"LIBRARY_DESCRIPTOR": {"LIBRARY_LAYOUT": {"PAIRED": {}, "SINGLE": {}}}}}
        assert _parse_library(exp)["libraryLayout"] is None

    def test_platform_multi_fallbacks_to_none(self) -> None:
        """複数 platform キーは None fallback。"""
        exp = {"PLATFORM": {"ILLUMINA": {}, "OXFORD_NANOPORE": {}}}
        assert _parse_library(exp)["platform"] is None

    def test_no_library_descriptor_returns_defaults(self) -> None:
        result = _parse_library({})
        assert result["libraryStrategy"] == []
        assert result["librarySource"] == []
        assert result["librarySelection"] == []
        assert result["libraryLayout"] is None
        assert result["platform"] is None
        assert result["instrumentModel"] == []

    def test_non_dict_input_returns_defaults(self) -> None:
        result = _parse_library(None)
        assert result["libraryLayout"] is None
        assert result["platform"] is None


class TestL9LxmlCommentInExperiment:
    """§4.11 L9: lxml Comment quirk の regression。

    xml_utils._element_to_dict は既に cyfunction を skip する fix 済だが、
    _parse_library の valid_keys フィルタで想定外キーが残ったケースでも
    安全側に None fallback するかの二重防御を固定する。
    """

    def test_platform_with_comment_quirk_selects_valid_key(self) -> None:
        """PLATFORM dict に cyfunction 想定外キーが混ざっても、valid_platforms==1 なら採用。"""
        exp = {
            "PLATFORM": {
                "ILLUMINA": {"INSTRUMENT_MODEL": "Illumina"},
                "<cyfunction Comment at 0x...>": "ignored",
            }
        }
        result = _parse_library(exp)
        assert result["platform"] == "ILLUMINA"
        assert result["instrumentModel"] == ["Illumina"]

    def test_layout_with_only_comment_quirk_returns_none(self) -> None:
        """LIBRARY_LAYOUT に cyfunction 想定外キーのみの場合 None fallback。"""
        exp = {"DESIGN": {"LIBRARY_DESCRIPTOR": {"LIBRARY_LAYOUT": {"<cyfunction Comment at 0x...>": "ignored"}}}}
        assert _parse_library(exp)["libraryLayout"] is None

    def test_end_to_end_parse_experiment_with_comment_xml(self) -> None:
        """xml_utils._element_to_dict の Comment skip fix の end-to-end regression。
        XML に <!-- comment --> を入れても PAIRED / ILLUMINA / INSTRUMENT_MODEL が正しく取れる。
        """
        xml_bytes = b"""<?xml version="1.0"?>
<EXPERIMENT_SET>
  <EXPERIMENT accession="DRX000001" center_name="NIID">
    <TITLE>T</TITLE>
    <DESIGN>
      <LIBRARY_DESCRIPTOR>
        <LIBRARY_LAYOUT>
          <!-- a comment -->
          <PAIRED/>
        </LIBRARY_LAYOUT>
      </LIBRARY_DESCRIPTOR>
    </DESIGN>
    <PLATFORM>
      <!-- another comment -->
      <ILLUMINA>
        <INSTRUMENT_MODEL>Illumina NovaSeq 6000</INSTRUMENT_MODEL>
      </ILLUMINA>
    </PLATFORM>
  </EXPERIMENT>
</EXPERIMENT_SET>
"""
        results = parse_experiment(xml_bytes, "DRA000001")
        assert len(results) == 1
        assert results[0]["libraryLayout"] == "PAIRED"
        assert results[0]["platform"] == "ILLUMINA"
        assert results[0]["instrumentModel"] == ["Illumina NovaSeq 6000"]


class TestParseAnalysisType:
    """_parse_analysis_type (§3.3 analysis 専用)."""

    @pytest.mark.parametrize(
        "value",
        ["DE_NOVO_ASSEMBLY", "REFERENCE_ALIGNMENT", "ABUNDANCE_MEASUREMENT", "SEQUENCE_ANNOTATION"],
    )
    def test_all_4_values(self, value: str) -> None:
        analysis = {"ANALYSIS_TYPE": {value: {}}}
        assert _parse_analysis_type(analysis) == value

    def test_unknown_key_falls_back_to_none(self) -> None:
        analysis = {"ANALYSIS_TYPE": {"UNKNOWN_TYPE": {}}}
        assert _parse_analysis_type(analysis) is None

    def test_multiple_keys_fall_back_to_none(self) -> None:
        analysis = {"ANALYSIS_TYPE": {"DE_NOVO_ASSEMBLY": {}, "REFERENCE_ALIGNMENT": {}}}
        assert _parse_analysis_type(analysis) is None

    def test_no_analysis_type_key(self) -> None:
        assert _parse_analysis_type({}) is None
        assert _parse_analysis_type({"ANALYSIS_TYPE": None}) is None

    def test_non_dict_input_returns_none(self) -> None:
        assert _parse_analysis_type(None) is None
        assert _parse_analysis_type([]) is None


class TestParseSubmissionDescription:
    """_parse_submission_description (§4.9.5 submission_comment → description)."""

    def test_typical_non_empty_comment(self) -> None:
        submission = {"submission_comment": "GenomeTrakr pathogen sampling project"}
        assert _parse_submission_description(submission) == "GenomeTrakr pathogen sampling project"

    def test_comment_trimmed(self) -> None:
        assert _parse_submission_description({"submission_comment": "   some comment   "}) == "some comment"

    def test_empty_string_returns_none(self) -> None:
        assert _parse_submission_description({"submission_comment": ""}) is None

    def test_whitespace_only_returns_none(self) -> None:
        assert _parse_submission_description({"submission_comment": "   "}) is None

    def test_missing_comment_returns_none(self) -> None:
        assert _parse_submission_description({"accession": "DRA000001"}) is None

    def test_non_str_returns_none(self) -> None:
        assert _parse_submission_description({"submission_comment": 123}) is None
        assert _parse_submission_description(None) is None

    def test_parse_submission_integration(self) -> None:
        """parse_submission の description に submission_comment が正規化されて入る。"""
        xml = b"""<?xml version="1.0"?>
<SUBMISSION accession="DRA000072" center_name="NIID" submission_date="2010-01-15" submission_comment="  Real comment  ">
  <TITLE>T</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml, "DRA000072")
        assert result is not None
        assert result["description"] == "Real comment"


class TestL1SubmissionPropertiesShape:
    """§4.11 L1 regression: submission JSONL の properties は SUBMISSION 直下
    (他 type は {TYPE}_SET.{TYPE} の 2 階層)。仕様据え置きを固定。"""

    def test_submission_properties_unwraps_to_submission_top_level(self) -> None:
        xml = b"""<?xml version="1.0"?>
<SUBMISSION accession="DRA000072" center_name="NIID" submission_date="2010-01-15">
  <TITLE>Test DRA Submission</TITLE>
</SUBMISSION>
"""
        result = parse_submission(xml, "DRA000072")
        assert result is not None
        props = result["properties"]
        assert "SUBMISSION" in props
        assert "SUBMISSION_SET" not in props

    def test_study_properties_keeps_study_set_wrapper(self) -> None:
        xml = b"""<?xml version="1.0"?>
<STUDY_SET>
  <STUDY accession="DRP000072" center_name="NIID">
    <DESCRIPTOR><STUDY_TITLE>T</STUDY_TITLE></DESCRIPTOR>
  </STUDY>
</STUDY_SET>
"""
        results = parse_study(xml, "DRA000072")
        assert len(results) == 1
        assert "STUDY_SET" in results[0]["properties"]
        assert "STUDY" in results[0]["properties"]["STUDY_SET"]

    def test_analysis_properties_keeps_analysis_set_wrapper(self) -> None:
        xml = b"""<?xml version="1.0"?>
<ANALYSIS_SET>
  <ANALYSIS accession="DRZ000001" center_name="NIID">
    <TITLE>T</TITLE>
    <ANALYSIS_TYPE><DE_NOVO_ASSEMBLY/></ANALYSIS_TYPE>
  </ANALYSIS>
</ANALYSIS_SET>
"""
        results = parse_analysis(xml, "DRA000001")
        assert len(results) == 1
        assert "ANALYSIS_SET" in results[0]["properties"]
        assert results[0]["analysisType"] == "DE_NOVO_ASSEMBLY"
