"""Tests for ddbj_search_converter.jsonl.utils module."""
import pytest

from ddbj_search_converter.jsonl.utils import to_xref


class TestToXref:
    """Tests for to_xref function."""

    def test_biosample_id(self) -> None:
        """BioSample ID を認識する。"""
        xref = to_xref("SAMD00000001")

        assert xref.identifier == "SAMD00000001"
        assert xref.type_ == "biosample"
        assert "biosample" in xref.url

    def test_bioproject_id(self) -> None:
        """BioProject ID を認識する。"""
        xref = to_xref("PRJDB12345")

        assert xref.identifier == "PRJDB12345"
        assert xref.type_ == "bioproject"
        assert "bioproject" in xref.url

    def test_jga_study_id(self) -> None:
        """JGA Study ID を認識する。"""
        xref = to_xref("JGAS000001")

        assert xref.identifier == "JGAS000001"
        assert xref.type_ == "jga-study"
        assert "jga-study" in xref.url

    def test_jga_dataset_id(self) -> None:
        """JGA Dataset ID を認識する。"""
        xref = to_xref("JGAD000001")

        assert xref.identifier == "JGAD000001"
        assert xref.type_ == "jga-dataset"
        assert "jga-dataset" in xref.url

    def test_jga_dac_id(self) -> None:
        """JGA DAC ID を認識する。"""
        xref = to_xref("JGAC000001")

        assert xref.identifier == "JGAC000001"
        assert xref.type_ == "jga-dac"
        assert "jga-dac" in xref.url

    def test_jga_policy_id(self) -> None:
        """JGA Policy ID を認識する。"""
        xref = to_xref("JGAP000001")

        assert xref.identifier == "JGAP000001"
        assert xref.type_ == "jga-policy"
        assert "jga-policy" in xref.url

    def test_sra_study_id(self) -> None:
        """SRA Study ID を認識する。"""
        xref = to_xref("SRP123456")

        assert xref.identifier == "SRP123456"
        assert xref.type_ == "sra-study"

    def test_dra_study_id(self) -> None:
        """DRA Study ID を認識する。"""
        xref = to_xref("DRP123456")

        assert xref.identifier == "DRP123456"
        assert xref.type_ == "sra-study"

    def test_gea_id(self) -> None:
        """GEA ID を認識する。"""
        xref = to_xref("E-GEAD-123")

        assert xref.identifier == "E-GEAD-123"
        assert xref.type_ == "gea"
        assert "E-GEAD-000" in xref.url

    def test_hum_id(self) -> None:
        """hum ID を認識する。"""
        xref = to_xref("hum0001")

        assert xref.identifier == "hum0001"
        assert xref.type_ == "hum-id"
        assert "humandbs.dbcls.jp" in xref.url

    def test_type_hint_overrides_detection(self) -> None:
        """type_hint を指定すると検出をスキップする。"""
        xref = to_xref("12345678", type_hint="pubmed-id")

        assert xref.identifier == "12345678"
        assert xref.type_ == "pubmed-id"
        assert "pubmed.ncbi.nlm.nih.gov" in xref.url

    def test_unknown_id_falls_back_to_taxonomy(self) -> None:
        """不明な ID は taxonomy にフォールバックする。"""
        xref = to_xref("9606")

        assert xref.identifier == "9606"
        assert xref.type_ == "taxonomy"
        assert "Taxonomy" in xref.url

    def test_insdc_assembly_id(self) -> None:
        """INSDC Assembly ID を認識する。"""
        xref = to_xref("GCA_000001405.15")

        assert xref.identifier == "GCA_000001405.15"
        assert xref.type_ == "insdc-assembly"

    def test_metabobank_id(self) -> None:
        """MetaboBank ID を認識する。"""
        xref = to_xref("MTBKS123")

        assert xref.identifier == "MTBKS123"
        assert xref.type_ == "metabobank"
