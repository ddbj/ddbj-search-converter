"""Tests for ddbj_search_converter.id_patterns module."""
import pytest

from ddbj_search_converter.id_patterns import ID_PATTERN_MAP, is_valid_accession


class TestIsValidAccession:
    """Tests for is_valid_accession function."""

    # --- biosample ---

    @pytest.mark.parametrize("acc", [
        "SAMN12345678",
        "SAMD00000001",
        "SAME12345678",
        "SAMEA12345678",
    ])
    def test_biosample_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "biosample") is True

    @pytest.mark.parametrize("acc", [
        "SAM",
        "12345",
        "PRJDB12345",
        "SAMX12345678",
    ])
    def test_biosample_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "biosample") is False

    # --- bioproject ---

    @pytest.mark.parametrize("acc", [
        "PRJDB12345",
        "PRJNA123456",
        "PRJEB99999",
    ])
    def test_bioproject_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "bioproject") is True

    @pytest.mark.parametrize("acc", [
        "PRJ",
        "PRJD12345",
        "12345",
        "SAMN12345678",
    ])
    def test_bioproject_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "bioproject") is False

    # --- umbrella-bioproject ---

    @pytest.mark.parametrize("acc", [
        "PRJDB12345",
        "PRJNA123456",
    ])
    def test_umbrella_bioproject_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "umbrella-bioproject") is True

    # --- sra-submission ---

    @pytest.mark.parametrize("acc", [
        "SRA123456",
        "DRA000001",
        "ERA999999",
    ])
    def test_sra_submission_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-submission") is True

    @pytest.mark.parametrize("acc", [
        "XRA123456",
        "SRP123456",
    ])
    def test_sra_submission_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-submission") is False

    # --- sra-study ---

    @pytest.mark.parametrize("acc", [
        "SRP123456",
        "DRP000001",
        "ERP999999",
    ])
    def test_sra_study_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-study") is True

    @pytest.mark.parametrize("acc", [
        "XRP123456",
        "SRA123456",
    ])
    def test_sra_study_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-study") is False

    # --- sra-experiment ---

    @pytest.mark.parametrize("acc", [
        "SRX123456",
        "DRX000001",
        "ERX999999",
    ])
    def test_sra_experiment_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-experiment") is True

    @pytest.mark.parametrize("acc", [
        "XRX123456",
        "SRP123456",
    ])
    def test_sra_experiment_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-experiment") is False

    # --- sra-run ---

    @pytest.mark.parametrize("acc", [
        "SRR123456",
        "DRR000001",
        "ERR999999",
    ])
    def test_sra_run_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-run") is True

    @pytest.mark.parametrize("acc", [
        "XRR123456",
        "SRP123456",
    ])
    def test_sra_run_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-run") is False

    # --- sra-sample ---

    @pytest.mark.parametrize("acc", [
        "SRS123456",
        "DRS000001",
        "ERS999999",
    ])
    def test_sra_sample_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-sample") is True

    @pytest.mark.parametrize("acc", [
        "XRS123456",
        "SRP123456",
    ])
    def test_sra_sample_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-sample") is False

    # --- sra-analysis ---

    @pytest.mark.parametrize("acc", [
        "SRZ123456",
        "DRZ000001",
        "ERZ999999",
    ])
    def test_sra_analysis_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-analysis") is True

    @pytest.mark.parametrize("acc", [
        "XRZ123456",
        "SRP123456",
    ])
    def test_sra_analysis_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "sra-analysis") is False

    # --- jga-study ---

    @pytest.mark.parametrize("acc", [
        "JGAS000001",
        "JGAS123456",
    ])
    def test_jga_study_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-study") is True

    @pytest.mark.parametrize("acc", [
        "JGAD000001",
        "JGAS",
        "jgas000001",
    ])
    def test_jga_study_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-study") is False

    # --- jga-dataset ---

    @pytest.mark.parametrize("acc", [
        "JGAD000001",
        "JGAD123456",
    ])
    def test_jga_dataset_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-dataset") is True

    @pytest.mark.parametrize("acc", [
        "JGAS000001",
        "JGAD",
    ])
    def test_jga_dataset_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-dataset") is False

    # --- jga-dac ---

    @pytest.mark.parametrize("acc", [
        "JGAC000001",
    ])
    def test_jga_dac_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-dac") is True

    @pytest.mark.parametrize("acc", [
        "JGAS000001",
        "JGAC",
    ])
    def test_jga_dac_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-dac") is False

    # --- jga-policy ---

    @pytest.mark.parametrize("acc", [
        "JGAP000001",
    ])
    def test_jga_policy_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-policy") is True

    @pytest.mark.parametrize("acc", [
        "JGAS000001",
        "JGAP",
    ])
    def test_jga_policy_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "jga-policy") is False

    # --- gea ---

    @pytest.mark.parametrize("acc", [
        "E-GEAD-123",
        "E-GEAD-1",
        "E-GEAD-99999",
    ])
    def test_gea_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "gea") is True

    @pytest.mark.parametrize("acc", [
        "EGEAD123",
        "E-GEAD-",
        "E-GEAD-abc",
    ])
    def test_gea_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "gea") is False

    # --- geo ---

    @pytest.mark.parametrize("acc", [
        "GSE12345",
        "GSE1",
    ])
    def test_geo_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "geo") is True

    @pytest.mark.parametrize("acc", [
        "GSE",
        "GDS12345",
    ])
    def test_geo_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "geo") is False

    # --- insdc-assembly ---

    @pytest.mark.parametrize("acc", [
        "GCA_000001405.15",
        "GCA_000000000",
    ])
    def test_insdc_assembly_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-assembly") is True

    @pytest.mark.parametrize("acc", [
        "GCF_000001405.15",
        "GCA_12345",
        "GCA000001405",
    ])
    def test_insdc_assembly_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-assembly") is False

    # --- insdc-master ---

    @pytest.mark.parametrize("acc", [
        "A00000",       # 1 letter + 5 zeros
        "AB000000",     # 2 letters + 6 zeros
        "ABCD00000000", # 4 letters + 8 zeros
        "BAA00000",     # 3 letters (B-J prefix) + 5 zeros
    ])
    def test_insdc_master_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-master") is True

    @pytest.mark.parametrize("acc", [
        "AB123456",
        "A12345",
        "ABCD12345678",
    ])
    def test_insdc_master_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-master") is False

    # --- metabobank ---

    @pytest.mark.parametrize("acc", [
        "MTBKS123",
        "MTBKS1",
    ])
    def test_metabobank_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "metabobank") is True

    @pytest.mark.parametrize("acc", [
        "XMTB123",
        "mtbks123",
    ])
    def test_metabobank_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "metabobank") is False

    # --- hum-id ---

    @pytest.mark.parametrize("acc", [
        "hum0001",
        "hum123456",
    ])
    def test_hum_id_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "hum-id") is True

    @pytest.mark.parametrize("acc", [
        "HUM0001",
        "hum",
        "humABC",
    ])
    def test_hum_id_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "hum-id") is False

    # --- pubmed-id ---

    @pytest.mark.parametrize("acc", [
        "12345678",
        "1",
    ])
    def test_pubmed_id_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "pubmed-id") is True

    @pytest.mark.parametrize("acc", [
        "abc",
        "12345abc",
    ])
    def test_pubmed_id_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "pubmed-id") is False

    # --- taxonomy ---

    @pytest.mark.parametrize("acc", [
        "9606",
        "1",
    ])
    def test_taxonomy_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "taxonomy") is True

    @pytest.mark.parametrize("acc", [
        "abc",
    ])
    def test_taxonomy_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "taxonomy") is False


class TestIdPatternMap:
    """Tests for ID_PATTERN_MAP completeness."""

    def test_all_accession_types_covered(self) -> None:
        """ID_PATTERN_MAP が全 AccessionType をカバーしている（taxonomy 以外の主要な型）。"""
        expected_types = {
            "biosample", "bioproject", "umbrella-bioproject",
            "sra-submission", "sra-study", "sra-experiment",
            "sra-run", "sra-sample", "sra-analysis",
            "jga-study", "jga-dataset", "jga-dac", "jga-policy",
            "gea", "geo",
            "insdc-assembly", "insdc-master",
            "metabobank", "hum-id", "pubmed-id", "taxonomy",
        }
        assert set(ID_PATTERN_MAP.keys()) == expected_types

    def test_unknown_type_returns_true(self) -> None:
        """パターンが定義されていない型は常に True を返す。"""
        # AccessionType にはないが、型チェックを無視してテスト
        assert is_valid_accession("anything", "nonexistent-type") is True  # type: ignore[arg-type]
