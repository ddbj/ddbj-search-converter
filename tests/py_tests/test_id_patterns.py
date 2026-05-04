"""Tests for ddbj_search_converter.id_patterns module."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.dblink.db import AccessionType
from ddbj_search_converter.id_patterns import ID_PATTERN_MAP, is_ddbj_sra_accession, is_valid_accession

from .strategies import ALL_ACCESSION_TYPES


class TestIsValidAccession:
    """Tests for is_valid_accession function."""

    # --- biosample ---

    @pytest.mark.parametrize(
        "acc",
        [
            "SAMN12345678",
            "SAMD00000001",
            "SAME12345678",
            "SAMEA12345678",
        ],
    )
    def test_biosample_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "biosample") is True

    @pytest.mark.parametrize(
        "acc",
        [
            "SAM",
            "12345",
            "PRJDB12345",
            "SAMX12345678",
        ],
    )
    def test_biosample_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "biosample") is False

    # --- bioproject ---

    @pytest.mark.parametrize(
        "acc",
        [
            "PRJDB12345",
            "PRJNA123456",
            "PRJEB99999",
        ],
    )
    def test_bioproject_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "bioproject") is True

    @pytest.mark.parametrize(
        "acc",
        [
            "PRJ",
            "PRJD12345",
            "12345",
            "SAMN12345678",
        ],
    )
    def test_bioproject_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "bioproject") is False

    # --- sra types ---

    @pytest.mark.parametrize(
        ("acc", "acc_type"),
        [
            ("SRA123456", "sra-submission"),
            ("DRA000001", "sra-submission"),
            ("ERA999999", "sra-submission"),
            ("SRP123456", "sra-study"),
            ("DRP000001", "sra-study"),
            ("SRX123456", "sra-experiment"),
            ("DRX000001", "sra-experiment"),
            ("SRR123456", "sra-run"),
            ("DRR000001", "sra-run"),
            ("SRS123456", "sra-sample"),
            ("DRS000001", "sra-sample"),
            ("SRZ123456", "sra-analysis"),
            ("DRZ000001", "sra-analysis"),
        ],
    )
    def test_sra_types_valid(self, acc: str, acc_type: str) -> None:
        assert is_valid_accession(acc, acc_type) is True  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("acc", "acc_type"),
        [
            ("XRA123456", "sra-submission"),
            ("SRP123456", "sra-submission"),
            ("XRP123456", "sra-study"),
            ("SRA123456", "sra-study"),
        ],
    )
    def test_sra_types_invalid(self, acc: str, acc_type: str) -> None:
        assert is_valid_accession(acc, acc_type) is False  # type: ignore[arg-type]

    # --- jga types ---

    @pytest.mark.parametrize(
        ("acc", "acc_type"),
        [
            ("JGAS000001", "jga-study"),
            ("JGAD000001", "jga-dataset"),
            ("JGAC000001", "jga-dac"),
            ("JGAP000001", "jga-policy"),
        ],
    )
    def test_jga_types_valid(self, acc: str, acc_type: str) -> None:
        assert is_valid_accession(acc, acc_type) is True  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("acc", "acc_type"),
        [
            ("JGAD000001", "jga-study"),
            ("JGAS000001", "jga-dataset"),
            ("JGAS000001", "jga-dac"),
            ("JGAS000001", "jga-policy"),
            ("jgas000001", "jga-study"),
        ],
    )
    def test_jga_types_invalid(self, acc: str, acc_type: str) -> None:
        assert is_valid_accession(acc, acc_type) is False  # type: ignore[arg-type]

    # --- gea ---

    @pytest.mark.parametrize("acc", ["E-GEAD-123", "E-GEAD-1", "E-GEAD-99999"])
    def test_gea_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "gea") is True

    @pytest.mark.parametrize("acc", ["EGEAD123", "E-GEAD-", "E-GEAD-abc"])
    def test_gea_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "gea") is False

    # --- geo ---

    @pytest.mark.parametrize("acc", ["GSE12345", "GSE1"])
    def test_geo_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "geo") is True

    @pytest.mark.parametrize("acc", ["GSE", "GDS12345"])
    def test_geo_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "geo") is False

    # --- insdc-assembly ---

    @pytest.mark.parametrize("acc", ["GCA_000001405.15", "GCA_000000000"])
    def test_insdc_assembly_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-assembly") is True

    @pytest.mark.parametrize("acc", ["GCF_000001405.15", "GCA_12345", "GCA000001405"])
    def test_insdc_assembly_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-assembly") is False

    # --- insdc-master ---

    @pytest.mark.parametrize(
        "acc",
        [
            "A00000",
            "AB000000",
            "ABCD00000000",
            "BAA00000",
        ],
    )
    def test_insdc_master_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-master") is True

    @pytest.mark.parametrize("acc", ["AB123456", "A12345", "ABCD12345678"])
    def test_insdc_master_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "insdc-master") is False

    # --- metabobank ---

    @pytest.mark.parametrize("acc", ["MTBKS123", "MTBKS1"])
    def test_metabobank_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "metabobank") is True

    @pytest.mark.parametrize("acc", ["XMTB123", "mtbks123"])
    def test_metabobank_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "metabobank") is False

    # --- humandbs ---

    @pytest.mark.parametrize("acc", ["hum0001", "hum123456"])
    def test_humandbs_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "humandbs") is True

    @pytest.mark.parametrize("acc", ["HUM0001", "hum", "humABC"])
    def test_humandbs_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "humandbs") is False

    # --- pubmed / taxonomy ---

    @pytest.mark.parametrize("acc", ["12345678", "1"])
    def test_pubmed_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "pubmed") is True

    @pytest.mark.parametrize("acc", ["abc", "12345abc"])
    def test_pubmed_invalid(self, acc: str) -> None:
        assert is_valid_accession(acc, "pubmed") is False

    @pytest.mark.parametrize("acc", ["9606", "1"])
    def test_taxonomy_valid(self, acc: str) -> None:
        assert is_valid_accession(acc, "taxonomy") is True


class TestIdPatternMap:
    """Tests for ID_PATTERN_MAP completeness."""

    # insdc は多様すぎて正規表現で網羅できないため、ID_PATTERN_MAP に含めない
    PATTERN_EXCLUDED_TYPES: set[AccessionType] = {"insdc"}

    def test_all_accession_types_covered(self) -> None:
        expected = set(ALL_ACCESSION_TYPES) - self.PATTERN_EXCLUDED_TYPES
        assert set(ID_PATTERN_MAP.keys()) == expected


class TestBug1UnknownAccessionType:
    """Bug #1 (fixed): 未知の AccessionType で is_valid_accession が False を返す。"""

    def test_unknown_type_should_reject(self) -> None:
        result = is_valid_accession("anything", "nonexistent-type")  # type: ignore[arg-type]
        assert result is False

    def test_unknown_type_rejects_all(self) -> None:
        assert is_valid_accession("", "nonexistent-type") is False  # type: ignore[arg-type]
        assert is_valid_accession("garbage!!!", "nonexistent-type") is False  # type: ignore[arg-type]


class TestPBT:
    """Property-based tests for is_valid_accession."""

    # insdc は ID_PATTERN_MAP にパターンがないため除外
    PATTERN_TYPES = [t for t in ALL_ACCESSION_TYPES if t != "insdc"]

    @given(
        acc_type=st.sampled_from(PATTERN_TYPES),
        text=st.text(max_size=100),
    )
    def test_result_matches_regex(self, acc_type: str, text: str) -> None:
        """is_valid_accession の結果は pattern.match と一致する。"""
        pattern = ID_PATTERN_MAP[acc_type]  # type: ignore[index]
        expected = bool(pattern.match(text))
        assert is_valid_accession(text, acc_type) is expected  # type: ignore[arg-type]

    @given(acc_type=st.sampled_from(PATTERN_TYPES))
    def test_empty_string_is_invalid(self, acc_type: str) -> None:
        """空文字列は（pubmed/taxonomy を除き）常に invalid。"""
        result = is_valid_accession("", acc_type)  # type: ignore[arg-type]
        if acc_type in ("pubmed", "taxonomy"):
            # "^\d+$" requires at least one digit
            assert result is False
        else:
            assert result is False


class TestBug12TrailingNewline:
    """Bug #12 (fixed): 末尾改行 (\\n) が全 AccessionType で invalid と判定される。

    原因: `$` は改行の前にもマッチする → `\\Z` に修正。
    """

    VALID_EXAMPLES: dict[str, str] = {
        "biosample": "SAMN12345678",
        "bioproject": "PRJDB12345",
        "sra-submission": "SRA123456",
        "sra-study": "SRP123456",
        "sra-experiment": "SRX123456",
        "sra-run": "SRR123456",
        "sra-sample": "SRS123456",
        "sra-analysis": "SRZ123456",
        "jga-study": "JGAS000001",
        "jga-dataset": "JGAD000001",
        "jga-dac": "JGAC000001",
        "jga-policy": "JGAP000001",
        "gea": "E-GEAD-123",
        "geo": "GSE12345",
        "insdc-assembly": "GCA_000001405",
        "insdc-master": "AB000000",
        "metabobank": "MTBKS123",
        "humandbs": "hum0001",
        "pubmed": "12345",
        "taxonomy": "9606",
    }

    # insdc は ID_PATTERN_MAP にパターンがないため除外
    PATTERN_TYPES = [t for t in ALL_ACCESSION_TYPES if t != "insdc"]

    @pytest.mark.parametrize("acc_type", PATTERN_TYPES)
    def test_valid_with_trailing_newline_is_rejected(self, acc_type: str) -> None:
        valid_acc = self.VALID_EXAMPLES[acc_type]
        assert is_valid_accession(valid_acc, acc_type) is True  # type: ignore[arg-type]
        assert is_valid_accession(valid_acc + "\n", acc_type) is False  # type: ignore[arg-type]


class TestEdgeCases:
    """Edge case tests for is_valid_accession."""

    # insdc は ID_PATTERN_MAP にパターンがないため除外
    PATTERN_TYPES = [t for t in ALL_ACCESSION_TYPES if t != "insdc"]

    @pytest.mark.parametrize("acc_type", PATTERN_TYPES)
    def test_null_byte_in_accession(self, acc_type: str) -> None:
        """null byte を含む文字列は invalid。"""
        assert is_valid_accession("PRJDB\x001", acc_type) is False  # type: ignore[arg-type]

    @pytest.mark.parametrize("acc_type", [t for t in ALL_ACCESSION_TYPES if t not in ("bioproject", "insdc")])
    def test_newline_in_accession(self, acc_type: str) -> None:
        """改行を含む文字列は invalid。"""
        assert is_valid_accession("PRJDB1\n", acc_type) is False  # type: ignore[arg-type]

    @pytest.mark.parametrize("acc_type", ["bioproject"])
    def test_newline_in_accession_bioproject(self, acc_type: str) -> None:
        """Bug #11 (fixed): 改行を含む bioproject 文字列は invalid。"""
        assert is_valid_accession("PRJDB1\n", acc_type) is False  # type: ignore[arg-type]

    @pytest.mark.parametrize("acc_type", PATTERN_TYPES)
    def test_very_long_string(self, acc_type: str) -> None:
        """超長文字列は invalid (pubmed/taxonomy 以外)。"""
        long_str = "A" * 10000
        result = is_valid_accession(long_str, acc_type)  # type: ignore[arg-type]
        if acc_type in ("pubmed", "taxonomy"):
            assert result is False  # only digits match
        else:
            assert result is False


class TestIsDdbjSraAccession:
    """Tests for is_ddbj_sra_accession function."""

    DDBJ_PREFIXES = ("DRA", "DRR", "DRX", "DRZ", "DRS", "DRP")
    NON_DDBJ_SRA_PREFIXES = (
        "SRA", "SRR", "SRX", "SRZ", "SRS", "SRP",
        "ERA", "ERR", "ERX", "ERZ", "ERS", "ERP",
    )

    @pytest.mark.parametrize("prefix", DDBJ_PREFIXES)
    def test_ddbj_prefix_with_digits_is_true(self, prefix: str) -> None:
        assert is_ddbj_sra_accession(f"{prefix}123456") is True

    @pytest.mark.parametrize("prefix", NON_DDBJ_SRA_PREFIXES)
    def test_non_ddbj_prefix_with_digits_is_false(self, prefix: str) -> None:
        assert is_ddbj_sra_accession(f"{prefix}123456") is False

    @pytest.mark.parametrize(
        "acc",
        [
            "",
            "DR",
            "D",
            "ABC",
            "PRJDB12345",
            "SAMD00000001",
            "JGAS000001",
            "dra000001",
            "drr000001",
        ],
    )
    def test_invalid_or_unrelated_strings_are_false(self, acc: str) -> None:
        assert is_ddbj_sra_accession(acc) is False

    @given(
        prefix=st.sampled_from(list(DDBJ_PREFIXES)),
        digits=st.integers(min_value=0, max_value=10**12).map(str),
    )
    def test_pbt_ddbj_prefix_always_true(self, prefix: str, digits: str) -> None:
        """DDBJ prefix + 任意桁の数字は True。"""
        assert is_ddbj_sra_accession(prefix + digits) is True

    @given(
        prefix=st.sampled_from(list(NON_DDBJ_SRA_PREFIXES)),
        digits=st.integers(min_value=0, max_value=10**12).map(str),
    )
    def test_pbt_non_ddbj_prefix_always_false(self, prefix: str, digits: str) -> None:
        """SRA / ERA 系 prefix + 任意桁の数字は False。"""
        assert is_ddbj_sra_accession(prefix + digits) is False

    @given(
        # DDBJ prefix と全く重ならない頭文字に絞った任意文字列
        text=st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Nd"), whitelist_characters="-_"),
            min_size=1,
            max_size=20,
        ).filter(lambda s: not s.startswith(("DRA", "DRR", "DRX", "DRZ", "DRS", "DRP"))),
    )
    def test_pbt_random_non_ddbj_text_is_false(self, text: str) -> None:
        """DDBJ prefix で始まらない任意文字列は False。"""
        assert is_ddbj_sra_accession(text) is False
