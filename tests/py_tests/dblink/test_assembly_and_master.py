from ddbj_search_converter.dblink.assembly_and_master import (
    normalize_insdc_master_id,
)


class TestNormalizeInsdcMasterId:
    def test_assembly_summary_format(self) -> None:
        # assembly_summary format: version suffix with dot
        assert normalize_insdc_master_id("ABCD12345.1") == "ABCD00000"
        assert normalize_insdc_master_id("XY678910.2") == "XY000000"

    def test_trad_format(self) -> None:
        # TRAD format: suffix with hyphen
        assert normalize_insdc_master_id("ABC123-1") == "ABC000"
        assert normalize_insdc_master_id("WXYZ9876-2") == "WXYZ0000"

    def test_mixed_format(self) -> None:
        # Both dot and hyphen
        assert normalize_insdc_master_id("ABC123.1-1") == "ABC000"

    def test_no_suffix(self) -> None:
        # No suffix
        assert normalize_insdc_master_id("ABCD12345") == "ABCD00000"

    def test_all_zeros(self) -> None:
        # Already all zeros
        assert normalize_insdc_master_id("ABC00000") == "ABC00000"

    def test_single_letter_prefix(self) -> None:
        # Single letter prefix (e.g., A12345)
        assert normalize_insdc_master_id("A12345") == "A00000"

    def test_long_prefix(self) -> None:
        # Long prefix (e.g., ABCDEF12345678)
        assert normalize_insdc_master_id("ABCDEF12345678") == "ABCDEF00000000"
