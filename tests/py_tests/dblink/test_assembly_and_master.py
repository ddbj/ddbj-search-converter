from ddbj_search_converter.dblink.assembly_and_master import (
    normalize_master_id, strip_version_suffix)


class TestStripVersionSuffix:

    def test_with_version_suffix(self) -> None:
        # Standard version suffix
        assert strip_version_suffix("GCA_000001215.4") == "GCA_000001215"
        assert strip_version_suffix("GCF_000001405.40") == "GCF_000001405"

    def test_multiple_dots(self) -> None:
        # Only first dot is used
        assert strip_version_suffix("ABC.1.2") == "ABC"

    def test_no_suffix(self) -> None:
        # No suffix should return as-is
        assert strip_version_suffix("GCA_000001215") == "GCA_000001215"
        assert strip_version_suffix("na") == "na"


class TestNormalizeMasterId:

    def test_assembly_summary_format(self) -> None:
        # assembly_summary format: version suffix, digits normalized
        assert normalize_master_id("AABU00000000.1") == "AABU00000000"
        assert normalize_master_id("CP035466.1") == "CP000000"

    def test_trad_format(self) -> None:
        # TRAD format: hyphen suffix, digits normalized
        assert normalize_master_id("BAAA01000001-1") == "BAAA00000000"
        assert normalize_master_id("ABCD12345-2") == "ABCD00000"

    def test_no_suffix(self) -> None:
        # No suffix, but digits still normalized
        assert normalize_master_id("ABC12345") == "ABC00000"

    def test_already_zeros(self) -> None:
        # Already all zeros
        assert normalize_master_id("ABC00000-1") == "ABC00000"
        assert normalize_master_id("ABC00000.1") == "ABC00000"
        assert normalize_master_id("ABC00000") == "ABC00000"

    def test_single_letter_prefix(self) -> None:
        # Single letter prefix
        assert normalize_master_id("A12345-1") == "A00000"
        assert normalize_master_id("A12345.1") == "A00000"

    def test_long_prefix(self) -> None:
        # Long prefix
        assert normalize_master_id("ABCDEF12345678-1") == "ABCDEF00000000"
        assert normalize_master_id("ABCDEF12345678.1") == "ABCDEF00000000"

    def test_na_value(self) -> None:
        # "na" value from assembly_summary
        assert normalize_master_id("na") == "na"

    def test_multiple_suffixes(self) -> None:
        # Both dot and hyphen (dot first)
        assert normalize_master_id("ABC123.1-2") == "ABC000"
        # Hyphen first, then dot
        assert normalize_master_id("ABC123-1.2") == "ABC000"
