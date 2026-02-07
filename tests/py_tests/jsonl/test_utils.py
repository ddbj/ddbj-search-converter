"""Tests for ddbj_search_converter.jsonl.utils module."""
import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (finalize_relation_db,
                                             init_dblink_db)
from ddbj_search_converter.jsonl.utils import (URL_TEMPLATE, get_dbxref_map,
                                               to_xref)
from ddbj_search_converter.schema import XrefType

from ..strategies import ALL_ACCESSION_TYPES


class TestToXref:
    """Tests for to_xref function."""

    def test_biosample_id(self) -> None:
        xref = to_xref("SAMD00000001")
        assert xref.identifier == "SAMD00000001"
        assert xref.type_ == "biosample"

    def test_bioproject_id(self) -> None:
        xref = to_xref("PRJDB12345")
        assert xref.identifier == "PRJDB12345"
        assert xref.type_ == "bioproject"

    @pytest.mark.parametrize("acc,expected_type", [
        ("JGAS000001", "jga-study"),
        ("JGAD000001", "jga-dataset"),
        ("JGAC000001", "jga-dac"),
        ("JGAP000001", "jga-policy"),
        ("SRP123456", "sra-study"),
        ("DRP123456", "sra-study"),
        ("SRR123456", "sra-run"),
        ("GCA_000001405.15", "insdc-assembly"),
        ("MTBKS123", "metabobank"),
        ("hum0001", "hum-id"),
    ])
    def test_auto_detection(self, acc: str, expected_type: str) -> None:
        xref = to_xref(acc)
        assert xref.type_ == expected_type

    def test_gea_id_url(self) -> None:
        xref = to_xref("E-GEAD-123")
        assert xref.type_ == "gea"
        assert "E-GEAD-000" in xref.url

    def test_gea_id_url_boundary(self) -> None:
        """GEA ID 999 -> prefix E-GEAD-000, ID 1000 -> prefix E-GEAD-1000."""
        xref_999 = to_xref("E-GEAD-999")
        assert "E-GEAD-000" in xref_999.url

        xref_1000 = to_xref("E-GEAD-1000")
        assert "E-GEAD-1000" in xref_1000.url

    def test_type_hint_overrides_detection(self) -> None:
        xref = to_xref("12345678", type_hint="pubmed-id")
        assert xref.type_ == "pubmed-id"
        assert "pubmed.ncbi.nlm.nih.gov" in xref.url

    def test_unknown_id_falls_back_to_taxonomy(self) -> None:
        xref = to_xref("9606")
        assert xref.type_ == "taxonomy"

    def test_type_hint_gea(self) -> None:
        xref = to_xref("E-GEAD-123", type_hint="gea")
        assert xref.type_ == "gea"
        assert "E-GEAD-000" in xref.url


class TestBug6GeaNonNumericId:
    """Bug #6 (fixed): to_xref GEA の非数値 ID でもクラッシュしない。"""

    def test_non_numeric_gea_id_with_hint(self) -> None:
        xref = to_xref("E-GEAD-abc", type_hint="gea")
        assert xref.type_ == "gea"

    def test_non_numeric_gea_id_without_hint(self) -> None:
        """type_hint なしの場合、パターンマッチで弾かれるので問題ない。"""
        xref = to_xref("E-GEAD-abc")
        assert xref.type_ == "taxonomy"


class TestBug7InvalidTypeHint:
    """Bug #7 (fixed): to_xref 無効な type_hint で ValueError を送出。"""

    def test_invalid_type_hint(self) -> None:
        with pytest.raises(ValueError, match="Unknown type_hint"):
            to_xref("test", type_hint="nonexistent")  # type: ignore[arg-type]


class TestPBT:
    """Property-based tests for to_xref."""

    @given(type_hint=st.sampled_from(list(URL_TEMPLATE.keys())), id_=st.text(min_size=1, max_size=20))
    def test_type_hint_always_returns_matching_type(self, type_hint: XrefType, id_: str) -> None:
        """type_hint 指定時、結果の type が type_hint と一致する。"""
        xref = to_xref(id_, type_hint=type_hint)
        assert xref.type_ == type_hint


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_string_without_hint(self) -> None:
        """空文字列は taxonomy にフォールバック。"""
        xref = to_xref("")
        assert xref.type_ == "taxonomy"

    def test_pubmed_id_vs_taxonomy_ambiguity(self) -> None:
        """数字のみの ID はパターンマッチで taxonomy にフォールバック。"""
        xref = to_xref("12345")
        # pubmed-id and taxonomy share the same pattern, but neither is in priority_types
        assert xref.type_ == "taxonomy"

    def test_gea_id_zero(self) -> None:
        xref = to_xref("E-GEAD-0")
        assert xref.type_ == "gea"
        assert "E-GEAD-000" in xref.url


class TestGetDbxrefMapUmbrella:
    """Tests for get_dbxref_map with umbrella_ids parameter."""

    def test_replaces_bioproject_with_umbrella(self, test_config: Config) -> None:
        """umbrella_ids に含まれる bioproject ID は umbrella-bioproject に変換される。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            # biosample -> bioproject 関連 (bioproject type で格納)
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB999', 'biosample', 'SAMD1')"
            )
        finalize_relation_db(test_config)

        umbrella_ids = {"PRJDB999"}
        result = get_dbxref_map(test_config, "biosample", ["SAMD1"], umbrella_ids=umbrella_ids)

        assert "SAMD1" in result
        xrefs = result["SAMD1"]
        assert len(xrefs) == 1
        assert xrefs[0].type_ == "umbrella-bioproject"
        assert xrefs[0].identifier == "PRJDB999"

    def test_does_not_replace_non_umbrella_bioproject(self, test_config: Config) -> None:
        """umbrella_ids に含まれない bioproject ID はそのまま bioproject のまま。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB100', 'biosample', 'SAMD1')"
            )
        finalize_relation_db(test_config)

        umbrella_ids = {"PRJDB999"}  # PRJDB100 は含まれない
        result = get_dbxref_map(test_config, "biosample", ["SAMD1"], umbrella_ids=umbrella_ids)

        assert "SAMD1" in result
        xrefs = result["SAMD1"]
        assert len(xrefs) == 1
        assert xrefs[0].type_ == "bioproject"

    def test_none_umbrella_ids_keeps_original_type(self, test_config: Config) -> None:
        """umbrella_ids=None の場合は従来通り動作する。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB999', 'biosample', 'SAMD1')"
            )
        finalize_relation_db(test_config)

        # umbrella_ids=None (デフォルト)
        result = get_dbxref_map(test_config, "biosample", ["SAMD1"])

        assert "SAMD1" in result
        xrefs = result["SAMD1"]
        assert len(xrefs) == 1
        assert xrefs[0].type_ == "bioproject"

    def test_mixed_umbrella_and_regular(self, test_config: Config) -> None:
        """umbrella と通常 bioproject が混在する場合、正しく区別される。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB100', 'biosample', 'SAMD1')"
            )
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB999', 'biosample', 'SAMD1')"
            )
        finalize_relation_db(test_config)

        umbrella_ids = {"PRJDB999"}
        result = get_dbxref_map(test_config, "biosample", ["SAMD1"], umbrella_ids=umbrella_ids)

        assert "SAMD1" in result
        xrefs = result["SAMD1"]
        assert len(xrefs) == 2
        types = {x.type_ for x in xrefs}
        assert "bioproject" in types
        assert "umbrella-bioproject" in types
