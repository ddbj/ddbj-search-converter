"""Tests for ddbj_search_converter.jsonl.utils module."""

import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import finalize_relation_db, init_dblink_db
from ddbj_search_converter.jsonl.utils import URL_TEMPLATE, ensure_list_children, get_dbxref_map, to_xref
from ddbj_search_converter.schema import XrefType


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

    @pytest.mark.parametrize(
        ("acc", "expected_type"),
        [
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
        ],
    )
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


class TestEnsureListChildren:
    """Tests for ensure_list_children function."""

    def test_single_dict_wrapped_in_list(self) -> None:
        result = ensure_list_children({"Child": {"key": "val"}})
        assert result == {"Child": [{"key": "val"}]}

    def test_multiple_children_stay_as_list(self) -> None:
        result = ensure_list_children({"Child": [{"a": "1"}, {"a": "2"}]})
        assert result == {"Child": [{"a": "1"}, {"a": "2"}]}

    def test_scalar_values_unchanged(self) -> None:
        result = ensure_list_children({"attr": "value", "id": "123", "count": 42})
        assert result == {"attr": "value", "id": "123", "count": 42}

    def test_none_value_unchanged(self) -> None:
        result = ensure_list_children({"Empty": None})
        assert result == {"Empty": None}

    def test_nested_recursion(self) -> None:
        result = ensure_list_children({"Parent": {"Child": {"GrandChild": {"leaf": "val"}}}})
        assert result == {"Parent": [{"Child": [{"GrandChild": [{"leaf": "val"}]}]}]}

    def test_does_not_mutate_input(self) -> None:
        original = {"Child": {"key": "val"}}
        inner = original["Child"]
        ensure_list_children(original)
        assert original == {"Child": {"key": "val"}}
        assert original["Child"] is inner

    def test_mixed_types_in_list(self) -> None:
        result = ensure_list_children({"Items": [{"id": "1"}, "text", None]})
        assert result == {"Items": [{"id": "1"}, "text", None]}

    def test_empty_dict(self) -> None:
        result = ensure_list_children({})
        assert result == {}

    def test_content_key_stays_scalar(self) -> None:
        """XML 属性付きテキスト要素は content がスカラーのまま。"""
        result = ensure_list_children({"Name": {"abbr": "DDBJ", "content": "DNA Data Bank"}})
        assert result == {"Name": [{"abbr": "DDBJ", "content": "DNA Data Bank"}]}

    def test_dict_in_list_is_recursed(self) -> None:
        result = ensure_list_children(
            {
                "Publications": [
                    {"Title": {"content": "Paper 1"}},
                    {"Title": {"content": "Paper 2"}},
                ]
            }
        )
        assert result == {
            "Publications": [
                {"Title": [{"content": "Paper 1"}]},
                {"Title": [{"content": "Paper 2"}]},
            ]
        }

    def test_realistic_bioproject(self) -> None:
        """BioProject 風の構造で全体テスト。"""
        props = {
            "ProjectDescr": {
                "Title": "Example Project",
                "Description": "A description",
                "Publication": {"id": "12345", "DbType": "ePubmed"},
                "Grant": [
                    {"Agency": {"abbr": "JST", "content": "JST"}, "GrantId": "001"},
                    {"Agency": {"abbr": "AMED", "content": "AMED"}, "GrantId": "002"},
                ],
            },
            "ProjectID": {
                "ArchiveID": {"accession": "PRJDB1", "archive": "DDBJ"},
            },
        }
        result = ensure_list_children(props)

        # ProjectDescr は [dict] にラップ
        assert isinstance(result["ProjectDescr"], list)
        assert len(result["ProjectDescr"]) == 1
        descr = result["ProjectDescr"][0]

        # スカラーはそのまま
        assert descr["Title"] == "Example Project"

        # Publication (単一 dict) は [dict] にラップ
        assert isinstance(descr["Publication"], list)
        assert descr["Publication"][0]["id"] == "12345"

        # Grant (既に list) はそのまま list、中の dict は再帰処理
        assert isinstance(descr["Grant"], list)
        assert len(descr["Grant"]) == 2
        assert isinstance(descr["Grant"][0]["Agency"], list)


class TestGetDbxrefMap:
    """Tests for get_dbxref_map function."""

    def test_returns_xrefs_for_accession(self, test_config: Config) -> None:
        """dblink DB からの関連を Xref リストとして返す。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB100', 'biosample', 'SAMD1')")
        finalize_relation_db(test_config)

        result = get_dbxref_map(test_config, "biosample", ["SAMD1"])

        assert "SAMD1" in result
        xrefs = result["SAMD1"]
        assert len(xrefs) == 1
        assert xrefs[0].type_ == "bioproject"
        assert xrefs[0].identifier == "PRJDB100"

    def test_empty_accessions_returns_empty(self, test_config: Config) -> None:
        """空の accessions で空 dict を返す。"""
        result = get_dbxref_map(test_config, "biosample", [])
        assert result == {}
