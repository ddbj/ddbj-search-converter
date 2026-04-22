"""Tests for ddbj_search_converter.jsonl.utils module."""

import copy
from typing import Any

import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import finalize_relation_db, init_dblink_db
from ddbj_search_converter.jsonl.utils import (
    URL_TEMPLATE,
    deduplicate_organizations,
    ensure_attribute_list,
    get_dbxref_map,
    to_xref,
)
from ddbj_search_converter.schema import Organization, XrefType


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
            ("hum0001", "humandbs"),
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
        xref = to_xref("12345678", type_hint="pubmed")
        assert xref.type_ == "pubmed"
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

    def test_pubmed_vs_taxonomy_ambiguity(self) -> None:
        """数字のみの ID はパターンマッチで taxonomy にフォールバック。"""
        xref = to_xref("12345")
        # pubmed and taxonomy share the same pattern, but neither is in priority_types
        assert xref.type_ == "taxonomy"

    def test_gea_id_zero(self) -> None:
        xref = to_xref("E-GEAD-0")
        assert xref.type_ == "gea"
        assert "E-GEAD-000" in xref.url


class TestEnsureAttributeList:
    """Tests for ensure_attribute_list function."""

    def test_single_dict_wrapped_in_list(self) -> None:
        props = {"Attributes": {"Attribute": {"attribute_name": "geo", "content": "Japan"}}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Attributes": {"Attribute": [{"attribute_name": "geo", "content": "Japan"}]}}

    def test_already_list_unchanged(self) -> None:
        props = {"Attributes": {"Attribute": [{"attribute_name": "a"}, {"attribute_name": "b"}]}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Attributes": {"Attribute": [{"attribute_name": "a"}, {"attribute_name": "b"}]}}

    def test_none_leaf_unchanged(self) -> None:
        props: dict[str, Any] = {"Attributes": {"Attribute": None}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Attributes": {"Attribute": None}}

    def test_missing_intermediate_key_no_op(self) -> None:
        props = {"Other": {"key": "val"}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Other": {"key": "val"}}

    def test_missing_leaf_key_no_op(self) -> None:
        props: dict[str, Any] = {"Attributes": {}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Attributes": {}}

    def test_empty_attribute_paths_no_op(self) -> None:
        props = {"Attributes": {"Attribute": {"k": "v"}}}
        ensure_attribute_list(props, [])
        assert props == {"Attributes": {"Attribute": {"k": "v"}}}

    def test_empty_path_in_paths_list_is_skipped(self) -> None:
        """attribute_paths に空 path が混ざっていても skip され、他の path は正常処理。"""
        props = {"Attributes": {"Attribute": {"k": "v"}}}
        ensure_attribute_list(props, [[], ["Attributes", "Attribute"]])
        assert props == {"Attributes": {"Attribute": [{"k": "v"}]}}

    def test_empty_properties(self) -> None:
        props: dict[str, Any] = {}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {}

    def test_intermediate_list_recurses(self) -> None:
        """途中経路に list がある場合、全要素に対して再帰処理する。"""
        props = {
            "STUDY_SET": {
                "STUDY": [
                    {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": {"TAG": "a"}}},
                    {"STUDY_ATTRIBUTES": {"STUDY_ATTRIBUTE": [{"TAG": "b"}, {"TAG": "c"}]}},
                ]
            }
        }
        path = ["STUDY_SET", "STUDY", "STUDY_ATTRIBUTES", "STUDY_ATTRIBUTE"]
        ensure_attribute_list(props, [path])
        assert props["STUDY_SET"]["STUDY"][0]["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"] == [{"TAG": "a"}]
        assert props["STUDY_SET"]["STUDY"][1]["STUDY_ATTRIBUTES"]["STUDY_ATTRIBUTE"] == [{"TAG": "b"}, {"TAG": "c"}]

    def test_multiple_paths(self) -> None:
        props = {
            "Attributes": {"Attribute": {"a": 1}},
            "Foo": {"FooAttribute": {"b": 2}},
        }
        ensure_attribute_list(
            props,
            [["Attributes", "Attribute"], ["Foo", "FooAttribute"]],
        )
        assert props == {
            "Attributes": {"Attribute": [{"a": 1}]},
            "Foo": {"FooAttribute": [{"b": 2}]},
        }

    def test_intermediate_scalar_no_op(self) -> None:
        """中間経路がスカラーならスキップ（例外にしない）。"""
        props: dict[str, Any] = {"Attributes": "unexpected_scalar"}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Attributes": "unexpected_scalar"}

    def test_scalar_leaf_unchanged(self) -> None:
        """末端がスカラーならラップしない。"""
        props: dict[str, Any] = {"Attributes": {"Attribute": "just_text"}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props == {"Attributes": {"Attribute": "just_text"}}

    def test_mutates_in_place(self) -> None:
        """入力 dict は破壊的に変更される。"""
        props = {"Attributes": {"Attribute": {"k": "v"}}}
        original_id = id(props)
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert id(props) == original_id
        assert props["Attributes"]["Attribute"] == [{"k": "v"}]

    def test_other_fields_preserved(self) -> None:
        """指定パス以外のフィールドは値・構造ともに不変。"""
        props = {
            "Attributes": {"Attribute": {"a": 1}},
            "Description": {"Title": "preserved"},
            "Ids": {"Id": "preserved"},
        }
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props["Description"] == {"Title": "preserved"}
        assert props["Ids"] == {"Id": "preserved"}

    def test_biosample_realistic(self) -> None:
        """BioSample 風の構造で動作確認。"""
        props = {
            "Ids": {"Id": [{"content": "SAMD1", "db": "BioSample"}]},
            "Attributes": {"Attribute": {"attribute_name": "host", "content": "Homo sapiens"}},
            "Description": {"Title": "Sample title"},
        }
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props["Attributes"]["Attribute"] == [{"attribute_name": "host", "content": "Homo sapiens"}]
        assert props["Ids"]["Id"] == [{"content": "SAMD1", "db": "BioSample"}]
        assert props["Description"] == {"Title": "Sample title"}


class TestEnsureAttributeListPBT:
    """Property-based tests for ensure_attribute_list."""

    @given(
        leaf=st.one_of(
            st.none(),
            st.text(max_size=20),
            st.integers(),
            st.dictionaries(st.text(min_size=1, max_size=10), st.integers(), max_size=3),
            st.lists(
                st.dictionaries(st.text(min_size=1, max_size=10), st.integers(), max_size=3),
                max_size=3,
            ),
        )
    )
    def test_idempotent(self, leaf: Any) -> None:
        """2 回適用しても 1 回目と同じ結果（冪等性）。"""
        props_once = {"Attributes": {"Attribute": copy.deepcopy(leaf)}}
        props_twice = {"Attributes": {"Attribute": copy.deepcopy(leaf)}}
        ensure_attribute_list(props_once, [["Attributes", "Attribute"]])
        ensure_attribute_list(props_twice, [["Attributes", "Attribute"]])
        ensure_attribute_list(props_twice, [["Attributes", "Attribute"]])
        assert props_once == props_twice

    @given(
        other_key=st.text(min_size=1, max_size=10).filter(lambda x: x != "Attributes"),
        other_value=st.one_of(
            st.none(),
            st.text(max_size=20),
            st.integers(),
            st.dictionaries(st.text(min_size=1, max_size=10), st.text(max_size=10), max_size=3),
        ),
    )
    def test_other_fields_preserved(self, other_key: str, other_value: Any) -> None:
        """指定パス以外のフィールドは値・構造ともに不変。"""
        expected_other = copy.deepcopy(other_value)
        props: dict[str, Any] = {
            "Attributes": {"Attribute": {"a": "1"}},
            other_key: other_value,
        }
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert props[other_key] == expected_other

    @given(
        leaf=st.one_of(
            st.dictionaries(st.text(min_size=1, max_size=10), st.integers(), max_size=3),
            st.lists(
                st.dictionaries(st.text(min_size=1, max_size=10), st.integers(), max_size=3),
                min_size=0,
                max_size=3,
            ),
        )
    )
    def test_dict_or_list_leaf_always_becomes_list(self, leaf: Any) -> None:
        """対象パス末端が dict または list なら適用後は必ず list。"""
        props = {"Attributes": {"Attribute": copy.deepcopy(leaf)}}
        ensure_attribute_list(props, [["Attributes", "Attribute"]])
        assert isinstance(props["Attributes"]["Attribute"], list)


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


class TestDeduplicateOrganizations:
    """Tests for deduplicate_organizations helper (M3)."""

    def test_duplicate_names_kept_once(self) -> None:
        orgs = [Organization(name="DDBJ"), Organization(name="DDBJ")]
        assert deduplicate_organizations(orgs) == [Organization(name="DDBJ")]

    def test_whitespace_treated_as_duplicate(self) -> None:
        orgs = [Organization(name="DDBJ"), Organization(name=" DDBJ ")]
        assert deduplicate_organizations(orgs) == [Organization(name="DDBJ")]

    def test_case_sensitive_not_deduplicated(self) -> None:
        orgs = [Organization(name="DDBJ"), Organization(name="ddbj")]
        assert deduplicate_organizations(orgs) == [
            Organization(name="DDBJ"),
            Organization(name="ddbj"),
        ]

    def test_order_preserved(self) -> None:
        orgs = [
            Organization(name="A"),
            Organization(name="B"),
            Organization(name="A"),
            Organization(name="C"),
        ]
        result = deduplicate_organizations(orgs)
        assert [o.name for o in result] == ["A", "B", "C"]

    def test_empty_or_none_name_collapse_to_single(self) -> None:
        orgs = [
            Organization(name=None),
            Organization(name=""),
            Organization(name="   "),
        ]
        result = deduplicate_organizations(orgs)
        assert len(result) == 1

    def test_empty_input_returns_empty(self) -> None:
        assert deduplicate_organizations([]) == []

    def test_attributes_preserved_for_first_entry(self) -> None:
        """重複時、最初に出現したエントリの属性が保持される。"""
        orgs = [
            Organization(name="DDBJ", abbreviation="DDBJ", role="owner"),
            Organization(name="DDBJ", abbreviation="OTHER"),
        ]
        result = deduplicate_organizations(orgs)
        assert len(result) == 1
        assert result[0].abbreviation == "DDBJ"
        assert result[0].role == "owner"
