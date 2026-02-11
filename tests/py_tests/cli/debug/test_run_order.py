"""Tests for ddbj_search_converter.cli.debug.run_order module."""

from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.cli.debug.run_order import (
    PIPELINE_ORDER,
    run_name_sort_key,
    sort_run_names,
)


class TestPipelineOrder:
    """PIPELINE_ORDER 定義自体のバリデーション。"""

    def test_no_duplicates(self) -> None:
        assert len(PIPELINE_ORDER) == len(set(PIPELINE_ORDER))

    def test_not_empty(self) -> None:
        assert len(PIPELINE_ORDER) > 0


class TestSortRunNames:
    """sort_run_names のテスト。"""

    def test_pipeline_only(self) -> None:
        """Pipeline に含まれる名前だけならパイプライン順に並ぶ。"""
        names = [
            "generate_bp_jsonl",
            "check_external_resources",
            "init_dblink_db",
        ]
        result = sort_run_names(names)
        assert result == [
            "check_external_resources",
            "init_dblink_db",
            "generate_bp_jsonl",
        ]

    def test_unknown_names_sorted_alphabetically_at_end(self) -> None:
        """Pipeline にない名前はアルファベット順で末尾に。"""
        names = ["zzz_unknown", "aaa_unknown", "check_external_resources"]
        result = sort_run_names(names)
        assert result == [
            "check_external_resources",
            "aaa_unknown",
            "zzz_unknown",
        ]

    def test_empty_list(self) -> None:
        assert sort_run_names([]) == []

    def test_single_element(self) -> None:
        assert sort_run_names(["check_external_resources"]) == ["check_external_resources"]

    def test_full_pipeline_order_preserved(self) -> None:
        """PIPELINE_ORDER をシャッフルしてもソート後は元の順序に戻る。"""
        import random

        shuffled = list(PIPELINE_ORDER)
        random.shuffle(shuffled)
        assert sort_run_names(shuffled) == list(PIPELINE_ORDER)

    def test_mixed_known_and_unknown(self) -> None:
        """既知と未知の混在。"""
        names = [
            "adhoc",
            "es_bulk_insert",
            "check_external_resources",
            "my_custom_tool",
        ]
        result = sort_run_names(names)
        assert result == [
            "check_external_resources",
            "es_bulk_insert",
            "adhoc",
            "my_custom_tool",
        ]

    @given(st.lists(st.text(min_size=1, max_size=50), max_size=20))
    def test_idempotent(self, names: list[str]) -> None:
        """2回ソートしても結果が同じ。"""
        once = sort_run_names(names)
        twice = sort_run_names(once)
        assert once == twice

    @given(st.lists(st.text(min_size=1, max_size=50), max_size=20))
    def test_output_is_permutation(self, names: list[str]) -> None:
        """ソート結果は入力の並び替えである。"""
        result = sort_run_names(names)
        assert sorted(result) == sorted(names)


class TestRunNameSortKey:
    """run_name_sort_key のテスト。"""

    def test_known_name_has_lower_priority_than_unknown(self) -> None:
        known = run_name_sort_key("check_external_resources")
        unknown = run_name_sort_key("zzz_unknown")
        assert known < unknown

    def test_pipeline_order_is_respected(self) -> None:
        key_check = run_name_sort_key("check_external_resources")
        key_init = run_name_sort_key("init_dblink_db")
        key_gen = run_name_sort_key("generate_bp_jsonl")
        assert key_check < key_init < key_gen

    def test_unknown_names_compared_alphabetically(self) -> None:
        key_a = run_name_sort_key("aaa")
        key_z = run_name_sort_key("zzz")
        assert key_a < key_z
