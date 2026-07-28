"""duckdb_bulk のテスト。

TSV を経由する以上、区切り文字・quote・改行が値に混ざったときに列がずれる、
空文字列が NULL に化ける、といった失敗が起こり得る。しかもそれらは例外ではなく
「静かに違うデータが入る」形で現れるので、round-trip の完全一致で潰す。
"""

from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.duckdb_bulk import encode_tsv_field, load_tsv_into_table, write_rows_to_tsv
from py_tests.strategies import st_tsv_hostile_text

_ROW_STRATEGY = st.tuples(
    st_tsv_hostile_text(min_size=1),
    st.one_of(st.none(), st_tsv_hostile_text()),
    st.one_of(st.none(), st_tsv_hostile_text()),
)


def _load(conn: duckdb.DuckDBPyConnection, tsv_path: Path, rows: list[tuple[str | None, ...]]) -> int:
    written = write_rows_to_tsv(tsv_path, rows)
    return load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written)


def _new_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t (a TEXT NOT NULL, b TEXT, c TEXT)")
    return conn


class TestEncodeTsvField:
    def test_encode_none_returns_unquoted_empty(self) -> None:
        assert encode_tsv_field(None) == ""

    def test_encode_empty_string_returns_quoted_empty(self) -> None:
        # None と同じ「空」でも quote の有無で区別が付く。
        assert encode_tsv_field("") == '""'

    def test_encode_embedded_quote_is_doubled(self) -> None:
        assert encode_tsv_field('a"b') == '"a""b"'

    @given(value=st_tsv_hostile_text())
    def test_encode_non_none_is_always_quoted(self, value: str) -> None:
        encoded = encode_tsv_field(value)
        assert encoded.startswith('"')
        assert encoded.endswith('"')


class TestRoundTrip:
    @given(rows=st.lists(_ROW_STRATEGY, min_size=0, max_size=30, unique_by=lambda row: row[0]))
    @settings(max_examples=30, deadline=None)
    def test_roundtrip_preserves_rows_exactly(
        self, rows: list[tuple[str | None, ...]], tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        tsv_path = tmp_path_factory.mktemp("bulk") / "rows.tsv"
        with _new_conn() as conn:
            inserted = _load(conn, tsv_path, rows)
            assert inserted == len(rows)
            got = conn.execute("SELECT a, b, c FROM t").fetchall()

        assert sorted(got) == sorted(rows)

    def test_empty_string_and_none_stay_distinct(self, tmp_path: Path) -> None:
        rows: list[tuple[str | None, ...]] = [("EMPTY", "", None), ("NONE", None, "")]
        with _new_conn() as conn:
            _load(conn, tmp_path / "rows.tsv", rows)
            got = {row[0]: (row[1], row[2]) for row in conn.execute("SELECT a, b, c FROM t").fetchall()}

        assert got["EMPTY"] == ("", None)
        assert got["NONE"] == (None, "")

    @pytest.mark.parametrize(
        "value",
        ["a\tb", "a\nb", "a\rb", 'a"b', '"', "\t\n\r", "a\\b"],
        ids=["tab", "lf", "cr", "quote", "only-quote", "all-separators", "backslash"],
    )
    def test_separator_characters_do_not_split_columns(self, value: str, tmp_path: Path) -> None:
        rows: list[tuple[str | None, ...]] = [("ACC", value, None)]
        with _new_conn() as conn:
            _load(conn, tmp_path / "rows.tsv", rows)
            got = conn.execute("SELECT a, b, c FROM t").fetchall()

        assert got == [("ACC", value, None)]

    def test_empty_input_loads_nothing_without_error(self, tmp_path: Path) -> None:
        """更新が 0 件の日は窓ビルドで普通に起きるので、空でも例外にしない。"""
        with _new_conn() as conn:
            assert _load(conn, tmp_path / "rows.tsv", []) == 0
            assert conn.execute("SELECT count(*) FROM t").fetchone() == (0,)

    def test_write_returns_number_of_rows_written(self, tmp_path: Path) -> None:
        written = write_rows_to_tsv(tmp_path / "rows.tsv", [("A", None, None), ("B", "x", "y")])
        assert written == 2

    def test_write_consumes_generator_before_returning(self, tmp_path: Path) -> None:
        """rows を消費し切ってから返ることを、generator の finally で確認する。

        呼び出し側はこの直後に PostgreSQL 接続を閉じる前提で組んでいるので、
        書き出しが遅延評価に変わると成立しなくなる。
        """
        closed: list[str] = []

        def rows() -> object:
            try:
                yield ("A", None, None)
                yield ("B", None, None)
            finally:
                closed.append("done")

        written = write_rows_to_tsv(tmp_path / "rows.tsv", rows())  # type: ignore[arg-type]

        assert written == 2
        assert closed == ["done"]


class TestRowCountMismatch:
    def test_insert_count_mismatch_raises(self, tmp_path: Path) -> None:
        tsv_path = tmp_path / "rows.tsv"
        written = write_rows_to_tsv(tsv_path, [("A", "1", None)])
        with _new_conn() as conn, pytest.raises(RuntimeError, match="row count mismatch"):
            load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written + 1)

    def test_duplicate_key_within_one_load_raises(self, tmp_path: Path) -> None:
        """同じキーが 1 回のロードに 2 行あると、どちらが勝つか曖昧なまま通さない。

        2 行書いて 1 行しか入らないので、件数照合がそれを捕まえる。
        """
        tsv_path = tmp_path / "rows.tsv"
        written = write_rows_to_tsv(tsv_path, [("A", "1", None), ("A", "2", None)])
        with _new_conn() as conn:
            conn.execute("CREATE UNIQUE INDEX idx ON t (a)")
            with pytest.raises(RuntimeError, match="row count mismatch"):
                load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written, conflict_key="a")


class TestConflictKey:
    def test_conflict_key_overwrites_conflicting_row(self, tmp_path: Path) -> None:
        with _new_conn() as conn:
            conn.execute("CREATE UNIQUE INDEX idx ON t (a)")
            conn.execute("INSERT INTO t VALUES ('A', 'old', NULL), ('B', 'keep', NULL)")

            tsv_path = tmp_path / "rows.tsv"
            written = write_rows_to_tsv(tsv_path, [("A", "new", "x")])
            load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written, conflict_key="a")

            got = {row[0]: row[1] for row in conn.execute("SELECT a, b FROM t").fetchall()}

        assert got == {"A": "new", "B": "keep"}

    def test_conflict_key_overwrites_even_with_secondary_index_present(self, tmp_path: Path) -> None:
        """非 UNIQUE index が同居していても既存行が更新されること。

        DuckDB 1.4.4 の `INSERT OR REPLACE` は、この状況で既存行を更新しないまま
        「挿入した」と報告する。件数照合をすり抜けて古い値が残るので、conflict
        target を明示して回避している。その回避が外れたらここで落ちる。
        """
        with _new_conn() as conn:
            conn.execute("CREATE UNIQUE INDEX idx_a ON t (a)")
            conn.execute("CREATE INDEX idx_b ON t (b)")
            conn.execute("INSERT INTO t VALUES ('A', 'old', NULL)")

            tsv_path = tmp_path / "rows.tsv"
            written = write_rows_to_tsv(tsv_path, [("A", "new", "x")])
            load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written, conflict_key="a")

            got = conn.execute("SELECT a, b, c FROM t").fetchall()

        assert got == [("A", "new", "x")]

    def test_conflict_key_inserts_rows_that_do_not_conflict(self, tmp_path: Path) -> None:
        with _new_conn() as conn:
            conn.execute("CREATE UNIQUE INDEX idx ON t (a)")
            conn.execute("INSERT INTO t VALUES ('A', 'old', NULL)")

            tsv_path = tmp_path / "rows.tsv"
            written = write_rows_to_tsv(tsv_path, [("A", "new", None), ("B", "added", None)])
            inserted = load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written, conflict_key="a")

            got = {row[0]: row[1] for row in conn.execute("SELECT a, b FROM t").fetchall()}

        assert inserted == 2
        assert got == {"A": "new", "B": "added"}

    def test_without_conflict_key_conflicting_row_raises(self, tmp_path: Path) -> None:
        with _new_conn() as conn:
            conn.execute("CREATE UNIQUE INDEX idx ON t (a)")
            conn.execute("INSERT INTO t VALUES ('A', 'old', NULL)")

            tsv_path = tmp_path / "rows.tsv"
            written = write_rows_to_tsv(tsv_path, [("A", "new", None)])
            with pytest.raises(duckdb.ConstraintException):
                load_tsv_into_table(conn, "t", ("a", "b", "c"), tsv_path, written)


class TestIdentifierValidation:
    @pytest.mark.parametrize("name", ["t; DROP TABLE t", "1t", "", "t-1", "t name", '"t"'])
    def test_invalid_table_name_raises(self, name: str, tmp_path: Path) -> None:
        tsv_path = tmp_path / "rows.tsv"
        write_rows_to_tsv(tsv_path, [])
        with _new_conn() as conn, pytest.raises(ValueError, match="invalid SQL identifier"):
            load_tsv_into_table(conn, name, ("a",), tsv_path, 0)

    def test_invalid_column_name_raises(self, tmp_path: Path) -> None:
        tsv_path = tmp_path / "rows.tsv"
        write_rows_to_tsv(tsv_path, [])
        with _new_conn() as conn, pytest.raises(ValueError, match="invalid SQL identifier"):
            load_tsv_into_table(conn, "t", ("a; DROP TABLE t",), tsv_path, 0)
