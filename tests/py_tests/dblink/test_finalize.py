"""finalize_dblink_db / finalize_umbrella_db の dbxref_heavy と再実行可能性のテスト。"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink import db as dblink_db
from ddbj_search_converter.dblink.db import (
    build_dbxref_heavy_table,
    build_dbxref_table,
    finalize_dblink_db,
    finalize_umbrella_db,
    init_dblink_db,
    init_umbrella_db,
    save_umbrella_relations,
)
from ddbj_search_converter.logging.logger import run_logger

Row = tuple[str, str, str, str]


def _tmp_db(config: Config) -> Path:
    return config.const_dir / "dblink" / "dblink.tmp.duckdb"


def _final_db(config: Config) -> Path:
    return config.const_dir / "dblink" / "dblink.duckdb"


def _insert_raw_edges(config: Config, rows: list[Row]) -> None:
    with duckdb.connect(str(_tmp_db(config))) as conn:
        if rows:
            conn.executemany("INSERT INTO raw_edges VALUES (?, ?, ?, ?)", rows)


def _tables(path: Path) -> set[str]:
    with duckdb.connect(str(path), read_only=True) as conn:
        return {r[0] for r in conn.execute("SHOW TABLES").fetchall()}


def _read(path: Path, sql: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(path), read_only=True) as conn:
        return conn.execute(sql).fetchall()


def _expected_heavy(rows: list[Row], threshold: int) -> list[tuple[str, str, str, int]]:
    """raw_edges から dbxref_heavy の期待値を DB を使わずに作る。"""
    half: set[Row] = set()
    for a_type, a_id, b_type, b_id in rows:
        half.add((a_type, a_id, b_type, b_id))
        half.add((b_type, b_id, a_type, a_id))
    totals: dict[tuple[str, str], int] = {}
    groups: dict[tuple[str, str, str], int] = {}
    for a_type, a_id, b_type, _ in half:
        totals[(a_type, a_id)] = totals.get((a_type, a_id), 0) + 1
        groups[(a_type, a_id, b_type)] = groups.get((a_type, a_id, b_type), 0) + 1
    return sorted((t, a, lt, n) for (t, a, lt), n in groups.items() if totals[(t, a)] > threshold)


class _FailingConn:
    """``execute`` に渡された SQL が *trigger* を含んだら例外を投げる接続の代理。"""

    def __init__(self, conn: duckdb.DuckDBPyConnection, trigger: str) -> None:
        self._conn = conn
        self._trigger = trigger

    def execute(self, sql: str, *args: Any) -> Any:
        if self._trigger in sql:
            msg = f"injected failure at: {self._trigger}"
            raise RuntimeError(msg)
        return self._conn.execute(sql, *args)

    def __enter__(self) -> _FailingConn:
        return self

    def __exit__(self, *exc: object) -> None:
        self._conn.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _fail_tmp_conn_at(monkeypatch: pytest.MonkeyPatch, trigger: str) -> None:
    original: Callable[[Config], duckdb.DuckDBPyConnection] = dblink_db._connect_tmp_db
    monkeypatch.setattr(dblink_db, "_connect_tmp_db", lambda config: _FailingConn(original(config), trigger))


SAMPLE_ROWS: list[Row] = [
    *[("bioproject", "PRJDB1", "biosample", f"SAMD{i:03d}") for i in range(5)],
    ("bioproject", "PRJDB1", "sra-study", "DRP001"),
    ("bioproject", "PRJDB2", "biosample", "SAMD900"),
]


class TestBuildDbxrefHeavyTable:
    def test_lists_only_accessions_over_the_threshold(
        self, test_config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dblink_db, "DBXREF_HEAVY_THRESHOLD", 5)
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        build_dbxref_table(test_config)

        build_dbxref_heavy_table(test_config)

        # PRJDB1 は 6 行 (> 5)。PRJDB2 は 1 行、各 SAMD は 1〜2 行
        assert _read(_tmp_db(test_config), "SELECT * FROM dbxref_heavy") == [
            ("bioproject", "PRJDB1", "biosample", 5),
            ("bioproject", "PRJDB1", "sra-study", 1),
        ]

    def test_threshold_is_exclusive(self, test_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(dblink_db, "DBXREF_HEAVY_THRESHOLD", 6)
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        build_dbxref_table(test_config)

        build_dbxref_heavy_table(test_config)

        assert _read(_tmp_db(test_config), "SELECT * FROM dbxref_heavy") == []

    def test_rebuild_replaces_the_table(self, test_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        build_dbxref_table(test_config)
        monkeypatch.setattr(dblink_db, "DBXREF_HEAVY_THRESHOLD", 1)
        build_dbxref_heavy_table(test_config)

        monkeypatch.setattr(dblink_db, "DBXREF_HEAVY_THRESHOLD", 5)
        build_dbxref_heavy_table(test_config)

        assert _read(_tmp_db(test_config), "SELECT accession FROM dbxref_heavy GROUP BY 1") == [("PRJDB1",)]

    @settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        rows=st.lists(
            st.tuples(
                st.sampled_from(["bioproject", "biosample", "sra-run"]),
                st.sampled_from(["A", "B", "C"]),
                st.sampled_from(["bioproject", "biosample", "sra-study"]),
                st.integers(min_value=0, max_value=9).map(str),
            ),
            max_size=40,
        ),
        threshold=st.integers(min_value=0, max_value=6),
    )
    def test_matches_reference(self, rows: list[Row], threshold: int, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(dblink_db, "DBXREF_HEAVY_THRESHOLD", threshold)
        with tempfile.TemporaryDirectory() as tmp:
            config = Config(result_dir=Path(tmp), const_dir=Path(tmp) / "const")
            init_dblink_db(config)
            _insert_raw_edges(config, rows)
            build_dbxref_table(config)
            build_dbxref_heavy_table(config)

            heavy = _read(_tmp_db(config), "SELECT * FROM dbxref_heavy ORDER BY ALL")

        assert heavy == _expected_heavy(rows, threshold)


class TestFinalizeDblinkDbIsResumable:
    def test_final_db_has_dbxref_and_heavy_but_no_raw_edges_or_index(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)

        finalize_dblink_db(test_config)

        assert not _tmp_db(test_config).exists()
        assert _tables(_final_db(test_config)) == {"dbxref", "dbxref_heavy"}
        assert _read(_final_db(test_config), "SELECT count(*) FROM duckdb_indexes()") == [(0,)]

    def test_failure_in_build_rolls_back_to_raw_edges(
        self, test_config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dbxref の構築後、raw_edges の DROP で落ちても tmp DB は構築前に戻る。"""
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        _fail_tmp_conn_at(monkeypatch, "DROP TABLE raw_edges")

        with pytest.raises(RuntimeError, match="build_dbxref_table"):
            finalize_dblink_db(test_config)

        assert _tables(_tmp_db(test_config)) == {"raw_edges"}
        assert not _final_db(test_config).exists()

    def test_rerun_after_build_failure_completes(self, test_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        with monkeypatch.context() as m:
            _fail_tmp_conn_at(m, "DROP TABLE raw_edges")
            with pytest.raises(RuntimeError):
                finalize_dblink_db(test_config)

        finalize_dblink_db(test_config)

        assert _read(_final_db(test_config), "SELECT count(*) FROM dbxref") == [(len(SAMPLE_ROWS) * 2,)]

    def test_rerun_after_heavy_failure_skips_the_build(
        self, test_config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dbxref の構築後に落ちたら、再実行では raw_edges が無くても dbxref から続ける。"""
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        with monkeypatch.context() as m:
            _fail_tmp_conn_at(m, "dbxref_heavy")
            with pytest.raises(RuntimeError, match="build_dbxref_heavy_table"):
                finalize_dblink_db(test_config)
        assert _tables(_tmp_db(test_config)) == {"dbxref"}

        monkeypatch.setattr(dblink_db, "DBXREF_HEAVY_THRESHOLD", 5)
        with run_logger(config=test_config):
            finalize_dblink_db(test_config)

        assert _tables(_final_db(test_config)) == {"dbxref", "dbxref_heavy"}
        assert _read(_final_db(test_config), "SELECT count(*) FROM dbxref") == [(len(SAMPLE_ROWS) * 2,)]
        assert _read(_final_db(test_config), "SELECT accession FROM dbxref_heavy GROUP BY 1") == [("PRJDB1",)]

    def test_rerun_after_success_is_a_no_op(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        _insert_raw_edges(test_config, SAMPLE_ROWS)
        finalize_dblink_db(test_config)
        before = _read(_final_db(test_config), "SELECT * FROM dbxref")

        with run_logger(config=test_config):
            finalize_dblink_db(test_config)

        assert _read(_final_db(test_config), "SELECT * FROM dbxref") == before

    def test_raises_when_there_is_nothing_to_finalize(self, test_config: Config) -> None:
        with pytest.raises(RuntimeError, match="neither"):
            finalize_dblink_db(test_config)

    def test_raises_when_tmp_db_has_neither_table(self, test_config: Config) -> None:
        _tmp_db(test_config).parent.mkdir(parents=True, exist_ok=True)
        duckdb.connect(str(_tmp_db(test_config))).close()

        with pytest.raises(RuntimeError, match="build_dbxref_table") as exc:
            finalize_dblink_db(test_config)

        assert "neither raw_edges nor dbxref" in str(exc.value.__cause__)


class TestTmpDbConnectionLimits:
    def test_every_tmp_connection_spills_under_result_dir(self, test_config: Config) -> None:
        init_dblink_db(test_config)

        with dblink_db._connect_tmp_db(test_config) as conn:
            row = conn.execute("SELECT current_setting('temp_directory')").fetchone()

        assert row is not None
        assert Path(row[0]).resolve().is_relative_to(test_config.result_dir.resolve())


class TestFinalizeUmbrellaDbIsResumable:
    def test_failure_before_commit_leaves_tmp_db_untouched_and_rerun_completes(
        self, test_config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        relations = {("PRJDB999", "PRJDB100"), ("PRJDB999", "PRJDB200")}
        with run_logger(config=test_config):
            init_umbrella_db(test_config)
            save_umbrella_relations(test_config, relations)
            save_umbrella_relations(test_config, relations)
        tmp_path = test_config.const_dir / "dblink" / "umbrella.tmp.duckdb"
        final_path = test_config.const_dir / "dblink" / "umbrella.duckdb"

        real_connect = duckdb.connect

        def failing_connect(path: str, *args: Any, **kwargs: Any) -> Any:
            return _FailingConn(real_connect(path, *args, **kwargs), "idx_umbrella_child")

        with monkeypatch.context() as m:
            m.setattr(duckdb, "connect", failing_connect)
            with pytest.raises(RuntimeError, match="injected"):
                finalize_umbrella_db(test_config)

        assert _tables(tmp_path) == {"umbrella_relation"}
        assert _read(tmp_path, "SELECT count(*) FROM umbrella_relation") == [(4,)]
        assert not final_path.exists()

        finalize_umbrella_db(test_config)

        assert sorted(_read(final_path, "SELECT parent_accession, child_accession FROM umbrella_relation")) == sorted(
            relations
        )
