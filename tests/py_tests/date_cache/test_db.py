"""Tests for ddbj_search_converter.date_cache.db module."""

import string
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.date_cache.db import (
    CHUNK_SIZE,
    date_cache_exists,
    fetch_bp_accessions_modified_since_from_cache,
    fetch_bp_dates_from_cache,
    fetch_bs_accessions_modified_since_from_cache,
    fetch_bs_dates_from_cache,
    finalize_date_cache_db,
    init_date_cache_db,
    insert_bp_dates,
    insert_bs_dates,
)


def _make_config(tmp_path: Path) -> Config:
    return Config(result_dir=tmp_path)


class TestInitCreatesTable:
    def test_init_creates_tables(self, tmp_path: Path) -> None:
        import duckdb

        config = _make_config(tmp_path)
        init_date_cache_db(config)

        db_path = tmp_path / "bp_bs_date.tmp.duckdb"
        assert db_path.exists()

        with duckdb.connect(str(db_path)) as conn:
            tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = {row[0] for row in tables}

        assert "bp_date" in table_names
        assert "bs_date" in table_names


class TestInsertAndFetchBpDates:
    def test_insert_and_fetch_bp_dates(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [
            ("PRJDB1", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"),
            ("PRJDB2", "2026-02-01T00:00:00Z", None, "2026-02-03T00:00:00Z"),
            ("PRJDB3", None, None, None),
        ]
        count = insert_bp_dates(config, rows)
        assert count == 3

        finalize_date_cache_db(config)

        result = fetch_bp_dates_from_cache(config, ["PRJDB1", "PRJDB2", "PRJDB3"])
        assert result == {
            "PRJDB1": ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"),
            "PRJDB2": ("2026-02-01T00:00:00Z", None, "2026-02-03T00:00:00Z"),
            "PRJDB3": (None, None, None),
        }

    def test_insert_empty_list(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        count = insert_bp_dates(config, [])
        assert count == 0


class TestInsertAndFetchBsDates:
    def test_insert_and_fetch_bs_dates(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [
            ("SAMD00000001", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"),
            ("SAMD00000002", None, "2026-02-02T00:00:00Z", None),
        ]
        count = insert_bs_dates(config, rows)
        assert count == 2

        finalize_date_cache_db(config)

        result = fetch_bs_dates_from_cache(config, ["SAMD00000001", "SAMD00000002"])
        assert result == {
            "SAMD00000001": ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z"),
            "SAMD00000002": (None, "2026-02-02T00:00:00Z", None),
        }


class TestFetchEmptyAccessions:
    def test_fetch_empty_accessions_returns_empty_dict(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)
        finalize_date_cache_db(config)

        assert fetch_bp_dates_from_cache(config, []) == {}
        assert fetch_bs_dates_from_cache(config, []) == {}


class TestFetchMissingAccessions:
    def test_fetch_missing_accessions_returns_empty_dict(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)
        finalize_date_cache_db(config)

        assert fetch_bp_dates_from_cache(config, ["PRJDB_NONEXISTENT"]) == {}
        assert fetch_bs_dates_from_cache(config, ["SAMD_NONEXISTENT"]) == {}


class TestDateCacheExists:
    def test_date_cache_exists_false_when_no_db(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        assert not date_cache_exists(config)

    def test_date_cache_exists_true_after_finalize(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)
        finalize_date_cache_db(config)
        assert date_cache_exists(config)


class TestFetchModifiedSince:
    def test_bp_modified_since(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [
            ("PRJDB1", None, "2026-01-01T00:00:00Z", None),
            ("PRJDB2", None, "2026-01-15T00:00:00Z", None),
            ("PRJDB3", None, "2026-02-01T00:00:00Z", None),
            ("PRJDB4", None, None, None),
        ]
        insert_bp_dates(config, rows)
        finalize_date_cache_db(config)

        result = fetch_bp_accessions_modified_since_from_cache(config, "2026-01-15T00:00:00Z")
        assert result == {"PRJDB2", "PRJDB3"}

    def test_bs_modified_since(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [
            ("SAMD00000001", None, "2026-01-01T00:00:00Z", None),
            ("SAMD00000002", None, "2026-03-01T00:00:00Z", None),
        ]
        insert_bs_dates(config, rows)
        finalize_date_cache_db(config)

        result = fetch_bs_accessions_modified_since_from_cache(config, "2026-02-01T00:00:00Z")
        assert result == {"SAMD00000002"}

    def test_modified_since_returns_empty_when_none_match(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [("PRJDB1", None, "2026-01-01T00:00:00Z", None)]
        insert_bp_dates(config, rows)
        finalize_date_cache_db(config)

        result = fetch_bp_accessions_modified_since_from_cache(config, "2099-01-01T00:00:00Z")
        assert result == set()


class TestChunkBoundary:
    def test_chunk_boundary_below(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [(f"PRJDB{i}", "2026-01-01T00:00:00Z", None, None) for i in range(CHUNK_SIZE - 1)]
        count = insert_bp_dates(config, rows)
        assert count == CHUNK_SIZE - 1

        finalize_date_cache_db(config)

        accessions = [f"PRJDB{i}" for i in range(CHUNK_SIZE - 1)]
        result = fetch_bp_dates_from_cache(config, accessions)
        assert len(result) == CHUNK_SIZE - 1

    def test_chunk_boundary_exact(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [(f"PRJDB{i}", "2026-01-01T00:00:00Z", None, None) for i in range(CHUNK_SIZE)]
        count = insert_bp_dates(config, rows)
        assert count == CHUNK_SIZE

        finalize_date_cache_db(config)

        accessions = [f"PRJDB{i}" for i in range(CHUNK_SIZE)]
        result = fetch_bp_dates_from_cache(config, accessions)
        assert len(result) == CHUNK_SIZE

    def test_chunk_boundary_above(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        init_date_cache_db(config)

        rows = [(f"PRJDB{i}", "2026-01-01T00:00:00Z", None, None) for i in range(CHUNK_SIZE + 1)]
        count = insert_bp_dates(config, rows)
        assert count == CHUNK_SIZE + 1

        finalize_date_cache_db(config)

        accessions = [f"PRJDB{i}" for i in range(CHUNK_SIZE + 1)]
        result = fetch_bp_dates_from_cache(config, accessions)
        assert len(result) == CHUNK_SIZE + 1


accession_strategy = st.text(
    alphabet=string.ascii_uppercase + string.digits,
    min_size=1,
    max_size=20,
)
_date_strategy = st.one_of(
    st.none(),
    st.from_regex(r"20[0-9]{2}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z", fullmatch=True),
)

date_row_strategy = st.tuples(accession_strategy, _date_strategy, _date_strategy, _date_strategy)


class TestPbtRoundTrip:
    @given(
        st.lists(
            date_row_strategy,
            min_size=0,
            max_size=50,
            unique_by=lambda x: x[0],
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_bp_insert_fetch_roundtrip(
        self,
        tmp_path_factory: "pytest.TempPathFactory",
        rows: list[tuple[str, str | None, str | None, str | None]],
    ) -> None:
        tmp_path = tmp_path_factory.mktemp("pbt")
        config = _make_config(tmp_path)
        init_date_cache_db(config)
        insert_bp_dates(config, rows)
        finalize_date_cache_db(config)

        accessions = [r[0] for r in rows]
        result = fetch_bp_dates_from_cache(config, accessions)

        expected = {r[0]: (r[1], r[2], r[3]) for r in rows}
        assert result == expected

    @given(
        st.lists(
            date_row_strategy,
            min_size=0,
            max_size=50,
            unique_by=lambda x: x[0],
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_bs_insert_fetch_roundtrip(
        self,
        tmp_path_factory: "pytest.TempPathFactory",
        rows: list[tuple[str, str | None, str | None, str | None]],
    ) -> None:
        tmp_path = tmp_path_factory.mktemp("pbt")
        config = _make_config(tmp_path)
        init_date_cache_db(config)
        insert_bs_dates(config, rows)
        finalize_date_cache_db(config)

        accessions = [r[0] for r in rows]
        result = fetch_bs_dates_from_cache(config, accessions)

        expected = {r[0]: (r[1], r[2], r[3]) for r in rows}
        assert result == expected
