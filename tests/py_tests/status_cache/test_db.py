"""Tests for ddbj_search_converter.status_cache.db module."""

import string

from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.status_cache.db import (
    CHUNK_SIZE,
    fetch_bp_statuses_from_cache,
    fetch_bs_statuses_from_cache,
    finalize_status_cache_db,
    init_status_cache_db,
    insert_bp_statuses,
    insert_bs_statuses,
    status_cache_exists,
)


def _make_config(tmp_path):
    return Config(result_dir=tmp_path)


class TestInitCreatesTable:
    def test_init_creates_tables(self, tmp_path):
        import duckdb

        config = _make_config(tmp_path)
        init_status_cache_db(config)

        db_path = tmp_path.joinpath("bp_bs_status.tmp.duckdb")
        assert db_path.exists()

        with duckdb.connect(str(db_path)) as conn:
            tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = {row[0] for row in tables}

        assert "bp_status" in table_names
        assert "bs_status" in table_names


class TestInsertAndFetchBpStatuses:
    def test_insert_and_fetch_bp_statuses(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)

        rows = [
            ("PRJDB1", "live"),
            ("PRJDB2", "suppressed"),
            ("PRJDB3", "withdrawn"),
        ]
        count = insert_bp_statuses(config, rows)
        assert count == 3

        finalize_status_cache_db(config)

        result = fetch_bp_statuses_from_cache(config, ["PRJDB1", "PRJDB2", "PRJDB3"])
        assert result == {"PRJDB1": "live", "PRJDB2": "suppressed", "PRJDB3": "withdrawn"}


class TestInsertAndFetchBsStatuses:
    def test_insert_and_fetch_bs_statuses(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)

        rows = [
            ("SAMD00000001", "live"),
            ("SAMD00000002", "suppressed"),
        ]
        count = insert_bs_statuses(config, rows)
        assert count == 2

        finalize_status_cache_db(config)

        result = fetch_bs_statuses_from_cache(config, ["SAMD00000001", "SAMD00000002"])
        assert result == {"SAMD00000001": "live", "SAMD00000002": "suppressed"}


class TestFetchEmptyAccessions:
    def test_fetch_empty_accessions_returns_empty_dict(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)
        finalize_status_cache_db(config)

        result = fetch_bp_statuses_from_cache(config, [])
        assert result == {}

        result = fetch_bs_statuses_from_cache(config, [])
        assert result == {}


class TestFetchMissingAccessions:
    def test_fetch_missing_accessions_returns_empty_dict(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)
        finalize_status_cache_db(config)

        result = fetch_bp_statuses_from_cache(config, ["PRJDB_NONEXISTENT"])
        assert result == {}


class TestStatusCacheExists:
    def test_status_cache_exists_false_when_no_db(self, tmp_path):
        config = _make_config(tmp_path)
        assert not status_cache_exists(config)

    def test_status_cache_exists_true_after_finalize(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)
        finalize_status_cache_db(config)
        assert status_cache_exists(config)


class TestChunkBoundary:
    def test_chunk_boundary_below(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)

        rows = [(f"PRJDB{i}", "live") for i in range(CHUNK_SIZE - 1)]
        count = insert_bp_statuses(config, rows)
        assert count == CHUNK_SIZE - 1

        finalize_status_cache_db(config)

        accessions = [f"PRJDB{i}" for i in range(CHUNK_SIZE - 1)]
        result = fetch_bp_statuses_from_cache(config, accessions)
        assert len(result) == CHUNK_SIZE - 1

    def test_chunk_boundary_exact(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)

        rows = [(f"PRJDB{i}", "live") for i in range(CHUNK_SIZE)]
        count = insert_bp_statuses(config, rows)
        assert count == CHUNK_SIZE

        finalize_status_cache_db(config)

        accessions = [f"PRJDB{i}" for i in range(CHUNK_SIZE)]
        result = fetch_bp_statuses_from_cache(config, accessions)
        assert len(result) == CHUNK_SIZE

    def test_chunk_boundary_above(self, tmp_path):
        config = _make_config(tmp_path)
        init_status_cache_db(config)

        rows = [(f"PRJDB{i}", "live") for i in range(CHUNK_SIZE + 1)]
        count = insert_bp_statuses(config, rows)
        assert count == CHUNK_SIZE + 1

        finalize_status_cache_db(config)

        accessions = [f"PRJDB{i}" for i in range(CHUNK_SIZE + 1)]
        result = fetch_bp_statuses_from_cache(config, accessions)
        assert len(result) == CHUNK_SIZE + 1


accession_strategy = st.text(
    alphabet=string.ascii_uppercase + string.digits,
    min_size=1,
    max_size=20,
)
status_strategy = st.sampled_from(["live", "suppressed", "withdrawn"])


class TestPbtRoundTrip:
    @given(
        st.lists(
            st.tuples(accession_strategy, status_strategy),
            min_size=0,
            max_size=50,
            unique_by=lambda x: x[0],
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_bp_insert_fetch_roundtrip(self, tmp_path_factory, rows):
        tmp_path = tmp_path_factory.mktemp("pbt")
        config = _make_config(tmp_path)
        init_status_cache_db(config)
        insert_bp_statuses(config, rows)
        finalize_status_cache_db(config)

        accessions = [r[0] for r in rows]
        result = fetch_bp_statuses_from_cache(config, accessions)

        expected = dict(rows)
        assert result == expected

    @given(
        st.lists(
            st.tuples(accession_strategy, status_strategy),
            min_size=0,
            max_size=50,
            unique_by=lambda x: x[0],
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_bs_insert_fetch_roundtrip(self, tmp_path_factory, rows):
        tmp_path = tmp_path_factory.mktemp("pbt")
        config = _make_config(tmp_path)
        init_status_cache_db(config)
        insert_bs_statuses(config, rows)
        finalize_status_cache_db(config)

        accessions = [r[0] for r in rows]
        result = fetch_bs_statuses_from_cache(config, accessions)

        expected = dict(rows)
        assert result == expected
