"""Shared fixtures for integration tests.

Connection settings are env-var driven (SSOT below). When a required env var
is unset, dependent tests are skipped automatically.

See ``tests/integration-note.md`` for the integration testing strategy and
``tests/integration-scenarios.md`` for scenario list.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.index import ALL_INDEXES, delete_physical_indexes, make_physical_index_name
from elasticsearch import Elasticsearch

# === Env vars (SSOT) ===

INTEGRATION_ENV_VAR_ES_URL = "DDBJ_SEARCH_INTEGRATION_ES_URL"
INTEGRATION_ENV_VAR_TRAD_POSTGRES_URL = "DDBJ_SEARCH_INTEGRATION_TRAD_POSTGRES_URL"
INTEGRATION_ENV_VAR_XSM_POSTGRES_URL = "DDBJ_SEARCH_INTEGRATION_XSM_POSTGRES_URL"
INTEGRATION_ENV_VAR_ALLOW_DESTRUCTIVE_ALIAS = "DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS"
INTEGRATION_ENV_VAR_DBLINK_DB_PATH = "DDBJ_SEARCH_INTEGRATION_DBLINK_DB_PATH"
INTEGRATION_ENV_VAR_LOG_DB_PATH = "DDBJ_SEARCH_INTEGRATION_LOG_DB_PATH"

# Default paths for the staging host. Override via env vars (above) on
# dev machines so the same suite can run against local fixtures.
_DEFAULT_DBLINK_DB_PATH = "/home/w3ddbjld/const/dblink/dblink.duckdb"
_DEFAULT_LOG_DB_PATH = "/app/ddbj_search_converter_results/log.duckdb"


# === Date suffixes for staging-isolated dated physical indexes ===
#
# 本番運用の dated physical index と衝突させない suffix を 2 種類用意する。
# NEW (REHEARSAL_DATE_SUFFIX) と OLD (REHEARSAL_OLD_DATE_SUFFIX) の 2 世代を
# 区別することで、alias swap (新世代 attach + 旧世代 detach) のテストが書ける。
REHEARSAL_DATE_SUFFIX = "99991231"
REHEARSAL_OLD_DATE_SUFFIX = "99991230"


# === Representative accessions for invariant assertions ===
#
# Some scenarios need a known accession of a specific status / data type.
# These IDs come from ``tests/fixtures/lustre9/.../bp-collab/bioproject/``
# and ``.../bs-collab/biosample/`` livelist files. If the fixture set is
# refreshed via ``scripts/fetch_test_fixtures.sh`` (run on the staging host),
# also update the values here.
#
# Shape: {data_type: {status: accession_id}}
INTEGRATION_REPRESENTATIVE_ACCESSIONS: dict[str, dict[str, str]] = {
    "bioproject": {
        "public": "PRJDB2",
        "suppressed": "PRJDB51",
        "withdrawn": "PRJDB2272",
    },
    "biosample": {
        "public": "SAMD00000001",
        "suppressed": "SAMD00003141",
        "withdrawn": "SAMD00008862",
    },
}


# === ES fixtures ===


@pytest.fixture(scope="session")
def integration_es_url() -> str:
    """Return the ES URL for integration tests, or skip if unset."""
    url = os.environ.get(INTEGRATION_ENV_VAR_ES_URL)
    if not url:
        pytest.skip(
            f"{INTEGRATION_ENV_VAR_ES_URL} is not set; "
            "point it at a non-production ES (staging or local compose) to run integration tests"
        )
    return url


@pytest.fixture(scope="session")
def integration_es_client(integration_es_url: str) -> Iterator[Elasticsearch]:
    """Return a verified ES client. Skip if ping fails."""
    client = Elasticsearch(integration_es_url, request_timeout=30)
    try:
        if not client.ping():
            pytest.skip(f"ES at {integration_es_url} did not respond to ping")
    except Exception as exc:
        pytest.skip(f"Failed to connect to ES at {integration_es_url}: {exc}")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="session")
def integration_config(integration_es_url: str) -> Config:
    """Return a ``Config`` pointing at the integration ES."""
    return Config(es_url=integration_es_url)


@pytest.fixture(scope="session")
def rehearsal_date_suffix() -> str:
    """Date suffix used to isolate dated physical indexes from staging operations."""
    return REHEARSAL_DATE_SUFFIX


@pytest.fixture(scope="session")
def rehearsal_old_date_suffix() -> str:
    """Older date suffix used to represent the previous Blue-Green generation in swap tests."""
    return REHEARSAL_OLD_DATE_SUFFIX


# === PostgreSQL fixtures ===


@pytest.fixture(scope="session")
def integration_xsm_postgres_url() -> str:
    """Return the XSM PostgreSQL URL, or skip if unset."""
    url = os.environ.get(INTEGRATION_ENV_VAR_XSM_POSTGRES_URL)
    if not url:
        pytest.skip(
            f"{INTEGRATION_ENV_VAR_XSM_POSTGRES_URL} is not set; "
            "point it at staging XSM PostgreSQL to run the connectivity smoke"
        )
    return url


@pytest.fixture(scope="session")
def integration_trad_postgres_url() -> str:
    """Return the TRAD PostgreSQL URL, or skip if unset."""
    url = os.environ.get(INTEGRATION_ENV_VAR_TRAD_POSTGRES_URL)
    if not url:
        pytest.skip(
            f"{INTEGRATION_ENV_VAR_TRAD_POSTGRES_URL} is not set; "
            "point it at staging TRAD PostgreSQL to run the connectivity smoke"
        )
    return url


# === Staging seed data fixtures ===
#
# Tests below depend on a populated converter pipeline output (dblink.duckdb,
# ES with the 14 logical indexes filled). Against an empty dev ES / missing
# DuckDB they have no meaning, so skip cleanly.


@pytest.fixture(scope="session")
def integration_dblink_db_path() -> Path:
    """Path to a populated ``dblink.duckdb`` on the integration host.

    Defaults to the staging path but can be overridden with
    ``DDBJ_SEARCH_INTEGRATION_DBLINK_DB_PATH`` for local development
    (e.g. running against a fixture-built DuckDB). Skip if absent.
    """
    raw = os.environ.get(INTEGRATION_ENV_VAR_DBLINK_DB_PATH) or _DEFAULT_DBLINK_DB_PATH
    path = Path(raw)
    if not path.exists():
        pytest.skip(
            f"dblink.duckdb not found at {path}; "
            f"set {INTEGRATION_ENV_VAR_DBLINK_DB_PATH} to override, "
            "or run the converter pipeline to produce it"
        )
    return path


@pytest.fixture(scope="session")
def integration_log_db_path() -> Path:
    """Path to a populated ``log.duckdb``.

    Defaults to the staging path but can be overridden with
    ``DDBJ_SEARCH_INTEGRATION_LOG_DB_PATH`` for local development.
    Skip if absent.

    SSOT: ``lifecycle`` は ``init_log_db`` で ``extra.lifecycle`` から denormalise される
    物理カラム (``logging/db.py`` docstring)。pipeline 実行前の古い log.duckdb には
    ``lifecycle`` 列が存在しないので、fixture で ``init_log_db`` を呼んで migration を
    保証する。``init_log_db`` は ``CREATE TABLE IF NOT EXISTS`` / ``ALTER TABLE ADD COLUMN
    IF NOT EXISTS`` のみで冪等。
    """
    raw = os.environ.get(INTEGRATION_ENV_VAR_LOG_DB_PATH) or _DEFAULT_LOG_DB_PATH
    path = Path(raw)
    if not path.exists():
        pytest.skip(
            f"log.duckdb not found at {path}; "
            f"set {INTEGRATION_ENV_VAR_LOG_DB_PATH} to override, "
            "or run the converter pipeline to produce it"
        )
    import duckdb

    from ddbj_search_converter.logging.db import init_log_db

    init_log_db(Config(result_dir=path.parent))
    with duckdb.connect(str(path), read_only=True) as conn:
        populated = conn.execute(
            "SELECT COUNT(*) FROM log_records WHERE lifecycle IS NOT NULL"
        ).fetchone()[0]
    if populated == 0:
        pytest.skip(
            f"log.duckdb at {path} has no rows with non-NULL lifecycle; "
            "run a pipeline after the lifecycle migration (c395172) so the "
            "physical lifecycle column gets populated by insert_log_records"
        )
    return path


@pytest.fixture(scope="session")
def staging_es_has_seed_data(integration_es_client: Elasticsearch) -> None:
    """Skip if the integration ES does not have seed data (entries alias missing or empty)."""
    try:
        if not integration_es_client.indices.exists_alias(name="entries"):
            pytest.skip("integration ES has no `entries` alias; needs an ES with seed data")
        count = integration_es_client.count(index="entries").body["count"]
        if count == 0:
            pytest.skip("integration ES `entries` alias resolves to 0 docs")
    except Exception as exc:
        pytest.skip(f"integration ES seed check failed: {exc}")


@pytest.fixture(scope="session")
def allow_destructive_alias_tests() -> None:
    """Gate that skips tests which would put_alias on production-shared names.

    The alias swap rehearsal uses ``entries`` / ``sra`` / ``jga`` / per-index
    alias names that are also the production runtime aliases. Running such a
    test against an ES that already has production data attached to those
    aliases would briefly disrupt search. Set
    ``DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS=1`` only against an ES
    that has no production aliases attached (e.g. local compose dev ES).
    """
    if os.environ.get(INTEGRATION_ENV_VAR_ALLOW_DESTRUCTIVE_ALIAS) != "1":
        pytest.skip(
            f"{INTEGRATION_ENV_VAR_ALLOW_DESTRUCTIVE_ALIAS}=1 is required; "
            "this test put_alias on production-shared names "
            "(entries / sra / jga / per-index) and would briefly disrupt "
            "search on staging or production"
        )


@pytest.fixture
def cleanup_rehearsal_indexes(integration_config: Config) -> Iterator[None]:
    """Cleanup of ``*-99991231`` rehearsal indexes around each test.

    Use as an explicit dependency in any test that creates dated indexes with
    :data:`REHEARSAL_DATE_SUFFIX`. Cleanup runs both before (defensive against
    leaks from earlier failures) and after the test body. Uses an explicit
    name list (not ES wildcard delete) since wildcard delete is rejected by
    ES when ``action.destructive_requires_name`` is enabled.
    """
    _delete_rehearsal_indexes(integration_config)
    yield
    _delete_rehearsal_indexes(integration_config)


def _delete_rehearsal_indexes(config: Config) -> None:
    names = [
        make_physical_index_name(idx, suffix)
        for suffix in (REHEARSAL_DATE_SUFFIX, REHEARSAL_OLD_DATE_SUFFIX)
        for idx in ALL_INDEXES
    ]
    delete_physical_indexes(config, names)
