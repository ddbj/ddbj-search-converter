"""Shared fixtures for integration tests.

Connection settings are env-var driven (SSOT below). When a required env var
is unset, dependent tests are skipped automatically.

See ``tests/integration-note.md`` for the integration testing strategy and
``tests/integration-scenarios.md`` for scenario list.
"""

import os
from collections.abc import Iterator

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.index import ALL_INDEXES, delete_physical_indexes, make_physical_index_name
from elasticsearch import Elasticsearch

# === Env vars (SSOT) ===

INTEGRATION_ENV_VAR_ES_URL = "DDBJ_SEARCH_INTEGRATION_ES_URL"
INTEGRATION_ENV_VAR_TRAD_POSTGRES_URL = "DDBJ_SEARCH_INTEGRATION_TRAD_POSTGRES_URL"
INTEGRATION_ENV_VAR_XSM_POSTGRES_URL = "DDBJ_SEARCH_INTEGRATION_XSM_POSTGRES_URL"


# === Date suffixes for staging-isolated dated physical indexes ===
#
# 未来日付 ``99991231`` / ``99991230`` を使うことで、staging の Blue-Green 物理 index
# (実日付 suffix) と衝突しない。teardown 失敗で残骸が出ても人間がすぐに気づける。
#
# OLD は alias swap rehearsal (Phase 2) で旧 dated index を表現するために使う。
REHEARSAL_DATE_SUFFIX = "99991231"
REHEARSAL_OLD_DATE_SUFFIX = "99991230"


# === Representative accessions for invariant assertions ===
#
# Some scenarios need a known accession of a specific status / data type.
# Add representative IDs here as scenarios are implemented; keep them
# in sync with ``scripts/fetch_test_fixtures.sh`` outputs.
#
# Shape: {data_type: {status: accession_id}}
INTEGRATION_REPRESENTATIVE_ACCESSIONS: dict[str, dict[str, str]] = {}


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
