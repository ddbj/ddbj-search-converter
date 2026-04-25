"""Integration: PostgreSQL EXPLAIN smoke for bp_date / bs_date SQL.

Verifies that the SQL templates the converter ships with still parse and
plan successfully against the staging XSM schema. A failure here means the
schema (table / column / join keys) drifted away from what the converter
expects, so the next pipeline run would crash at fetch time.

EXPLAIN does not execute the query, so this is fast and non-disruptive.
"""

from typing import Any

from ddbj_search_converter.postgres.bp_date import (
    BP_ACCESSIONS_MODIFIED_SINCE_QUERY,
    BP_DATES_BULK_QUERY_TEMPLATE,
)
from ddbj_search_converter.postgres.bs_date import (
    BS_ACCESSIONS_MODIFIED_SINCE_QUERY,
    BS_DATES_BULK_QUERY_TEMPLATE,
)
from ddbj_search_converter.postgres.utils import postgres_connection


def _explain(url: str, dbname: str, query: str) -> list[Any]:
    with postgres_connection(url, dbname) as conn, conn.cursor() as cur:
        cur.execute(f"EXPLAIN {query}")
        rows: list[Any] = list(cur.fetchall())
    return rows


def test_bp_dates_bulk_query_plans_against_xsm_bioproject(integration_xsm_postgres_url: str) -> None:
    """IT-PG-03-bp-bulk: BP バルク取得 SQL が staging XSM (`bioproject`) で EXPLAIN 通る。"""
    query = BP_DATES_BULK_QUERY_TEMPLATE.format(placeholders="'PRJDB1'")
    plan = _explain(integration_xsm_postgres_url, "bioproject", query)
    assert plan, "EXPLAIN returned empty plan"


def test_bp_accessions_modified_since_query_plans_against_xsm_bioproject(
    integration_xsm_postgres_url: str,
) -> None:
    """IT-PG-03-bp-modified: BP 更新分取得 SQL が staging XSM (`bioproject`) で EXPLAIN 通る。"""
    query = BP_ACCESSIONS_MODIFIED_SINCE_QUERY.replace("%s", "'2026-01-01'")
    plan = _explain(integration_xsm_postgres_url, "bioproject", query)
    assert plan


def test_bs_dates_bulk_query_plans_against_xsm_biosample(integration_xsm_postgres_url: str) -> None:
    """IT-PG-03-bs-bulk: BS バルク取得 SQL が staging XSM (`biosample`) で EXPLAIN 通る。"""
    query = BS_DATES_BULK_QUERY_TEMPLATE.format(placeholders="'SAMD00000001'")
    plan = _explain(integration_xsm_postgres_url, "biosample", query)
    assert plan


def test_bs_accessions_modified_since_query_plans_against_xsm_biosample(
    integration_xsm_postgres_url: str,
) -> None:
    """IT-PG-03-bs-modified: BS 更新分取得 SQL が staging XSM (`biosample`) で EXPLAIN 通る。"""
    query = BS_ACCESSIONS_MODIFIED_SINCE_QUERY.replace("%s", "'2026-01-01'")
    plan = _explain(integration_xsm_postgres_url, "biosample", query)
    assert plan
