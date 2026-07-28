"""Integration: PostgreSQL EXPLAIN smoke for the date cache SQL.

Verifies that the SQL the date cache actually issues still parses and plans
against the staging XSM schema. A failure here means the schema (table /
column / join keys) drifted away from what the converter expects, so the
next pipeline run would crash at fetch time.

EXPLAIN does not execute the query, so this is fast and non-disruptive.
"""

from typing import Any

import pytest

from ddbj_search_converter.date_cache.build import DATE_SOURCES, DateSource, build_query
from ddbj_search_converter.postgres.utils import postgres_connection

# index 利用 (Index Scan / Bitmap Index Scan / Index Only Scan) や明示 join
# (Hash Join) が全て消えると性能が劇的に悪化するため、構造的な regression を
# 検出する。Seq Scan と Nested Loop だけに退化したケースを catch する想定。
EXPECTED_PLAN_NODES = ["Index Scan", "Bitmap Index Scan", "Index Only Scan", "Hash Join"]


def _explain(url: str, dbname: str, query: str) -> list[Any]:
    with postgres_connection(url, dbname) as conn, conn.cursor() as cur:
        cur.execute(f"EXPLAIN {query}")
        rows: list[Any] = list(cur.fetchall())
    return rows


def _plan_text(rows: list[Any]) -> str:
    """EXPLAIN row 群を 1 文字列に結合して keyword match を可能にする。"""
    return "\n".join(str(r[0]) if isinstance(r, tuple | list) else str(r) for r in rows)


def _assert_plan_has_node(rows: list[Any], expected_any: list[str]) -> None:
    text = _plan_text(rows)
    assert any(token in text for token in expected_any), f"plan missing expected node ({expected_any}): {text!r}"


@pytest.mark.parametrize("source", DATE_SOURCES, ids=lambda source: source.table)
def test_full_build_query_plans_against_xsm(source: DateSource, integration_xsm_postgres_url: str) -> None:
    """IT-PG-03: 全件ビルドが発行する SQL が staging XSM で EXPLAIN 通る。"""
    plan = _explain(
        integration_xsm_postgres_url,
        source.postgres_db_name,
        build_query(source.base_query, None),
    )
    assert plan, "EXPLAIN returned empty plan"
    _assert_plan_has_node(plan, EXPECTED_PLAN_NODES)


@pytest.mark.parametrize("source", DATE_SOURCES, ids=lambda source: source.table)
def test_window_build_query_plans_against_xsm(source: DateSource, integration_xsm_postgres_url: str) -> None:
    """IT-PG-03: 窓ビルドが発行する SQL が staging XSM で EXPLAIN 通る。

    窓の条件は `modified_date` に効く。この列に index が無くても planner は
    Parallel Seq Scan + Hash Join を選ぶので、join が消えていないことだけを見る。
    """
    query = build_query(source.base_query, "2026-01-01").replace("%s", "'2026-01-01'")
    plan = _explain(integration_xsm_postgres_url, source.postgres_db_name, query)
    assert plan, "EXPLAIN returned empty plan"
    _assert_plan_has_node(plan, EXPECTED_PLAN_NODES)
