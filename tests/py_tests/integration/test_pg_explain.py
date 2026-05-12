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
    BP_DATES_BULK_QUERY,
)
from ddbj_search_converter.postgres.bs_date import (
    BS_ACCESSIONS_MODIFIED_SINCE_QUERY,
    BS_DATES_BULK_QUERY,
)
from ddbj_search_converter.postgres.utils import postgres_connection


def _explain(url: str, dbname: str, query: str) -> list[Any]:
    with postgres_connection(url, dbname) as conn, conn.cursor() as cur:
        cur.execute(f"EXPLAIN {query}")
        rows: list[Any] = list(cur.fetchall())
    return rows


def _plan_text(rows: list[Any]) -> str:
    """EXPLAIN row 群を 1 文字列に結合して keyword match を可能にする。"""
    return "\n".join(str(r[0]) if isinstance(r, tuple | list) else str(r) for r in rows)


def _assert_plan_has_node(rows: list[Any], expected_any: list[str]) -> None:
    """plan に ``expected_any`` のいずれかの plan node が登場することを assert。

    index 利用 (Index Scan / Bitmap Index Scan) や明示 join (Hash Join / Nested
    Loop) が消えると性能が劇的に悪化するため、構造的な regression を検出する。
    Seq Scan 1 つだけにフォールバックしているケースを catch する想定。
    """
    text = _plan_text(rows)
    assert any(token in text for token in expected_any), (
        f"plan missing expected node ({expected_any}): {text!r}"
    )


def test_bp_dates_bulk_query_plans_against_xsm_bioproject(integration_xsm_postgres_url: str) -> None:
    """IT-PG-03-bp-bulk: BP バルク取得 SQL が staging XSM (`bioproject`) で EXPLAIN 通る。

    plan に Index Scan / Bitmap Index Scan / Hash Join のいずれかが含まれることを
    確認 (Seq Scan だけにフォールバックしているケースを catch)。
    """
    query = BP_DATES_BULK_QUERY.replace("%s", "ARRAY['PRJDB1']::text[]")
    plan = _explain(integration_xsm_postgres_url, "bioproject", query)
    assert plan, "EXPLAIN returned empty plan"
    _assert_plan_has_node(plan, ["Index Scan", "Bitmap Index Scan", "Hash Join"])


def test_bp_accessions_modified_since_query_plans_against_xsm_bioproject(
    integration_xsm_postgres_url: str,
) -> None:
    """IT-PG-03-bp-modified: BP 更新分取得 SQL が staging XSM (`bioproject`) で EXPLAIN 通る。

    modified_since query は集計 + join を伴うため、planner が Index Scan ではなく
    Hash Join + Seq Scan を選ぶことが現実的に多い (テーブル サイズが大きいほど
    Hash Join が有利)。Index Scan が **完全に消える** ことだけが回帰のサイン
    だが、Hash Join も明示 join なので加える (Nested Loop だけに退化したら検出
    したい)。
    """
    query = BP_ACCESSIONS_MODIFIED_SINCE_QUERY.replace("%s", "'2026-01-01'")
    plan = _explain(integration_xsm_postgres_url, "bioproject", query)
    assert plan
    _assert_plan_has_node(plan, ["Index Scan", "Bitmap Index Scan", "Index Only Scan", "Hash Join"])


def test_bs_dates_bulk_query_plans_against_xsm_biosample(integration_xsm_postgres_url: str) -> None:
    """IT-PG-03-bs-bulk: BS バルク取得 SQL が staging XSM (`biosample`) で EXPLAIN 通る。"""
    query = BS_DATES_BULK_QUERY.replace("%s", "ARRAY['SAMD00000001']::text[]")
    plan = _explain(integration_xsm_postgres_url, "biosample", query)
    assert plan
    _assert_plan_has_node(plan, ["Index Scan", "Bitmap Index Scan", "Hash Join"])


def test_bs_accessions_modified_since_query_plans_against_xsm_biosample(
    integration_xsm_postgres_url: str,
) -> None:
    """IT-PG-03-bs-modified: BS 更新分取得 SQL が staging XSM (`biosample`) で EXPLAIN 通る。

    biosample テーブルは ~200 万行で、modified_since query には集計 (HashAggregate)
    と複数 join が含まれる。staging では planner が Parallel Seq Scan + Hash Join を
    選ぶ (これが index lookup より cost 効率がいい)。Hash Join が消えて Nested
    Loop だけになる退化は検出したいので、Hash Join も期待リストに含める。
    """
    query = BS_ACCESSIONS_MODIFIED_SINCE_QUERY.replace("%s", "'2026-01-01'")
    plan = _explain(integration_xsm_postgres_url, "biosample", query)
    assert plan
    _assert_plan_has_node(plan, ["Index Scan", "Bitmap Index Scan", "Index Only Scan", "Hash Join"])
