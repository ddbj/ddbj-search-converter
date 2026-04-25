"""Integration: log.duckdb run lifecycle invariants.

Verifies that the run lifecycle records the converter writes are
internally consistent: every run has at most one start and at most one
termination (end or failed), and the DB contains evidence of completed
runs (not just starts).
"""

from pathlib import Path

import duckdb


def test_log_db_has_start_and_termination_records(integration_log_db_path: Path) -> None:
    """IT-LOG-01: log.duckdb に start lifecycle と (end か failed) lifecycle が記録されている。"""
    with duckdb.connect(str(integration_log_db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT json_extract_string(extra, '$.lifecycle') AS lifecycle, COUNT(*) AS cnt
            FROM log_records
            WHERE json_extract_string(extra, '$.lifecycle') IS NOT NULL
            GROUP BY lifecycle
            """,
        ).fetchall()

    counts: dict[str, int] = dict(rows)
    assert counts.get("start", 0) > 0, "no start lifecycle records"
    completed = counts.get("end", 0) + counts.get("failed", 0)
    assert completed > 0, "no end/failed lifecycle records (no completed runs?)"


def test_log_db_each_run_has_at_most_one_start_and_one_termination(integration_log_db_path: Path) -> None:
    """IT-LOG-02: 各 run_id は start <= 1 個 + (end + failed) <= 1 個。

    冪等性が壊れて lifecycle が重複記録されると success / failure 判定が壊れる。
    in-progress な run は start のみ (end/failed なし) なので不等号で許容。
    """
    with duckdb.connect(str(integration_log_db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT
                run_id,
                COUNT(*) FILTER (WHERE json_extract_string(extra, '$.lifecycle') = 'start') AS starts,
                COUNT(*) FILTER (WHERE json_extract_string(extra, '$.lifecycle') = 'end') AS ends,
                COUNT(*) FILTER (WHERE json_extract_string(extra, '$.lifecycle') = 'failed') AS fails
            FROM log_records
            WHERE run_id IS NOT NULL
            GROUP BY run_id
            """,
        ).fetchall()

    bad: list[tuple[str, str, int]] = []
    for run_id, starts, ends, fails in rows:
        if starts > 1:
            bad.append((run_id, "multiple starts", starts))
        if ends + fails > 1:
            bad.append((run_id, "multiple terminations", ends + fails))

    assert not bad, f"unbalanced lifecycle for {len(bad)} run(s) (first 5): {bad[:5]}"


def test_get_last_successful_run_date_returns_a_date(integration_log_db_path: Path) -> None:
    """IT-LOG-03: ``get_last_successful_run_date`` が staging の log.duckdb で動く。

    `logging/db.py::get_last_successful_run_date` が `INFO` + `lifecycle='end'` を
    JSON extract で抽出する経路の round-trip 確認。少なくとも 1 つの run_name が
    成功完了している前提 (staging 通常運用)。
    """
    with duckdb.connect(str(integration_log_db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT run_name, MAX(run_date) AS last_date
            FROM log_records
            WHERE log_level = 'INFO'
              AND json_extract_string(extra, '$.lifecycle') = 'end'
            GROUP BY run_name
            """,
        ).fetchall()

    assert rows, "no successful run records (INFO + lifecycle='end') in staging log.duckdb"
    for run_name, last_date in rows:
        assert run_name, "run_name is empty"
        assert last_date is not None, f"run_name={run_name} has NULL last_date"
