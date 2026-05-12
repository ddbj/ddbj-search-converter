"""``finalize_dblink_db`` 完了後の ``dbxref`` 不変条件 helper。

各 dblink モジュール (insdc / bp_bs / jga / gea / metabobank / sra_internal) の
テストは元データを ``raw_edges`` に積んだ後、``finalize_dblink_db`` を実行し、
本 helper を呼び出して半辺化対称性が成立しているかを assert する。

不変条件:
    (1) ``dbxref`` の行数 = 2 × (unique canonical edge 数) - (self-loop 数)
        self-loop ``(A,a)-(A,a)`` は build_dbxref_table の UNION ALL でも
        1 行しか出ない (両方向が等価)。``allow_self_loops=True`` で許容する。
    (2) 任意の ``(a→b)`` 行に対し ``(b→a)`` 行が存在する (self-loop を除く)。
    (3) DISTINCT で重複なし (`SELECT COUNT(*) == COUNT(DISTINCT ...)`)。
    (4) ``idx_dbxref_accession`` / ``idx_dbxref_unique`` index が登録済み。

SPEC: docs/data-architecture.md §DBLink DB の半辺化スキーマ。
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ddbj_search_converter.config import Config


def _final_db_path(config: Config) -> Path:
    from ddbj_search_converter.config import DBLINK_DB_FILE_NAME

    return config.const_dir.joinpath("dblink", DBLINK_DB_FILE_NAME)


def assert_dbxref_symmetric(
    config: Config,
    *,
    allow_self_loops: bool = False,
) -> None:
    """``finalize_dblink_db`` 後の ``dbxref`` を構造的不変条件で検証する。

    Args:
        config: ``const_dir`` 経由で ``dblink.duckdb`` を解決する Config。
        allow_self_loops: True なら ``(A,a)-(A,a)`` を許容して count を補正、
            False なら self-loop の存在自体を error にする。
    """
    db_path = _final_db_path(config)
    assert db_path.exists(), f"finalize_dblink_db 未実行か path 不一致: {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # (1) 行数と DISTINCT 数が一致
        total = con.execute("SELECT COUNT(*) FROM dbxref").fetchone()
        distinct = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT DISTINCT accession_type, accession, linked_type, linked_accession
                FROM dbxref
            )
            """
        ).fetchone()
        assert total is not None
        assert distinct is not None
        assert total[0] == distinct[0], f"dbxref に重複行: total={total[0]} distinct={distinct[0]}"

        # (2) self-loop 検査
        self_loops = con.execute(
            """
            SELECT COUNT(*) FROM dbxref
            WHERE accession_type = linked_type AND accession = linked_accession
            """
        ).fetchone()
        assert self_loops is not None
        if not allow_self_loops and self_loops[0] > 0:
            samples = con.execute(
                """
                SELECT accession_type, accession FROM dbxref
                WHERE accession_type = linked_type AND accession = linked_accession
                LIMIT 5
                """
            ).fetchall()
            raise AssertionError(f"self-loop が {self_loops[0]} 件存在 (allow_self_loops=False): {samples}")

        # (3) 対称性: 任意 row (a→b) に対し (b→a) が存在 (self-loop を除く)
        missing_reverse = con.execute(
            """
            SELECT a.accession_type, a.accession, a.linked_type, a.linked_accession
            FROM dbxref a
            LEFT JOIN dbxref b
              ON a.accession_type = b.linked_type
             AND a.accession      = b.linked_accession
             AND a.linked_type    = b.accession_type
             AND a.linked_accession = b.accession
            WHERE b.accession IS NULL
              AND NOT (a.accession_type = a.linked_type AND a.accession = a.linked_accession)
            LIMIT 5
            """
        ).fetchall()
        assert not missing_reverse, f"対称性違反: 逆方向行が欠落しているサンプル {missing_reverse}"

        # (4) 半辺化件数: 非 self-loop は 2 行ペア。canonical edge 数 = (total - self_loops) / 2 + self_loops
        non_self = total[0] - self_loops[0]
        assert non_self % 2 == 0, f"半辺化スキーマ違反: 非 self-loop 行数 {non_self} が偶数でない"

        # (5) index が張られているか
        idx_rows = con.execute("SELECT index_name FROM duckdb_indexes() WHERE table_name = 'dbxref'").fetchall()
        names = {r[0] for r in idx_rows}
        assert "idx_dbxref_accession" in names, f"idx_dbxref_accession 不在: {names}"
        assert "idx_dbxref_unique" in names, f"idx_dbxref_unique 不在: {names}"
    finally:
        con.close()


def count_canonical_edges(config: Config) -> int:
    """無向 edge 数 (canonical 形) を返す。``show_dblink_counts`` の集計と同じ式。

    区切り ``|`` は AccessionType (Literal 値) や accession (英数字 + ``_``/``-``/
    ``.``) に出現しないため、衝突しない。
    """
    db_path = _final_db_path(config)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = con.execute(
            """
            SELECT COUNT(DISTINCT (
                LEAST(accession_type || '|' || accession,
                      linked_type   || '|' || linked_accession),
                GREATEST(accession_type || '|' || accession,
                         linked_type   || '|' || linked_accession)
            ))
            FROM dbxref
            """
        ).fetchone()
        assert result is not None
        return int(result[0])
    finally:
        con.close()
