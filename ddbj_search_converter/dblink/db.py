"""
DBLink 関連を格納する DuckDB データベースの操作モジュール。

DBLink は各種 accession 間の関連を無向グラフとして管理する。

スキーマは 2 段階:
    - 中間 ``raw_edges`` テーブル (src_type, src_accession, dst_type, dst_accession):
      各 parser が canonical 形 (``normalize_edge`` で ``(src_type, src_accession)
      <= (dst_type, dst_accession)`` を保証) で append する。
    - 最終 ``dbxref`` テーブル (accession_type, accession, linked_type,
      linked_accession): ``build_dbxref_table`` で ``raw_edges`` を UNION ALL で
      両方向に mirror (半辺化) して構築する。1 つの無向 edge ``{A, B}`` が 2 行
      として保存されるため、``WHERE accession_type=? AND accession=?`` の単一
      lookup で両端点の隣接を取得でき、DuckDB の zone map が両方向で効く。

``finalize_dblink_db`` で ``raw_edges`` は DROP され、最終 DB には ``dbxref``
のみ残る。

ファイルパス:
    - 一時 DB: {const_dir}/dblink/dblink.tmp.duckdb
    - 最終 DB: {const_dir}/dblink/dblink.duckdb
"""

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Literal

import duckdb

from ddbj_search_converter.config import (
    DBLINK_DB_FILE_NAME,
    TMP_DBLINK_DB_FILE_NAME,
    TMP_UMBRELLA_DB_FILE_NAME,
    TODAY_STR,
    UMBRELLA_DB_FILE_NAME,
    Config,
)
from ddbj_search_converter.logging.logger import log_info

AccessionType = Literal[
    "bioproject",
    "biosample",
    "gea",
    "geo",
    "humandbs",
    "insdc",
    "insdc-assembly",
    "insdc-master",
    "jga-dac",
    "jga-dataset",
    "jga-policy",
    "jga-study",
    "metabobank",
    "pubmed",
    "sra-analysis",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-study",
    "sra-submission",
    "taxonomy",
]

Edge = tuple[AccessionType, str, AccessionType, str]
IdPairs = set[tuple[str, str]]


def _tmp_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("dblink", TMP_DBLINK_DB_FILE_NAME)


def _final_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("dblink", DBLINK_DB_FILE_NAME)


def normalize_edge(
    a_type: AccessionType,
    a_id: str,
    b_type: AccessionType,
    b_id: str,
) -> Edge:
    """無向 edge を canonical 形に正規化する。

    ``(a_type, a_id) <= (b_type, b_id)`` となる向きで 4-tuple を返す。同じ edge
    が複数の source から異なる向きで来ても、``raw_edges`` には同じ行として投入
    されるため、``build_dbxref_table`` の DISTINCT で自然に dedup される。
    """
    if (a_type, a_id) <= (b_type, b_id):
        return a_type, a_id, b_type, b_id
    return b_type, b_id, a_type, a_id


def init_dblink_db(config: Config) -> None:
    """一時 DB に ``raw_edges`` テーブル (canonical 形の edge 蓄積用) を作る。"""
    db_path = _tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE raw_edges (
                src_type TEXT,
                src_accession TEXT,
                dst_type TEXT,
                dst_accession TEXT
            )
        """)


def finalize_dblink_db(config: Config) -> None:
    """``raw_edges`` から ``dbxref`` を構築し、index を張り、tmp → final に replace。"""
    build_dbxref_table(config)
    create_dbxref_indexes(config)

    tmp_path = _tmp_db_path(config)
    final_path = _final_db_path(config)

    tmp_path.replace(final_path)


# === Write operations ===


def get_tmp_dir(config: Config) -> Path:
    tmp_dir = config.result_dir.joinpath("dblink", "tmp", TODAY_STR)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    return tmp_dir


def write_edges_to_tsv(
    output_path: Path,
    edges: Iterable[Edge],
    *,
    append: bool = False,
) -> None:
    """Edge を canonical 形に正規化して TSV に書き出す。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with output_path.open(mode, encoding="utf-8") as f:
        for r in edges:
            normalized = normalize_edge(*r)
            f.write("\t".join(normalized) + "\n")


def load_edges_from_tsv(config: Config, tsv_path: Path) -> None:
    """Canonical TSV を ``raw_edges`` テーブルに bulk COPY する。"""
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        safe_path = str(tsv_path).replace("'", "''")
        conn.execute(f"""
            INSERT INTO raw_edges
            SELECT * FROM read_csv(
                '{safe_path}',
                header=false,
                columns={{
                    'src_type': 'TEXT',
                    'src_accession': 'TEXT',
                    'dst_type': 'TEXT',
                    'dst_accession': 'TEXT'
                }},
                delim='\t'
            )
        """)


def load_to_db(
    config: Config,
    lines: IdPairs,
    type_src: AccessionType,
    type_dst: AccessionType,
) -> None:
    def line_generator() -> Iterable[Edge]:
        for src_id, dst_id in lines:
            yield (type_src, src_id, type_dst, dst_id)

    tsv_name = f"{type_src}_to_{type_dst}.tsv"
    tmp_dir = get_tmp_dir(config)
    tsv_path = tmp_dir.joinpath(tsv_name)

    log_info(f"writing {len(lines)} edges to {tsv_path}", file=str(tsv_path))
    write_edges_to_tsv(tsv_path, line_generator(), append=True)

    log_info(f"loading edges from {tsv_path}", file=str(tsv_path))
    load_edges_from_tsv(config, tsv_path)


def build_dbxref_table(config: Config) -> None:
    """``raw_edges`` を両方向に mirror して半辺化 ``dbxref`` を構築する。

    canonical 形 (A -> B, A <= B) の edge を 2 つの半辺 (A -> B と B -> A) に
    展開し、DISTINCT + ORDER BY で sort 済みの最終テーブルを作る。完了後、
    ``raw_edges`` は DROP する。
    """
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE dbxref AS
            SELECT DISTINCT
                accession_type, accession, linked_type, linked_accession
            FROM (
                SELECT
                    src_type AS accession_type,
                    src_accession AS accession,
                    dst_type AS linked_type,
                    dst_accession AS linked_accession
                FROM raw_edges
                UNION ALL
                SELECT
                    dst_type AS accession_type,
                    dst_accession AS accession,
                    src_type AS linked_type,
                    src_accession AS linked_accession
                FROM raw_edges
            )
            ORDER BY accession_type, accession, linked_type, linked_accession
        """)
        conn.execute("DROP TABLE raw_edges")


def create_dbxref_indexes(config: Config) -> None:
    """``dbxref`` に unique 制約 + accession 前方検索の index を張る。

    半辺化により ``(accession_type, accession)`` prefix で両端点が covering
    されるため、旧 ``idx_relation_dst`` 相当の逆方向 index は不要。
    """
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE UNIQUE INDEX idx_dbxref_unique
            ON dbxref (accession_type, accession, linked_type, linked_accession)
        """)
        conn.execute("""
            CREATE INDEX idx_dbxref_accession
            ON dbxref (accession_type, accession)
        """)


# === Read operations ===


def get_linked_entities(
    config: Config,
    *,
    entity_type: AccessionType,
    accession: str,
) -> Iterator[tuple[str, str]]:
    """半辺化 ``dbxref`` なので単一 WHERE で両端点の隣接を取得できる。"""
    db_path = _final_db_path(config)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT linked_type, linked_accession
            FROM dbxref
            WHERE accession_type = ? AND accession = ?
            """,
            (entity_type, accession),
        ).fetchall()

    yield from rows


def get_linked_entities_bulk(
    config: Config,
    *,
    entity_type: AccessionType,
    accessions: list[str],
) -> dict[str, list[tuple[AccessionType, str]]]:
    if not accessions:
        return {}

    db_path = _final_db_path(config)

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            WITH input(accession) AS (
                SELECT UNNEST(?)
            )
            SELECT
                i.accession AS query_accession,
                d.linked_type,
                d.linked_accession
            FROM dbxref d
            JOIN input i
              ON d.accession_type = ?
             AND d.accession = i.accession
            """,
            (
                accessions,
                entity_type,
            ),
        ).fetchall()

    result: dict[str, list[tuple[AccessionType, str]]] = {}
    for acc, r_type, r_acc in rows:
        result.setdefault(acc, []).append((r_type, r_acc))

    return result


def export_edges(
    config: Config,
    output_path: Path,
    *,
    type_a: AccessionType,
    type_b: AccessionType,
) -> None:
    """無向 edge を TSV に書き出す。

    半辺化 ``dbxref`` では ``(accession_type, linked_type)`` の一方向のみ
    SELECT するだけで canonical に 1 edge 1 行が取れる (逆方向の半辺は
    ``(linked_type, accession_type)`` フィルタ側に入るため)。

    Note:
        現在 ``type_a == type_b`` の呼び出しは存在しない (出力 TSV 18 種すべて
        異なる type 間の関連、`docs/data-architecture.md` 参照)。将来同一 type
        間の関連を export する場合は ``AND accession <= linked_accession`` を
        追加して半辺ペアを 1 行に dedup する必要がある。
    """
    with duckdb.connect(str(_final_db_path(config)), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT accession, linked_accession
            FROM dbxref
            WHERE accession_type = ? AND linked_type = ?
            ORDER BY 1, 2
            """,
            (type_a, type_b),
        ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for a, b in rows:
            f.write(f"{a}\t{b}\n")


# === Umbrella DB operations ===


def _umbrella_tmp_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("dblink", TMP_UMBRELLA_DB_FILE_NAME)


def _umbrella_final_db_path(config: Config) -> Path:
    return config.const_dir.joinpath("dblink", UMBRELLA_DB_FILE_NAME)


def init_umbrella_db(config: Config) -> None:
    """Umbrella DB (tmp) を初期化する。"""
    db_path = _umbrella_tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE umbrella_relation (
                parent_accession TEXT NOT NULL,
                child_accession TEXT NOT NULL
            )
        """)


def finalize_umbrella_db(config: Config) -> None:
    """Umbrella DB を重複排除・インデックス作成して tmp → final に移動する。"""
    tmp_path = _umbrella_tmp_db_path(config)
    if not tmp_path.exists():
        return

    with duckdb.connect(str(tmp_path)) as conn:
        conn.execute("""
            CREATE TABLE umbrella_relation_dedup AS
            SELECT DISTINCT parent_accession, child_accession
            FROM umbrella_relation
        """)
        conn.execute("DROP TABLE umbrella_relation")
        conn.execute("ALTER TABLE umbrella_relation_dedup RENAME TO umbrella_relation")
        conn.execute("""
            CREATE INDEX idx_umbrella_parent
            ON umbrella_relation (parent_accession)
        """)
        conn.execute("""
            CREATE INDEX idx_umbrella_child
            ON umbrella_relation (child_accession)
        """)

    final_path = _umbrella_final_db_path(config)
    tmp_path.replace(final_path)


def save_umbrella_relations(config: Config, relations: IdPairs) -> None:
    """Umbrella DB (tmp) に (parent, child) ペアを書き込む。"""
    if not relations:
        return

    db_path = _umbrella_tmp_db_path(config)
    rows = list(relations)

    log_info(f"saving {len(rows)} umbrella relations")

    with duckdb.connect(str(db_path)) as conn:
        conn.executemany(
            "INSERT INTO umbrella_relation (parent_accession, child_accession) VALUES (?, ?)",
            rows,
        )


def get_umbrella_parent_child_maps(
    config: Config,
    accessions: list[str],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Umbrella DB (final) から親子マップを取得する。

    Returns:
        (parent_map, child_map):
        - parent_map[accession] = そのaccessionの親 accession リスト
        - child_map[accession] = そのaccessionの子 accession リスト
    """
    if not accessions:
        return {}, {}

    db_path = _umbrella_final_db_path(config)
    if not db_path.exists():
        return {}, {}

    with duckdb.connect(str(db_path), read_only=True) as conn:
        # child → parent (対象 accession が child として存在する行)
        parent_rows = conn.execute(
            """
            WITH input(accession) AS (SELECT UNNEST(?))
            SELECT i.accession, r.parent_accession
            FROM umbrella_relation r
            JOIN input i ON r.child_accession = i.accession
            """,
            (accessions,),
        ).fetchall()

        # parent → child (対象 accession が parent として存在する行)
        child_rows = conn.execute(
            """
            WITH input(accession) AS (SELECT UNNEST(?))
            SELECT i.accession, r.child_accession
            FROM umbrella_relation r
            JOIN input i ON r.parent_accession = i.accession
            """,
            (accessions,),
        ).fetchall()

    parent_map: dict[str, list[str]] = {}
    for acc, parent_acc in parent_rows:
        parent_map.setdefault(acc, []).append(parent_acc)

    child_map: dict[str, list[str]] = {}
    for acc, child_acc in child_rows:
        child_map.setdefault(acc, []).append(child_acc)

    return parent_map, child_map
