"""
DBLink 関連を格納する DuckDB データベースの操作モジュール。

DBLink は各種 accession 間の関連を無向グラフとして管理する。
relation テーブルには (src_type, src_accession, dst_type, dst_accession) の形式で
正規化された関連が格納される。

ファイルパス:
    - 一時 DB: {const_dir}/dblink/dblink.tmp.duckdb
    - 最終 DB: {const_dir}/dblink/dblink.duckdb
"""
import shutil
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Literal, Tuple

import duckdb

from ddbj_search_converter.config import (DBLINK_DB_FILE_NAME,
                                          TMP_DBLINK_DB_FILE_NAME, TODAY_STR,
                                          Config)
from ddbj_search_converter.logging.logger import log_info

AccessionType = Literal[
    "bioproject",
    "umbrella-bioproject",
    "biosample",
    "gea",
    "hum-id",
    "insdc-assembly",
    "insdc-master",
    "jga-dac",
    "jga-dataset",
    "jga-policy",
    "jga-study",
    "metabobank",
    "pubmed-id",
    "taxonomy",
]

Relation = Tuple[AccessionType, str, AccessionType, str]
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
) -> Relation:
    """無向グラフなので、同じ関連が (A,B) と (B,A) で重複しないよう正規化。"""
    if (a_type, a_id) <= (b_type, b_id):
        return a_type, a_id, b_type, b_id
    return b_type, b_id, a_type, a_id


def init_dblink_db(config: Config) -> None:
    db_path = _tmp_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE relation (
                src_type TEXT,
                src_accession TEXT,
                dst_type TEXT,
                dst_accession TEXT
            )
        """)


def finalize_relation_db(config: Config) -> None:
    deduplicate_relations(config)
    create_relation_indexes(config)

    tmp_path = _tmp_db_path(config)
    final_path = _final_db_path(config)

    if final_path.exists():
        final_path.unlink()

    shutil.move(str(tmp_path), str(final_path))


# === Write operations ===


def get_tmp_dir(config: Config) -> Path:
    tmp_dir = config.result_dir.joinpath("dblink", "tmp", TODAY_STR)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    return tmp_dir


def write_relations_to_tsv(
    output_path: Path,
    relations: Iterable[Relation],
    *,
    append: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with output_path.open(mode, encoding="utf-8") as f:
        for r in relations:
            normalized = normalize_edge(*r)
            f.write("\t".join(normalized) + "\n")


def load_relations_from_tsv(config: Config, tsv_path: Path) -> None:
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(f"""
            INSERT INTO relation
            SELECT * FROM read_csv(
                '{tsv_path}',
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
    def line_generator() -> Iterable[Relation]:
        for src_id, dst_id in lines:
            yield (type_src, src_id, type_dst, dst_id)

    tsv_name = f"{type_src}_to_{type_dst}.tsv"
    tmp_dir = get_tmp_dir(config)
    tsv_path = tmp_dir.joinpath(tsv_name)

    log_info(f"writing {len(lines)} relations to {tsv_path}", file=str(tsv_path))
    write_relations_to_tsv(tsv_path, line_generator(), append=True)

    log_info(f"loading relations from {tsv_path}", file=str(tsv_path))
    load_relations_from_tsv(config, tsv_path)


def deduplicate_relations(config: Config) -> None:
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE relation_dedup AS
            SELECT DISTINCT
                src_type, src_accession, dst_type, dst_accession
            FROM relation
        """)
        conn.execute("DROP TABLE relation")
        conn.execute("ALTER TABLE relation_dedup RENAME TO relation")


def create_relation_indexes(config: Config) -> None:
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE UNIQUE INDEX idx_relation_unique
            ON relation (src_type, src_accession, dst_type, dst_accession)
        """)
        conn.execute("""
            CREATE INDEX idx_relation_src
            ON relation (src_type, src_accession)
        """)
        conn.execute("""
            CREATE INDEX idx_relation_dst
            ON relation (dst_type, dst_accession)
        """)


# === Read operations ===


def get_related_entities(
    config: Config,
    *,
    entity_type: AccessionType,
    accession: str,
) -> Iterator[tuple[str, str]]:
    """無向グラフなので src/dst 両方向を検索。"""
    db_path = _final_db_path(config)
    with duckdb.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN src_type = ? AND src_accession = ?
                    THEN dst_type
                    ELSE src_type
                END AS related_type,
                CASE
                    WHEN src_type = ? AND src_accession = ?
                    THEN dst_accession
                    ELSE src_accession
                END AS related_accession
            FROM relation
            WHERE
                (src_type = ? AND src_accession = ?)
             OR (dst_type = ? AND dst_accession = ?)
            """,
            (
                entity_type, accession,
                entity_type, accession,
                entity_type, accession,
                entity_type, accession,
            ),
        ).fetchall()

    yield from rows


def get_related_entities_bulk(
    config: Config,
    *,
    entity_type: AccessionType,
    accessions: list[str],
) -> Dict[str, List[Tuple[AccessionType, str]]]:
    if not accessions:
        return {}

    db_path = _final_db_path(config)

    with duckdb.connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH input(accession) AS (
                SELECT UNNEST(?)
            )
            SELECT
                i.accession AS query_accession,
                CASE
                    WHEN r.src_type = ? AND r.src_accession = i.accession
                    THEN r.dst_type
                    ELSE r.src_type
                END AS related_type,
                CASE
                    WHEN r.src_type = ? AND r.src_accession = i.accession
                    THEN r.dst_accession
                    ELSE r.src_accession
                END AS related_accession
            FROM relation r
            JOIN input i
              ON (r.src_type = ? AND r.src_accession = i.accession)
              OR (r.dst_type = ? AND r.dst_accession = i.accession)
            """,
            (
                accessions,
                entity_type,
                entity_type,
                entity_type,
                entity_type,
            ),
        ).fetchall()

    result: Dict[str, List[Tuple[AccessionType, str]]] = {}
    for acc, r_type, r_acc in rows:
        result.setdefault(acc, []).append((r_type, r_acc))

    return result


def export_relations(
    config: Config,
    output_path: Path,
    *,
    type_a: AccessionType,
    type_b: AccessionType,
) -> None:
    with duckdb.connect(str(_final_db_path(config))) as conn:
        rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN src_type = ? THEN src_accession ELSE dst_accession
                END AS a_accession,
                CASE
                    WHEN src_type = ? THEN dst_accession ELSE src_accession
                END AS b_accession
            FROM relation
            WHERE
                (src_type = ? AND dst_type = ?)
             OR (src_type = ? AND dst_type = ?)
            ORDER BY 1, 2
            """,
            (
                type_a, type_a,
                type_a, type_b,
                type_b, type_a,
            ),
        ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for a, b in rows:
            f.write(f"{a}\t{b}\n")
