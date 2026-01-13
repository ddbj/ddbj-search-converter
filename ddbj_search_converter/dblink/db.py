import shutil
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Literal, Tuple

import duckdb

from ddbj_search_converter.config import Config

DB_FILE_NAME = "dblink.duckdb"
TMP_DB_FILE_NAME = "dblink.tmp.duckdb"

AccessionType = Literal[
    "bioproject",
    "umbrella-bioproject",
    "biosample",
    "gea",
    "hum-id",
    "insdc-assembly",
    "insdc-master",
    "jga-dataset",
    "jga-study",
    "metabobank",
    "pubmed-id",
    "taxonomy",
]

Relation = Tuple[AccessionType, str, AccessionType, str]
# (src_type, src_accession, dst_type, dst_accession)


def _tmp_db_path(config: Config) -> Path:
    return config.const_dir.joinpath(TMP_DB_FILE_NAME)


def _final_db_path(config: Config) -> Path:
    return config.const_dir.joinpath(DB_FILE_NAME)


def normalize_edge(
    a_type: AccessionType,
    a_id: str,
    b_type: AccessionType,
    b_id: str,
) -> Relation:
    """
    Normalize an undirected edge into a canonical (src, dst) order.
    """
    if (a_type, a_id) <= (b_type, b_id):
        return a_type, a_id, b_type, b_id
    return b_type, b_id, a_type, a_id


def init_dblink_db(config: Config) -> None:
    """
    Initialize a new dblink relation database.
    Existing file is overwritten.
    """
    db_path = _tmp_db_path(config)
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
    """
    Atomically replace final DB with tmp DB.
    """
    deduplicate_relations(config)
    create_relation_indexes(config)

    tmp_path = _tmp_db_path(config)
    final_path = _final_db_path(config)

    if final_path.exists():
        final_path.unlink()

    shutil.move(str(tmp_path), str(final_path))


# === Write operations ===


def insert_relation(
    config: Config,
    src_type: AccessionType,
    src_accession: str,
    dst_type: AccessionType,
    dst_accession: str,
) -> None:
    """
    Insert a single relation (normalized).
    """
    db_path = _tmp_db_path(config)
    s_type, s_id, d_type, d_id = normalize_edge(
        src_type, src_accession, dst_type, dst_accession
    )
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO relation VALUES (?, ?, ?, ?)",
            (s_type, s_id, d_type, d_id),
        )


def bulk_insert_relations(
    config: Config,
    relations: Iterable[Relation],
    *,
    chunk_size: int = 100000,
) -> None:
    """
    Bulk insert relations efficiently.
    """
    db_path = _tmp_db_path(config)
    with duckdb.connect(str(db_path)) as conn:
        buffer: List[Tuple[str, str, str, str]] = []

        def flush() -> None:
            if buffer:
                conn.executemany(
                    "INSERT INTO relation VALUES (?, ?, ?, ?)",
                    buffer,
                )
                buffer.clear()

        for r in relations:
            buffer.append(normalize_edge(*r))
            if len(buffer) >= chunk_size:
                flush()
        flush()


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
        # DuckDB では list を UNNEST できる
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
    """
    Export relations to TSV as:
      accession_of_type_a \t accession_of_type_b
    """
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
