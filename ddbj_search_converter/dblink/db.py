import shutil
from pathlib import Path
from typing import Iterable, Iterator, List, Literal, Tuple

import duckdb
from duckdb import DuckDBPyConnection

from ddbj_search_converter.config import Config

DB_FILE_NAME = "dblink.duckdb"
TMP_DB_FILE_NAME = "dblink.tmp.duckdb"

AccessionType = Literal[
    "bioproject",
    "umbrella-bioproject",
    "biosample",
    "gea",
    "hum-id",
    "insdc",
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


def init_dblink_db(config: Config) -> DuckDBPyConnection:
    """
    Initialize a new dblink relation database.
    Existing file is overwritten.
    """
    db_path = config.const_dir.joinpath(TMP_DB_FILE_NAME)
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))

    conn.execute(
        """
        CREATE TABLE relation (
            src_type      TEXT,
            src_accession TEXT,
            dst_type      TEXT,
            dst_accession TEXT,
        );
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX idx_relation_unique
        ON relation (
            src_type, src_accession,
            dst_type, dst_accession
        );
        """
    )

    conn.execute(
        "CREATE INDEX idx_relation_src ON relation (src_type, src_accession);"
    )
    conn.execute(
        "CREATE INDEX idx_relation_dst ON relation (dst_type, dst_accession);"
    )

    return conn


def finalize_relation_db(config: Config) -> None:
    """
    Atomically replace final DB with tmp DB.
    """
    tmp_path = config.const_dir.joinpath(TMP_DB_FILE_NAME)
    final_path = config.const_dir.joinpath(DB_FILE_NAME)

    if final_path.exists():
        final_path.unlink()

    shutil.move(str(tmp_path), str(final_path))


def insert_relation(
    conn: DuckDBPyConnection,
    src_type: AccessionType,
    src_accession: str,
    dst_type: AccessionType,
    dst_accession: str,
) -> None:
    """
    Insert a single relation (normalized).
    """
    s_type, s_id, d_type, d_id = normalize_edge(
        src_type, src_accession, dst_type, dst_accession
    )

    conn.execute(
        """
        INSERT OR IGNORE INTO relation
        VALUES (?, ?, ?, ?)
        """,
        (s_type, s_id, d_type, d_id),
    )


def bulk_insert_relations(
    conn: DuckDBPyConnection,
    relations: Iterable[Relation],
    *,
    chunk_size: int = 100_000,
) -> None:
    """
    Bulk insert relations efficiently.
    """
    buffer: List[Tuple[str, str, str, str]] = []

    def flush() -> None:
        if not buffer:
            return
        conn.executemany(
            """
            INSERT OR IGNORE INTO relation
            VALUES (?, ?, ?, ?)
            """,
            buffer,
        )
        buffer.clear()

    for src_type, src_id, dst_type, dst_id in relations:
        s_type, s_id, d_type, d_id = normalize_edge(
            src_type, src_id, dst_type, dst_id
        )
        buffer.append((s_type, s_id, d_type, d_id))

        if len(buffer) >= chunk_size:
            flush()

    flush()


def get_related_entities(
    conn: DuckDBPyConnection,
    *,
    entity_type: str,
    accession: str,
) -> Iterator[Tuple[str, str]]:
    """
    Get all entities related to a given entity.
    Returns (related_type, related_accession).
    """
    query = """
        SELECT
            CASE
                WHEN src_type = ? THEN dst_type
                ELSE src_type
            END AS related_type,
            CASE
                WHEN src_accession = ? THEN dst_accession
                ELSE src_accession
            END AS related_accession
        FROM relation
        WHERE
            (src_type = ? AND src_accession = ?)
         OR (dst_type = ? AND dst_accession = ?)
    """

    rows = conn.execute(
        query,
        (
            entity_type,
            accession,
            entity_type,
            accession,
            entity_type,
            accession,
        ),
    ).fetchall()

    yield from rows


def get_relations_between_types(
    conn: DuckDBPyConnection,
    type_a: AccessionType,
    type_b: AccessionType,
) -> Iterator[Relation]:
    """
    Get all relations between two entity types.
    """
    query = """
        SELECT src_type, src_accession, dst_type, dst_accession
        FROM relation
        WHERE
            (src_type = ? AND dst_type = ?)
         OR (src_type = ? AND dst_type = ?)
    """

    rows = conn.execute(
        query,
        (type_a, type_b, type_b, type_a),
    ).fetchall()

    yield from rows


def export_relations(
    conn: DuckDBPyConnection,
    output_path: Path,
    *,
    type_a: AccessionType,
    type_b: AccessionType,
) -> None:
    """
    Export relations to TSV.
    """
    query = """
        SELECT src_accession, dst_accession
        FROM relation
        WHERE
            (src_type = ? AND dst_type = ?)
            OR (src_type = ? AND dst_type = ?)
        ORDER BY 1, 2
    """
    rows = conn.execute(
        query, (type_a, type_b, type_b, type_a)
    ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write("\t".join(row))
