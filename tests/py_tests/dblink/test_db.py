"""Tests for ddbj_search_converter.dblink.db module."""

import tempfile
from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (
    AccessionType,
    Edge,
    build_dbxref_table,
    export_edges,
    finalize_dblink_db,
    finalize_umbrella_db,
    get_linked_entities,
    get_linked_entities_bulk,
    get_umbrella_parent_child_maps,
    init_dblink_db,
    init_umbrella_db,
    load_edges_from_tsv,
    normalize_edge,
    save_umbrella_relations,
    write_edges_to_tsv,
)
from ddbj_search_converter.logging.logger import run_logger
from tests.py_tests.strategies import st_accession_type


class TestNormalizeEdge:
    """Tests for normalize_edge function."""

    def test_already_sorted_order(self) -> None:
        result = normalize_edge("bioproject", "PRJDB1", "biosample", "SAMD1")
        assert result == ("bioproject", "PRJDB1", "biosample", "SAMD1")

    def test_reverse_order_gets_normalized(self) -> None:
        result = normalize_edge("biosample", "SAMD1", "bioproject", "PRJDB1")
        assert result == ("bioproject", "PRJDB1", "biosample", "SAMD1")

    def test_same_type_different_id(self) -> None:
        result = normalize_edge("bioproject", "PRJDB2", "bioproject", "PRJDB1")
        assert result == ("bioproject", "PRJDB1", "bioproject", "PRJDB2")

    def test_identical_pair(self) -> None:
        result = normalize_edge("bioproject", "PRJDB1", "bioproject", "PRJDB1")
        assert result == ("bioproject", "PRJDB1", "bioproject", "PRJDB1")

    @pytest.mark.parametrize(
        ("a_type", "a_id", "b_type", "b_id", "expected"),
        [
            ("gea", "E-GEAD-1", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "gea", "E-GEAD-1")),
            ("jga-study", "JGAS1", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "jga-study", "JGAS1")),
            ("taxonomy", "9606", "biosample", "SAMD1", ("biosample", "SAMD1", "taxonomy", "9606")),
            (
                "insdc-assembly",
                "GCA_001",
                "bioproject",
                "PRJDB1",
                ("bioproject", "PRJDB1", "insdc-assembly", "GCA_001"),
            ),
        ],
    )
    def test_various_accession_types(
        self,
        a_type: AccessionType,
        a_id: str,
        b_type: AccessionType,
        b_id: str,
        expected: Edge,
    ) -> None:
        result = normalize_edge(a_type, a_id, b_type, b_id)
        assert result == expected


class TestNormalizeEdgePBT:
    """Property-based tests for normalize_edge."""

    @given(
        a_type=st_accession_type(),
        a_id=st.text(min_size=1, max_size=20),
        b_type=st_accession_type(),
        b_id=st.text(min_size=1, max_size=20),
    )
    def test_commutativity(self, a_type: str, a_id: str, b_type: str, b_id: str) -> None:
        """normalize_edge(a,b) == normalize_edge(b,a): 可換性。"""
        result1 = normalize_edge(a_type, a_id, b_type, b_id)  # type: ignore[arg-type]
        result2 = normalize_edge(b_type, b_id, a_type, a_id)  # type: ignore[arg-type]
        assert result1 == result2

    @given(
        a_type=st_accession_type(),
        a_id=st.text(min_size=1, max_size=20),
        b_type=st_accession_type(),
        b_id=st.text(min_size=1, max_size=20),
    )
    def test_idempotency(self, a_type: str, a_id: str, b_type: str, b_id: str) -> None:
        """normalize_edge は冪等。"""
        result1 = normalize_edge(a_type, a_id, b_type, b_id)  # type: ignore[arg-type]
        result2 = normalize_edge(*result1)
        assert result1 == result2

    @given(
        a_type=st_accession_type(),
        a_id=st.text(min_size=1, max_size=20),
        b_type=st_accession_type(),
        b_id=st.text(min_size=1, max_size=20),
    )
    def test_result_is_sorted(self, a_type: str, a_id: str, b_type: str, b_id: str) -> None:
        """結果のペアは辞書順でソートされている。"""
        r = normalize_edge(a_type, a_id, b_type, b_id)  # type: ignore[arg-type]
        assert (r[0], r[1]) <= (r[2], r[3])


class TestNormalizeEdgeEdgeCases:
    """Edge case tests for normalize_edge."""

    def test_empty_ids(self) -> None:
        result = normalize_edge("bioproject", "", "biosample", "")
        assert result == ("bioproject", "", "biosample", "")

    def test_special_characters_in_id(self) -> None:
        result = normalize_edge("bioproject", "PRJ\tDB1", "biosample", "SAM\nD1")
        # Should not crash, just return normalized
        assert len(result) == 4


class TestInitDblinkDb:
    """Tests for init_dblink_db function."""

    def test_creates_db_and_raw_edges_table(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        assert db_path.exists()

        with duckdb.connect(str(db_path)) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            assert ("raw_edges",) in tables

    def test_overwrites_existing_db(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"

        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('test', 'id1', 'test', 'id2')")

        init_dblink_db(test_config)

        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM raw_edges").fetchone()
            assert count is not None
            assert count[0] == 0


class TestWriteEdgesToTsv:
    """Tests for write_edges_to_tsv function."""

    def test_writes_and_normalizes(self, tmp_path: Path) -> None:
        output_path = tmp_path / "edges.tsv"
        edges: list[Edge] = [
            ("biosample", "SAMD1", "bioproject", "PRJDB1"),
        ]
        write_edges_to_tsv(output_path, edges)
        content = output_path.read_text(encoding="utf-8")
        assert content.strip() == "bioproject\tPRJDB1\tbiosample\tSAMD1"

    def test_append_mode(self, tmp_path: Path) -> None:
        output_path = tmp_path / "edges.tsv"
        r1: list[Edge] = [("bioproject", "PRJDB1", "biosample", "SAMD1")]
        r2: list[Edge] = [("bioproject", "PRJDB2", "biosample", "SAMD2")]
        write_edges_to_tsv(output_path, r1, append=False)
        write_edges_to_tsv(output_path, r2, append=True)
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2


class TestLoadEdgesFromTsv:
    """Tests for load_edges_from_tsv function."""

    def test_loads_tsv_to_raw_edges(self, test_config: Config, tmp_path: Path) -> None:
        init_dblink_db(test_config)
        tsv_path = tmp_path / "edges.tsv"
        tsv_path.write_text(
            "bioproject\tPRJDB1\tbiosample\tSAMD1\nbioproject\tPRJDB2\tbiosample\tSAMD2\n",
            encoding="utf-8",
        )
        load_edges_from_tsv(test_config, tsv_path)

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges ORDER BY src_accession").fetchall()
            assert len(rows) == 2


class TestBuildDbxrefTable:
    """Tests for build_dbxref_table function.

    半辺化により、canonical な N 件の edge は最終 ``dbxref`` に 2N 件の半辺行として
    展開される。DISTINCT により ``raw_edges`` 段階での完全重複は dedup される。
    """

    def test_removes_duplicates_and_expands(self, test_config: Config) -> None:
        """重複する canonical edge が dedup され、残った各 edge が半辺 2 行に展開される。"""
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
        build_dbxref_table(test_config)

        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM dbxref").fetchone()
            assert count is not None
            # 2 unique canonical edges × 2 半辺行 = 4
            assert count[0] == 4

    def test_raw_edges_is_dropped(self, test_config: Config) -> None:
        """build_dbxref_table 後、raw_edges は DROP される。"""
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
        build_dbxref_table(test_config)

        with duckdb.connect(str(db_path)) as conn:
            tables = {t[0] for t in conn.execute("SHOW TABLES").fetchall()}
            assert "dbxref" in tables
            assert "raw_edges" not in tables

    def test_half_edge_mirroring(self, test_config: Config) -> None:
        """各 canonical edge が (A→B) と (B→A) の 2 半辺に展開される。"""
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
        build_dbxref_table(test_config)

        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute(
                "SELECT accession_type, accession, linked_type, linked_accession "
                "FROM dbxref ORDER BY accession_type, accession"
            ).fetchall()
        assert rows == [
            ("bioproject", "PRJDB1", "biosample", "SAMD1"),
            ("biosample", "SAMD1", "bioproject", "PRJDB1"),
        ]

    def test_result_is_sorted(self, test_config: Config) -> None:
        """build 後の dbxref は 4 カラムの辞書順でソートされている (非 canonical 入力も許容)。"""
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            # 意図的に非 canonical (sra-run を src にしてある) も混在させる
            conn.execute("INSERT INTO raw_edges VALUES ('sra-run', 'DRR999', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD2')")
        build_dbxref_table(test_config)

        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute(
                "SELECT accession_type, accession, linked_type, linked_accession FROM dbxref"
            ).fetchall()
        assert rows == [
            ("bioproject", "PRJDB1", "biosample", "SAMD1"),
            ("bioproject", "PRJDB1", "biosample", "SAMD2"),
            ("bioproject", "PRJDB2", "biosample", "SAMD2"),
            ("biosample", "SAMD1", "bioproject", "PRJDB1"),
            ("biosample", "SAMD1", "sra-run", "DRR999"),
            ("biosample", "SAMD2", "bioproject", "PRJDB1"),
            ("biosample", "SAMD2", "bioproject", "PRJDB2"),
            ("sra-run", "DRR999", "biosample", "SAMD1"),
        ]

    @settings(deadline=1000)
    @given(
        rows=st.lists(
            st.tuples(
                st_accession_type(),
                st.text(min_size=1, max_size=10, alphabet=st.characters(categories=("L", "N"))),
                st_accession_type(),
                st.text(min_size=1, max_size=10, alphabet=st.characters(categories=("L", "N"))),
            ),
            min_size=1,
            max_size=30,
        ),
    )
    def test_result_is_always_sorted(self, rows: list[tuple[str, str, str, str]]) -> None:
        """PBT: build 後の dbxref は常に 4 カラムの辞書順ソート済み。

        半辺化ルール: 各 ``raw_edges`` 行 ``(src, src_acc, dst, dst_acc)`` は
        ``dbxref`` に 2 半辺 ``(src, src_acc, dst, dst_acc)`` と
        ``(dst, dst_acc, src, src_acc)`` として展開される。
        """
        with tempfile.TemporaryDirectory() as tmp:
            config = Config(result_dir=Path(tmp), const_dir=Path(tmp) / "const")
            init_dblink_db(config)
            db_path = config.const_dir / "dblink" / "dblink.tmp.duckdb"
            with duckdb.connect(str(db_path)) as conn:
                conn.executemany("INSERT INTO raw_edges VALUES (?, ?, ?, ?)", rows)
            build_dbxref_table(config)

            with duckdb.connect(str(db_path)) as conn:
                result = conn.execute(
                    "SELECT accession_type, accession, linked_type, linked_accession FROM dbxref"
                ).fetchall()

        # 期待値: 各 raw row を半辺 2 方向に展開した集合 (DISTINCT 済み) を辞書順ソート
        expected = set()
        for a_type, a_id, b_type, b_id in rows:
            expected.add((a_type, a_id, b_type, b_id))
            expected.add((b_type, b_id, a_type, a_id))
        assert result == sorted(expected)


class TestFinalizeDblinkDb:
    """Tests for finalize_dblink_db function."""

    def test_finalizes_db(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")

        finalize_dblink_db(test_config)
        assert not tmp_db_path.exists()
        final_db_path = test_config.const_dir / "dblink" / "dblink.duckdb"
        assert final_db_path.exists()

        with duckdb.connect(str(final_db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM dbxref").fetchone()
            assert count is not None
            # 1 unique canonical edge × 2 半辺 = 2
            assert count[0] == 2


class TestGetLinkedEntities:
    """Tests for get_linked_entities function."""

    def test_finds_linked(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD2')")
        finalize_dblink_db(test_config)

        results = list(get_linked_entities(test_config, entity_type="bioproject", accession="PRJDB1"))
        assert len(results) == 2
        assert ("biosample", "SAMD1") in results
        assert ("biosample", "SAMD2") in results

    def test_finds_reverse_linked(self, test_config: Config) -> None:
        """半辺化により逆方向 (biosample → bioproject) も単一 WHERE で取れる。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
        finalize_dblink_db(test_config)

        results = list(get_linked_entities(test_config, entity_type="biosample", accession="SAMD1"))
        assert len(results) == 1
        assert ("bioproject", "PRJDB1") in results

    def test_returns_empty_for_no_match(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        finalize_dblink_db(test_config)
        results = list(get_linked_entities(test_config, entity_type="bioproject", accession="NONEXISTENT"))
        assert results == []


class TestGetLinkedEntitiesBulk:
    """Tests for get_linked_entities_bulk function."""

    def test_bulk_lookup(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB2', 'gea', 'E-GEAD-1')")
        finalize_dblink_db(test_config)

        results = get_linked_entities_bulk(
            test_config,
            entity_type="bioproject",
            accessions=["PRJDB1", "PRJDB2"],
        )
        assert "PRJDB1" in results
        assert len(results["PRJDB1"]) == 1
        assert "PRJDB2" in results
        assert len(results["PRJDB2"]) == 2

    def test_returns_empty_dict_for_empty_input(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        finalize_dblink_db(test_config)
        results = get_linked_entities_bulk(
            test_config,
            entity_type="bioproject",
            accessions=[],
        )
        assert results == {}


class TestExportEdges:
    """Tests for export_edges function."""

    def test_exports_to_tsv(self, test_config: Config, tmp_path: Path) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB3', 'gea', 'E-GEAD-1')")
        finalize_dblink_db(test_config)

        output_path = tmp_path / "bp_bs.tsv"
        export_edges(test_config, output_path, type_a="bioproject", type_b="biosample")
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        # 半辺化 dbxref では (accession_type='bioproject', linked_type='biosample') の
        # 一方向 WHERE で canonical 1 edge = 1 行が取れる
        assert len(lines) == 2

    def test_export_reverse_direction(self, test_config: Config, tmp_path: Path) -> None:
        """逆方向 (biosample, bioproject) でも独立に 1 edge = 1 行で取れる (他方の半辺)。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO raw_edges VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
        finalize_dblink_db(test_config)

        output_path = tmp_path / "bs_bp.tsv"
        export_edges(test_config, output_path, type_a="biosample", type_b="bioproject")
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_empty_result(self, test_config: Config, tmp_path: Path) -> None:
        init_dblink_db(test_config)
        finalize_dblink_db(test_config)
        output_path = tmp_path / "empty.tsv"
        export_edges(test_config, output_path, type_a="bioproject", type_b="biosample")
        assert output_path.read_text(encoding="utf-8") == ""


class TestUmbrellaDb:
    """Tests for umbrella DB operations."""

    def test_init_and_finalize(self, test_config: Config) -> None:
        """init → finalize で DB ファイルが作成される。"""
        init_umbrella_db(test_config)
        tmp_path = test_config.const_dir / "dblink" / "umbrella.tmp.duckdb"
        assert tmp_path.exists()

        finalize_umbrella_db(test_config)
        assert not tmp_path.exists()
        final_path = test_config.const_dir / "dblink" / "umbrella.duckdb"
        assert final_path.exists()

    def test_save_and_get_parent_child_maps(self, test_config: Config) -> None:
        """save → finalize → get で親子マップを取得できる。"""
        with run_logger(config=test_config):
            init_umbrella_db(test_config)
            save_umbrella_relations(test_config, {("PRJDB999", "PRJDB100"), ("PRJDB999", "PRJDB200")})
            finalize_umbrella_db(test_config)

        parent_map, child_map = get_umbrella_parent_child_maps(test_config, ["PRJDB100", "PRJDB200", "PRJDB999"])

        # PRJDB100 の親は PRJDB999
        assert "PRJDB100" in parent_map
        assert parent_map["PRJDB100"] == ["PRJDB999"]
        # PRJDB999 の子は PRJDB100 と PRJDB200
        assert "PRJDB999" in child_map
        assert sorted(child_map["PRJDB999"]) == ["PRJDB100", "PRJDB200"]

    def test_dag_multiple_parents(self, test_config: Config) -> None:
        """1 つの child が複数の parent を持つ DAG 構造。"""
        with run_logger(config=test_config):
            init_umbrella_db(test_config)
            save_umbrella_relations(test_config, {("PRJDB001", "PRJDB100"), ("PRJDB002", "PRJDB100")})
            finalize_umbrella_db(test_config)

        parent_map, _ = get_umbrella_parent_child_maps(test_config, ["PRJDB100"])
        assert "PRJDB100" in parent_map
        assert sorted(parent_map["PRJDB100"]) == ["PRJDB001", "PRJDB002"]

    def test_empty_accessions(self, test_config: Config) -> None:
        """空の accessions で空 dict を返す。"""
        init_umbrella_db(test_config)
        finalize_umbrella_db(test_config)

        parent_map, child_map = get_umbrella_parent_child_maps(test_config, [])
        assert parent_map == {}
        assert child_map == {}

    def test_no_db_file(self, test_config: Config) -> None:
        """DB ファイルが存在しない場合は空 dict を返す。"""
        parent_map, child_map = get_umbrella_parent_child_maps(test_config, ["PRJDB100"])
        assert parent_map == {}
        assert child_map == {}

    def test_deduplication(self, test_config: Config) -> None:
        """重複する関連が finalize 時に重複排除される。"""
        with run_logger(config=test_config):
            init_umbrella_db(test_config)
            save_umbrella_relations(test_config, {("PRJDB999", "PRJDB100")})
            save_umbrella_relations(test_config, {("PRJDB999", "PRJDB100")})
            finalize_umbrella_db(test_config)

        _, child_map = get_umbrella_parent_child_maps(test_config, ["PRJDB999"])
        assert child_map["PRJDB999"] == ["PRJDB100"]
