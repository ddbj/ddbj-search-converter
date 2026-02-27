"""Tests for ddbj_search_converter.dblink.db module."""
from pathlib import Path
from typing import Generator, List

import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (AccessionType, IdPairs, Relation,
                                             create_relation_indexes,
                                             deduplicate_relations,
                                             export_relations,
                                             finalize_relation_db,
                                             get_related_entities,
                                             get_related_entities_bulk,
                                             get_tmp_dir,
                                             get_umbrella_bioproject_ids,
                                             init_dblink_db,
                                             load_relations_from_tsv,
                                             load_to_db, normalize_edge,
                                             write_relations_to_tsv)
from ddbj_search_converter.logging.logger import _ctx, init_logger

from ..strategies import ALL_ACCESSION_TYPES, st_accession_type


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

    @pytest.mark.parametrize("a_type,a_id,b_type,b_id,expected", [
        ("gea", "E-GEAD-1", "bioproject", "PRJDB1",
         ("bioproject", "PRJDB1", "gea", "E-GEAD-1")),
        ("jga-study", "JGAS1", "bioproject", "PRJDB1",
         ("bioproject", "PRJDB1", "jga-study", "JGAS1")),
        ("umbrella-bioproject", "PRJDB1", "bioproject", "PRJDB2",
         ("bioproject", "PRJDB2", "umbrella-bioproject", "PRJDB1")),
        ("taxonomy", "9606", "biosample", "SAMD1",
         ("biosample", "SAMD1", "taxonomy", "9606")),
        ("insdc-assembly", "GCA_001", "bioproject", "PRJDB1",
         ("bioproject", "PRJDB1", "insdc-assembly", "GCA_001")),
    ])
    def test_various_accession_types(
        self, a_type: AccessionType, a_id: str, b_type: AccessionType, b_id: str,
        expected: Relation,
    ) -> None:
        result = normalize_edge(a_type, a_id, b_type, b_id)
        assert result == expected


class TestNormalizeEdgePBT:
    """Property-based tests for normalize_edge."""

    @given(
        a_type=st_accession_type(), a_id=st.text(min_size=1, max_size=20),
        b_type=st_accession_type(), b_id=st.text(min_size=1, max_size=20),
    )
    def test_commutativity(self, a_type: str, a_id: str, b_type: str, b_id: str) -> None:
        """normalize_edge(a,b) == normalize_edge(b,a): 可換性。"""
        result1 = normalize_edge(a_type, a_id, b_type, b_id)  # type: ignore[arg-type]
        result2 = normalize_edge(b_type, b_id, a_type, a_id)  # type: ignore[arg-type]
        assert result1 == result2

    @given(
        a_type=st_accession_type(), a_id=st.text(min_size=1, max_size=20),
        b_type=st_accession_type(), b_id=st.text(min_size=1, max_size=20),
    )
    def test_idempotency(self, a_type: str, a_id: str, b_type: str, b_id: str) -> None:
        """normalize_edge は冪等。"""
        result1 = normalize_edge(a_type, a_id, b_type, b_id)  # type: ignore[arg-type]
        result2 = normalize_edge(*result1)
        assert result1 == result2

    @given(
        a_type=st_accession_type(), a_id=st.text(min_size=1, max_size=20),
        b_type=st_accession_type(), b_id=st.text(min_size=1, max_size=20),
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

    def test_creates_db_and_table(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        assert db_path.exists()

        with duckdb.connect(str(db_path)) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            assert ("relation",) in tables

    def test_overwrites_existing_db(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"

        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('test', 'id1', 'test', 'id2')")

        init_dblink_db(test_config)

        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 0


class TestWriteRelationsToTsv:
    """Tests for write_relations_to_tsv function."""

    def test_writes_and_normalizes(self, tmp_path: Path) -> None:
        output_path = tmp_path / "relations.tsv"
        relations: List[Relation] = [
            ("biosample", "SAMD1", "bioproject", "PRJDB1"),
        ]
        write_relations_to_tsv(output_path, relations)
        content = output_path.read_text(encoding="utf-8")
        assert content.strip() == "bioproject\tPRJDB1\tbiosample\tSAMD1"

    def test_append_mode(self, tmp_path: Path) -> None:
        output_path = tmp_path / "relations.tsv"
        r1: List[Relation] = [("bioproject", "PRJDB1", "biosample", "SAMD1")]
        r2: List[Relation] = [("bioproject", "PRJDB2", "biosample", "SAMD2")]
        write_relations_to_tsv(output_path, r1, append=False)
        write_relations_to_tsv(output_path, r2, append=True)
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2


class TestLoadRelationsFromTsv:
    """Tests for load_relations_from_tsv function."""

    def test_loads_tsv_to_db(self, test_config: Config, tmp_path: Path) -> None:
        init_dblink_db(test_config)
        tsv_path = tmp_path / "relations.tsv"
        tsv_path.write_text("bioproject\tPRJDB1\tbiosample\tSAMD1\nbioproject\tPRJDB2\tbiosample\tSAMD2\n", encoding="utf-8")
        load_relations_from_tsv(test_config, tsv_path)

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM relation ORDER BY src_accession").fetchall()
            assert len(rows) == 2


class TestDeduplicateRelations:
    """Tests for deduplicate_relations function."""

    def test_removes_duplicates(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")
            conn.execute("INSERT INTO relation VALUES ('bp', 'P2', 'bs', 'S2')")
        deduplicate_relations(test_config)

        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 2


class TestFinalizeRelationDb:
    """Tests for finalize_relation_db function."""

    def test_finalizes_db(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")

        finalize_relation_db(test_config)
        assert not tmp_db_path.exists()
        final_db_path = test_config.const_dir / "dblink" / "dblink.duckdb"
        assert final_db_path.exists()

        with duckdb.connect(str(final_db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 1


class TestGetRelatedEntities:
    """Tests for get_related_entities function."""

    def test_finds_relations(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD2')")
        finalize_relation_db(test_config)

        results = list(get_related_entities(test_config, entity_type="bioproject", accession="PRJDB1"))
        assert len(results) == 2
        assert ("biosample", "SAMD1") in results
        assert ("biosample", "SAMD2") in results

    def test_finds_reverse_relations(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
        finalize_relation_db(test_config)

        results = list(get_related_entities(test_config, entity_type="biosample", accession="SAMD1"))
        assert len(results) == 1
        assert ("bioproject", "PRJDB1") in results

    def test_returns_empty_for_no_match(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        finalize_relation_db(test_config)
        results = list(get_related_entities(test_config, entity_type="bioproject", accession="NONEXISTENT"))
        assert results == []


class TestGetRelatedEntitiesBulk:
    """Tests for get_related_entities_bulk function."""

    def test_bulk_lookup(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'gea', 'E-GEAD-1')")
        finalize_relation_db(test_config)

        results = get_related_entities_bulk(
            test_config, entity_type="bioproject", accessions=["PRJDB1", "PRJDB2"],
        )
        assert "PRJDB1" in results
        assert len(results["PRJDB1"]) == 1
        assert "PRJDB2" in results
        assert len(results["PRJDB2"]) == 2

    def test_returns_empty_dict_for_empty_input(self, test_config: Config) -> None:
        init_dblink_db(test_config)
        finalize_relation_db(test_config)
        results = get_related_entities_bulk(
            test_config, entity_type="bioproject", accessions=[],
        )
        assert results == {}


class TestExportRelations:
    """Tests for export_relations function."""

    def test_exports_to_tsv(self, test_config: Config, tmp_path: Path) -> None:
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB3', 'gea', 'E-GEAD-1')")
        finalize_relation_db(test_config)

        output_path = tmp_path / "bp_bs.tsv"
        export_relations(test_config, output_path, type_a="bioproject", type_b="biosample")
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_empty_result(self, test_config: Config, tmp_path: Path) -> None:
        init_dblink_db(test_config)
        finalize_relation_db(test_config)
        output_path = tmp_path / "empty.tsv"
        export_relations(test_config, output_path, type_a="bioproject", type_b="biosample")
        assert output_path.read_text(encoding="utf-8") == ""


class TestGetUmbrellaBioprojectIds:
    """Tests for get_umbrella_bioproject_ids function."""

    def test_returns_umbrella_ids_from_dst(self, test_config: Config) -> None:
        """dst_type が umbrella-bioproject の場合に dst_accession を返す。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB100', 'umbrella-bioproject', 'PRJDB999')"
            )
        finalize_relation_db(test_config)

        result = get_umbrella_bioproject_ids(test_config)
        assert result == {"PRJDB999"}

    def test_returns_umbrella_ids_from_src(self, test_config: Config) -> None:
        """src_type が umbrella-bioproject の場合に src_accession を返す。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            # normalize_edge で src/dst 反転する場合をシミュレート
            conn.execute(
                "INSERT INTO relation VALUES "
                "('umbrella-bioproject', 'PRJDB888', 'umbrella-bioproject', 'PRJDB999')"
            )
        finalize_relation_db(test_config)

        result = get_umbrella_bioproject_ids(test_config)
        assert "PRJDB888" in result
        assert "PRJDB999" in result

    def test_returns_empty_set_when_no_umbrella(self, test_config: Config) -> None:
        """umbrella-bioproject 関連がない場合は空セットを返す。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB1', 'biosample', 'SAMD1')"
            )
        finalize_relation_db(test_config)

        result = get_umbrella_bioproject_ids(test_config)
        assert result == set()

    def test_deduplicates_ids(self, test_config: Config) -> None:
        """同じ umbrella ID が複数行に出現しても重複なしで返す。"""
        init_dblink_db(test_config)
        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB100', 'umbrella-bioproject', 'PRJDB999')"
            )
            conn.execute(
                "INSERT INTO relation VALUES "
                "('bioproject', 'PRJDB200', 'umbrella-bioproject', 'PRJDB999')"
            )
        finalize_relation_db(test_config)

        result = get_umbrella_bioproject_ids(test_config)
        assert result == {"PRJDB999"}
