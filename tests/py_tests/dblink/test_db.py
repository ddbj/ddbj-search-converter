"""Tests for ddbj_search_converter.dblink.db module."""
from pathlib import Path
from typing import Generator, List

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (AccessionType, IdPairs, Relation,
                                             create_relation_indexes,
                                             deduplicate_relations,
                                             export_relations,
                                             finalize_relation_db,
                                             get_related_entities,
                                             get_related_entities_bulk,
                                             get_tmp_dir, init_dblink_db,
                                             load_relations_from_tsv,
                                             load_to_db, normalize_edge,
                                             write_relations_to_tsv)
from ddbj_search_converter.logging.logger import _ctx, init_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestNormalizeEdge:
    """Tests for normalize_edge function."""

    def test_already_sorted_order(self) -> None:
        """既にソート済みの順序はそのまま返す。"""
        result = normalize_edge("bioproject", "PRJDB1", "biosample", "SAMD1")
        assert result == ("bioproject", "PRJDB1", "biosample", "SAMD1")

    def test_reverse_order_gets_normalized(self) -> None:
        """逆順の場合はソートして正規化される。"""
        result = normalize_edge("biosample", "SAMD1", "bioproject", "PRJDB1")
        assert result == ("bioproject", "PRJDB1", "biosample", "SAMD1")

    def test_same_type_different_id(self) -> None:
        """同じタイプでID違いの場合、ID順でソート。"""
        result = normalize_edge("bioproject", "PRJDB2", "bioproject", "PRJDB1")
        assert result == ("bioproject", "PRJDB1", "bioproject", "PRJDB2")

    def test_same_type_already_sorted(self) -> None:
        """同じタイプで既にソート済み。"""
        result = normalize_edge("bioproject", "PRJDB1", "bioproject", "PRJDB2")
        assert result == ("bioproject", "PRJDB1", "bioproject", "PRJDB2")

    def test_identical_pair(self) -> None:
        """完全に同一のペアはそのまま返す。"""
        result = normalize_edge("bioproject", "PRJDB1", "bioproject", "PRJDB1")
        assert result == ("bioproject", "PRJDB1", "bioproject", "PRJDB1")

    @pytest.mark.parametrize(
        "a_type,a_id,b_type,b_id,expected",
        [
            ("gea", "E-GEAD-1", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "gea", "E-GEAD-1")),
            ("jga-study", "JGAS1", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "jga-study", "JGAS1")),
            ("metabobank", "MTBKS1", "biosample", "SAMD1", ("biosample", "SAMD1", "metabobank", "MTBKS1")),
            ("umbrella-bioproject", "PRJDB1", "bioproject", "PRJDB2", ("bioproject", "PRJDB2", "umbrella-bioproject", "PRJDB1")),
            ("taxonomy", "9606", "biosample", "SAMD1", ("biosample", "SAMD1", "taxonomy", "9606")),
            ("pubmed-id", "12345", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "pubmed-id", "12345")),
            ("insdc-assembly", "GCA_001", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "insdc-assembly", "GCA_001")),
            ("insdc-master", "BZZZ01", "bioproject", "PRJDB1", ("bioproject", "PRJDB1", "insdc-master", "BZZZ01")),
        ],
    )
    def test_various_accession_types(
        self,
        a_type: AccessionType,
        a_id: str,
        b_type: AccessionType,
        b_id: str,
        expected: Relation,
    ) -> None:
        """各種AccessionTypeの組み合わせテスト。"""
        result = normalize_edge(a_type, a_id, b_type, b_id)
        assert result == expected


class TestInitDblinkDb:
    """Tests for init_dblink_db function."""

    def test_creates_db_and_table(self, test_config: Config) -> None:
        """DBファイルとrelationテーブルが作成される。"""
        init_dblink_db(test_config)

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        assert db_path.exists()

        with duckdb.connect(str(db_path)) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            assert ("relation",) in tables

            columns = conn.execute("DESCRIBE relation").fetchall()
            column_names = [col[0] for col in columns]
            assert "src_type" in column_names
            assert "src_accession" in column_names
            assert "dst_type" in column_names
            assert "dst_accession" in column_names

    def test_overwrites_existing_db(self, test_config: Config) -> None:
        """既存のDBファイルがある場合は上書きする。"""
        init_dblink_db(test_config)
        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"

        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('test', 'id1', 'test', 'id2')")
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 1

        init_dblink_db(test_config)

        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 0


class TestGetTmpDir:
    """Tests for get_tmp_dir function."""

    def test_creates_tmp_dir(self, test_config: Config) -> None:
        """一時ディレクトリが作成される。"""
        tmp_dir = get_tmp_dir(test_config)
        assert tmp_dir.exists()
        assert tmp_dir.is_dir()
        assert "dblink" in str(tmp_dir)
        assert "tmp" in str(tmp_dir)


class TestWriteRelationsToTsv:
    """Tests for write_relations_to_tsv function."""

    def test_writes_relations(self, tmp_path: Path) -> None:
        """relationsをTSVファイルに書き出す。"""
        output_path = tmp_path / "relations.tsv"
        relations: List[Relation] = [
            ("bioproject", "PRJDB1", "biosample", "SAMD1"),
            ("bioproject", "PRJDB2", "biosample", "SAMD2"),
        ]

        write_relations_to_tsv(output_path, relations)

        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert len(lines) == 2
        assert lines[0] == "bioproject\tPRJDB1\tbiosample\tSAMD1"
        assert lines[1] == "bioproject\tPRJDB2\tbiosample\tSAMD2"

    def test_normalizes_on_write(self, tmp_path: Path) -> None:
        """書き込み時に正規化される。"""
        output_path = tmp_path / "relations.tsv"
        relations: List[Relation] = [
            ("biosample", "SAMD1", "bioproject", "PRJDB1"),
        ]

        write_relations_to_tsv(output_path, relations)

        content = output_path.read_text(encoding="utf-8")
        assert content.strip() == "bioproject\tPRJDB1\tbiosample\tSAMD1"

    def test_append_mode(self, tmp_path: Path) -> None:
        """appendモードで追記する。"""
        output_path = tmp_path / "relations.tsv"
        relations1: List[Relation] = [("bioproject", "PRJDB1", "biosample", "SAMD1")]
        relations2: List[Relation] = [("bioproject", "PRJDB2", "biosample", "SAMD2")]

        write_relations_to_tsv(output_path, relations1, append=False)
        write_relations_to_tsv(output_path, relations2, append=True)

        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        """親ディレクトリが存在しない場合は作成する。"""
        output_path = tmp_path / "nested" / "dir" / "relations.tsv"
        relations: List[Relation] = [("bioproject", "PRJDB1", "biosample", "SAMD1")]

        write_relations_to_tsv(output_path, relations)

        assert output_path.exists()


class TestLoadRelationsFromTsv:
    """Tests for load_relations_from_tsv function."""

    def test_loads_tsv_to_db(self, test_config: Config, tmp_path: Path) -> None:
        """TSVファイルからDBにロードする。"""
        init_dblink_db(test_config)

        tsv_path = tmp_path / "relations.tsv"
        tsv_content = "bioproject\tPRJDB1\tbiosample\tSAMD1\nbioproject\tPRJDB2\tbiosample\tSAMD2\n"
        tsv_path.write_text(tsv_content, encoding="utf-8")

        load_relations_from_tsv(test_config, tsv_path)

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM relation ORDER BY src_accession").fetchall()
            assert len(rows) == 2
            assert rows[0] == ("bioproject", "PRJDB1", "biosample", "SAMD1")
            assert rows[1] == ("bioproject", "PRJDB2", "biosample", "SAMD2")


class TestLoadToDb:
    """Tests for load_to_db function."""

    def test_loads_id_pairs(self, test_config: Config, clean_ctx: None) -> None:
        """IdPairsをDBにロードする。"""
        init_logger(run_name="test_loads_id_pairs", config=test_config)
        init_dblink_db(test_config)

        id_pairs: IdPairs = {("PRJDB1", "SAMD1"), ("PRJDB2", "SAMD2")}
        load_to_db(test_config, id_pairs, "bioproject", "biosample")

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 2

    def test_creates_tsv_file(self, test_config: Config, clean_ctx: None) -> None:
        """中間TSVファイルが作成される。"""
        init_logger(run_name="test_creates_tsv_file", config=test_config)
        init_dblink_db(test_config)

        id_pairs: IdPairs = {("PRJDB1", "SAMD1")}
        load_to_db(test_config, id_pairs, "bioproject", "biosample")

        tmp_dir = get_tmp_dir(test_config)
        tsv_files = list(tmp_dir.glob("*.tsv"))
        assert len(tsv_files) > 0


class TestDeduplicateRelations:
    """Tests for deduplicate_relations function."""

    def test_removes_duplicates(self, test_config: Config) -> None:
        """重複エントリを除去する。"""
        init_dblink_db(test_config)

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")
            conn.execute("INSERT INTO relation VALUES ('bp', 'P2', 'bs', 'S2')")

        deduplicate_relations(test_config)

        with duckdb.connect(str(db_path)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM relation").fetchone()
            assert count is not None
            assert count[0] == 2


class TestCreateRelationIndexes:
    """Tests for create_relation_indexes function."""

    def test_creates_indexes(self, test_config: Config) -> None:
        """インデックスが作成される。"""
        init_dblink_db(test_config)

        db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bp', 'P1', 'bs', 'S1')")

        deduplicate_relations(test_config)
        create_relation_indexes(test_config)

        with duckdb.connect(str(db_path)) as conn:
            indexes = conn.execute("""
                SELECT index_name FROM duckdb_indexes()
                WHERE table_name = 'relation'
            """).fetchall()
            index_names = [idx[0] for idx in indexes]
            assert "idx_relation_unique" in index_names
            assert "idx_relation_src" in index_names
            assert "idx_relation_dst" in index_names


class TestFinalizeRelationDb:
    """Tests for finalize_relation_db function."""

    def test_finalizes_db(self, test_config: Config) -> None:
        """DBをfinalizeする (重複除去 + インデックス + ファイル移動)。"""
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
        """関連エンティティを取得する。"""
        init_dblink_db(test_config)

        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD3')")

        finalize_relation_db(test_config)

        results = list(get_related_entities(test_config, entity_type="bioproject", accession="PRJDB1"))
        assert len(results) == 2
        assert ("biosample", "SAMD1") in results
        assert ("biosample", "SAMD2") in results

    def test_finds_reverse_relations(self, test_config: Config) -> None:
        """逆方向の関連も取得する。"""
        init_dblink_db(test_config)

        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")

        finalize_relation_db(test_config)

        results = list(get_related_entities(test_config, entity_type="biosample", accession="SAMD1"))
        assert len(results) == 1
        assert ("bioproject", "PRJDB1") in results

    def test_returns_empty_for_no_match(self, test_config: Config) -> None:
        """マッチしない場合は空を返す。"""
        init_dblink_db(test_config)
        finalize_relation_db(test_config)

        results = list(get_related_entities(test_config, entity_type="bioproject", accession="NONEXISTENT"))
        assert results == []


class TestGetRelatedEntitiesBulk:
    """Tests for get_related_entities_bulk function."""

    def test_bulk_lookup(self, test_config: Config) -> None:
        """複数アクセッションの一括検索。"""
        init_dblink_db(test_config)

        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'gea', 'E-GEAD-1')")

        finalize_relation_db(test_config)

        results = get_related_entities_bulk(
            test_config,
            entity_type="bioproject",
            accessions=["PRJDB1", "PRJDB2"],
        )

        assert "PRJDB1" in results
        assert len(results["PRJDB1"]) == 1
        assert ("biosample", "SAMD1") in results["PRJDB1"]

        assert "PRJDB2" in results
        assert len(results["PRJDB2"]) == 2

    def test_returns_empty_dict_for_empty_input(self, test_config: Config) -> None:
        """空リストの場合は空dictを返す。"""
        init_dblink_db(test_config)
        finalize_relation_db(test_config)

        results = get_related_entities_bulk(
            test_config,
            entity_type="bioproject",
            accessions=[],
        )

        assert results == {}


class TestExportRelations:
    """Tests for export_relations function."""

    def test_exports_to_tsv(self, test_config: Config, tmp_path: Path) -> None:
        """指定タイプペアの関連をTSVに出力する。"""
        init_dblink_db(test_config)

        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB2', 'biosample', 'SAMD2')")
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB3', 'gea', 'E-GEAD-1')")

        finalize_relation_db(test_config)

        output_path = tmp_path / "bp_bs.tsv"
        export_relations(test_config, output_path, type_a="bioproject", type_b="biosample")

        assert output_path.exists()
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert "PRJDB1\tSAMD1" in lines
        assert "PRJDB2\tSAMD2" in lines

    def test_exports_reverse_order(self, test_config: Config, tmp_path: Path) -> None:
        """逆順のタイプ指定でも正しく出力する。"""
        init_dblink_db(test_config)

        tmp_db_path = test_config.const_dir / "dblink" / "dblink.tmp.duckdb"
        with duckdb.connect(str(tmp_db_path)) as conn:
            conn.execute("INSERT INTO relation VALUES ('bioproject', 'PRJDB1', 'biosample', 'SAMD1')")

        finalize_relation_db(test_config)

        output_path = tmp_path / "bs_bp.tsv"
        export_relations(test_config, output_path, type_a="biosample", type_b="bioproject")

        content = output_path.read_text(encoding="utf-8").strip()
        assert content == "SAMD1\tPRJDB1"

    def test_empty_result(self, test_config: Config, tmp_path: Path) -> None:
        """マッチなしの場合は空ファイルを出力する。"""
        init_dblink_db(test_config)
        finalize_relation_db(test_config)

        output_path = tmp_path / "empty.tsv"
        export_relations(test_config, output_path, type_a="bioproject", type_b="biosample")

        assert output_path.exists()
        assert output_path.read_text(encoding="utf-8") == ""
