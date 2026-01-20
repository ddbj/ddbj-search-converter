"""Tests for ddbj_search_converter.sra_accessions_tab module."""
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import _ctx, init_logger
from ddbj_search_converter.sra_accessions_tab import (
    build_dra_accessions_db, build_sra_accessions_db, finalize_db,
    find_latest_dra_accessions_tab_file, find_latest_sra_accessions_tab_file,
    init_accession_db, iter_bp_bs_relations, load_tsv_to_tmp_db)


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestInitAccessionDb:
    """Tests for init_accession_db function."""

    def test_creates_db_and_table(self, tmp_path: Path) -> None:
        """DBファイルとaccessions テーブルが作成される。"""
        db_path = tmp_path / "sra" / "test.duckdb"
        init_accession_db(db_path)

        assert db_path.exists()

        with duckdb.connect(db_path) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            assert ("accessions",) in tables

            columns = conn.execute("DESCRIBE accessions").fetchall()
            column_names = [col[0] for col in columns]
            assert "Accession" in column_names
            assert "BioSample" in column_names
            assert "BioProject" in column_names
            assert "Study" in column_names
            assert "Experiment" in column_names
            assert "Type" in column_names
            assert "Status" in column_names

    def test_overwrites_existing_db(self, tmp_path: Path) -> None:
        """既存のDBがある場合は削除して新規作成する。"""
        db_path = tmp_path / "test.duckdb"

        init_accession_db(db_path)
        with duckdb.connect(db_path) as conn:
            conn.execute("""
                INSERT INTO accessions (Accession, BioSample, BioProject)
                VALUES ('DRA000001', 'SAMD001', 'PRJDB001')
            """)

        init_accession_db(db_path)

        with duckdb.connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()
            assert count is not None
            assert count[0] == 0


class TestLoadTsvToTmpDb:
    """Tests for load_tsv_to_tmp_db function."""

    def test_loads_tsv(self, tmp_path: Path) -> None:
        """TSVファイルをDBにロードする。"""
        tsv_path = tmp_path / "accessions.tsv"
        tsv_content = """Accession\tBioSample\tBioProject\tStudy\tExperiment\tType\tStatus\tVisibility\tUpdated\tPublished\tReceived
DRA000001\tSAMD00016353\tPRJDA38027\tDRP000001\tDRX000001\tRUN\tlive\tpublic\t2022-09-23\t2010-03-24\t2009-06-20
DRA000002\tSAMD00016354\tPRJDA38028\tDRP000002\tDRX000002\tRUN\tlive\tpublic\t2022-09-24\t2010-03-25\t2009-06-21
"""
        tsv_path.write_text(tsv_content, encoding="utf-8")

        db_path = tmp_path / "test.duckdb"
        init_accession_db(db_path)
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            rows = conn.execute("SELECT * FROM accessions ORDER BY Accession").fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "DRA000001"  # Accession
            assert rows[0][1] == "SAMD00016353"  # BioSample
            assert rows[0][2] == "PRJDA38027"  # BioProject

    def test_handles_null_values(self, tmp_path: Path) -> None:
        """'-' は NULL として扱われる。"""
        tsv_path = tmp_path / "accessions.tsv"
        tsv_content = """Accession\tBioSample\tBioProject\tStudy\tExperiment\tType\tStatus\tVisibility\tUpdated\tPublished\tReceived
DRA000001\t-\tPRJDA38027\t-\tDRX000001\tRUN\tlive\tpublic\t2022-09-23\t2010-03-24\t2009-06-20
"""
        tsv_path.write_text(tsv_content, encoding="utf-8")

        db_path = tmp_path / "test.duckdb"
        init_accession_db(db_path)
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            rows = conn.execute("SELECT BioSample, Study FROM accessions").fetchall()
            assert len(rows) == 1
            assert rows[0][0] is None  # BioSample
            assert rows[0][1] is None  # Study


class TestFinalizeDb:
    """Tests for finalize_db function."""

    def test_creates_indexes_and_moves(self, tmp_path: Path) -> None:
        """インデックスを作成してファイルを移動する。"""
        tmp_db = tmp_path / "tmp.duckdb"
        final_db = tmp_path / "final.duckdb"

        init_accession_db(tmp_db)
        with duckdb.connect(tmp_db) as conn:
            conn.execute("""
                INSERT INTO accessions (Accession, BioSample, BioProject)
                VALUES ('DRA000001', 'SAMD001', 'PRJDB001')
            """)

        finalize_db(tmp_db, final_db)

        assert not tmp_db.exists()
        assert final_db.exists()

        with duckdb.connect(final_db) as conn:
            indexes = conn.execute("""
                SELECT index_name FROM duckdb_indexes()
                WHERE table_name = 'accessions'
            """).fetchall()
            index_names = [idx[0] for idx in indexes]
            assert "idx_bp" in index_names
            assert "idx_bs" in index_names
            assert "idx_acc" in index_names

    def test_overwrites_existing_final(self, tmp_path: Path) -> None:
        """既存のfinal DBがある場合は上書きする。"""
        tmp_db = tmp_path / "tmp.duckdb"
        final_db = tmp_path / "final.duckdb"

        final_db.write_bytes(b"old content")
        init_accession_db(tmp_db)

        finalize_db(tmp_db, final_db)

        assert final_db.exists()
        with duckdb.connect(final_db) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            assert ("accessions",) in tables


class TestIterBpBsRelations:
    """Tests for iter_bp_bs_relations function."""

    def test_iterates_bp_bs_pairs_sra(self, test_config: Config) -> None:
        """SRA DBからBP-BSペアをイテレートする。"""
        sra_dir = test_config.const_dir / "sra"
        sra_dir.mkdir(parents=True, exist_ok=True)
        sra_db = sra_dir / "sra_accessions.duckdb"

        init_accession_db(sra_db)
        with duckdb.connect(sra_db) as conn:
            conn.execute("""
                INSERT INTO accessions (Accession, BioSample, BioProject)
                VALUES
                    ('DRA001', 'SAMD001', 'PRJDB001'),
                    ('DRA002', 'SAMD002', 'PRJDB001'),
                    ('DRA003', 'SAMD003', 'PRJDB002')
            """)

        results = list(iter_bp_bs_relations(test_config, source="sra"))
        assert len(results) == 3
        assert ("PRJDB001", "SAMD001") in results
        assert ("PRJDB001", "SAMD002") in results
        assert ("PRJDB002", "SAMD003") in results

    def test_iterates_bp_bs_pairs_dra(self, test_config: Config) -> None:
        """DRA DBからBP-BSペアをイテレートする。"""
        sra_dir = test_config.const_dir / "sra"
        sra_dir.mkdir(parents=True, exist_ok=True)
        dra_db = sra_dir / "dra_accessions.duckdb"

        init_accession_db(dra_db)
        with duckdb.connect(dra_db) as conn:
            conn.execute("""
                INSERT INTO accessions (Accession, BioSample, BioProject)
                VALUES
                    ('DRR001', 'SAMD100', 'PRJDB100')
            """)

        results = list(iter_bp_bs_relations(test_config, source="dra"))
        assert len(results) == 1
        assert ("PRJDB100", "SAMD100") in results

    def test_excludes_null_values(self, test_config: Config) -> None:
        """NULL値は除外される。"""
        sra_dir = test_config.const_dir / "sra"
        sra_dir.mkdir(parents=True, exist_ok=True)
        sra_db = sra_dir / "sra_accessions.duckdb"

        init_accession_db(sra_db)
        with duckdb.connect(sra_db) as conn:
            conn.execute("""
                INSERT INTO accessions (Accession, BioSample, BioProject)
                VALUES
                    ('DRA001', 'SAMD001', 'PRJDB001'),
                    ('DRA002', NULL, 'PRJDB001'),
                    ('DRA003', 'SAMD003', NULL)
            """)

        results = list(iter_bp_bs_relations(test_config, source="sra"))
        assert len(results) == 1
        assert ("PRJDB001", "SAMD001") in results

    def test_returns_distinct_pairs(self, test_config: Config) -> None:
        """重複するペアは1つにまとめられる。"""
        sra_dir = test_config.const_dir / "sra"
        sra_dir.mkdir(parents=True, exist_ok=True)
        sra_db = sra_dir / "sra_accessions.duckdb"

        init_accession_db(sra_db)
        with duckdb.connect(sra_db) as conn:
            conn.execute("""
                INSERT INTO accessions (Accession, BioSample, BioProject)
                VALUES
                    ('DRA001', 'SAMD001', 'PRJDB001'),
                    ('DRA002', 'SAMD001', 'PRJDB001'),
                    ('DRA003', 'SAMD001', 'PRJDB001')
            """)

        results = list(iter_bp_bs_relations(test_config, source="sra"))
        assert len(results) == 1
        assert ("PRJDB001", "SAMD001") in results


class TestFindLatestSraAccessionsTabFile:
    """Tests for find_latest_sra_accessions_tab_file function."""

    def test_finds_existing_file(self, tmp_path: Path) -> None:
        """存在するファイルを見つける。"""
        from ddbj_search_converter.sra_accessions_tab import TODAY

        date_str = TODAY.strftime("%Y%m%d")
        year = TODAY.strftime("%Y")
        month = TODAY.strftime("%m")

        mock_path = tmp_path / year / month / f"SRA_Accessions.tab.{date_str}"
        mock_path.parent.mkdir(parents=True, exist_ok=True)
        mock_path.write_text("test", encoding="utf-8")

        with patch(
            "ddbj_search_converter.sra_accessions_tab.SRA_ACCESSIONS_BASE_PATH",
            tmp_path,
        ):
            result = find_latest_sra_accessions_tab_file()
            assert result is not None
            assert result == mock_path

    def test_returns_none_when_not_found(self, tmp_path: Path) -> None:
        """ファイルが見つからない場合はNoneを返す。"""
        with patch(
            "ddbj_search_converter.sra_accessions_tab.SRA_ACCESSIONS_BASE_PATH",
            tmp_path,
        ):
            result = find_latest_sra_accessions_tab_file()
            assert result is None


class TestFindLatestDraAccessionsTabFile:
    """Tests for find_latest_dra_accessions_tab_file function."""

    def test_finds_existing_file(self, tmp_path: Path) -> None:
        """存在するファイルを見つける。"""
        from ddbj_search_converter.sra_accessions_tab import TODAY

        date_str = TODAY.strftime("%Y%m%d")

        mock_path = tmp_path / f"{date_str}.DRA_Accessions.tab"
        mock_path.write_text("test", encoding="utf-8")

        with patch(
            "ddbj_search_converter.sra_accessions_tab.DRA_ACCESSIONS_BASE_PATH",
            tmp_path,
        ):
            result = find_latest_dra_accessions_tab_file()
            assert result is not None
            assert result == mock_path

    def test_returns_none_when_not_found(self, tmp_path: Path) -> None:
        """ファイルが見つからない場合はNoneを返す。"""
        with patch(
            "ddbj_search_converter.sra_accessions_tab.DRA_ACCESSIONS_BASE_PATH",
            tmp_path,
        ):
            result = find_latest_dra_accessions_tab_file()
            assert result is None


class TestBuildSraAccessionsDb:
    """Tests for build_sra_accessions_db function."""

    def test_builds_database(self, test_config: Config, tmp_path: Path) -> None:
        """SRA Accessionsデータベースを構築する。"""
        from ddbj_search_converter.sra_accessions_tab import TODAY

        date_str = TODAY.strftime("%Y%m%d")
        year = TODAY.strftime("%Y")
        month = TODAY.strftime("%m")

        tsv_path = tmp_path / "sra" / year / month / f"SRA_Accessions.tab.{date_str}"
        tsv_path.parent.mkdir(parents=True, exist_ok=True)
        tsv_content = """Accession\tBioSample\tBioProject\tStudy\tExperiment\tType\tStatus\tVisibility\tUpdated\tPublished\tReceived
DRA000001\tSAMD001\tPRJDB001\tDRP001\tDRX001\tRUN\tlive\tpublic\t2022-01-01\t2022-01-01\t2022-01-01
"""
        tsv_path.write_text(tsv_content, encoding="utf-8")

        with patch(
            "ddbj_search_converter.sra_accessions_tab.SRA_ACCESSIONS_BASE_PATH",
            tmp_path / "sra",
        ):
            result = build_sra_accessions_db(test_config)

        assert result.exists()
        with duckdb.connect(result) as conn:
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()
            assert count is not None
            assert count[0] == 1

    def test_raises_when_file_not_found(self, test_config: Config, tmp_path: Path) -> None:
        """ファイルが見つからない場合はエラー。"""
        with patch(
            "ddbj_search_converter.sra_accessions_tab.SRA_ACCESSIONS_BASE_PATH",
            tmp_path,
        ):
            with pytest.raises(FileNotFoundError, match="SRA_Accessions.tab not found"):
                build_sra_accessions_db(test_config)


class TestBuildDraAccessionsDb:
    """Tests for build_dra_accessions_db function."""

    def test_builds_database(self, test_config: Config, tmp_path: Path) -> None:
        """DRA Accessionsデータベースを構築する。"""
        from ddbj_search_converter.sra_accessions_tab import TODAY

        date_str = TODAY.strftime("%Y%m%d")

        tsv_path = tmp_path / "dra" / f"{date_str}.DRA_Accessions.tab"
        tsv_path.parent.mkdir(parents=True, exist_ok=True)
        tsv_content = """Accession\tBioSample\tBioProject\tStudy\tExperiment\tType\tStatus\tVisibility\tUpdated\tPublished\tReceived
DRR000001\tSAMD100\tPRJDB100\tDRP100\tDRX100\tRUN\tlive\tpublic\t2022-01-01\t2022-01-01\t2022-01-01
"""
        tsv_path.write_text(tsv_content, encoding="utf-8")

        with patch(
            "ddbj_search_converter.sra_accessions_tab.DRA_ACCESSIONS_BASE_PATH",
            tmp_path / "dra",
        ):
            result = build_dra_accessions_db(test_config)

        assert result.exists()
        with duckdb.connect(result) as conn:
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()
            assert count is not None
            assert count[0] == 1

    def test_raises_when_file_not_found(self, test_config: Config, tmp_path: Path) -> None:
        """ファイルが見つからない場合はエラー。"""
        with patch(
            "ddbj_search_converter.sra_accessions_tab.DRA_ACCESSIONS_BASE_PATH",
            tmp_path,
        ):
            with pytest.raises(FileNotFoundError, match="DRA_Accessions.tab not found"):
                build_dra_accessions_db(test_config)
