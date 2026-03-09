"""Tests for ddbj_search_converter.sra.dra_file_index module."""

import tempfile
from pathlib import Path

import duckdb
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra.dra_file_index import (
    dra_file_index_exists,
    get_dra_file_index_db_path,
    query_fastq_dirs_bulk,
    query_sra_files_bulk,
)


def _create_test_db(
    db_path: Path,
    fastq_rows: list[tuple[str, str]] | None = None,
    sra_rows: list[str] | None = None,
) -> None:
    """テスト用の DRA ファイルインデックス DB を作成するヘルパー。"""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE dra_fastq_dir (submission TEXT NOT NULL, experiment TEXT NOT NULL)")
        conn.execute("CREATE TABLE dra_sra_file (run TEXT NOT NULL)")
        conn.execute("CREATE INDEX idx_dra_fastq_sub ON dra_fastq_dir(submission)")
        conn.execute("CREATE INDEX idx_dra_sra_run ON dra_sra_file(run)")

        if fastq_rows:
            conn.executemany("INSERT INTO dra_fastq_dir VALUES (?, ?)", fastq_rows)
        if sra_rows:
            conn.executemany("INSERT INTO dra_sra_file VALUES (?)", [(r,) for r in sra_rows])


class TestDbPathAndExists:
    """DB パスと存在確認のテスト。"""

    def test_get_dra_file_index_db_path(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        path = get_dra_file_index_db_path(config)

        assert path.parent == tmp_path.joinpath("sra_tar")
        assert path.name == "dra_file_index.duckdb"

    def test_dra_file_index_exists_false_when_no_db(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)

        assert dra_file_index_exists(config) is False

    def test_dra_file_index_exists_true_when_db_present(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        assert dra_file_index_exists(config) is True


class TestTableSchema:
    """テーブル作成とスキーマ検証のテスト。"""

    def test_tables_exist(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        with duckdb.connect(str(db_path), read_only=True) as conn:
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = {t[0] for t in tables}

        assert "dra_fastq_dir" in table_names
        assert "dra_sra_file" in table_names

    def test_fastq_dir_columns(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        with duckdb.connect(str(db_path), read_only=True) as conn:
            cols = conn.execute("DESCRIBE dra_fastq_dir").fetchall()
            col_names = [c[0] for c in cols]

        assert "submission" in col_names
        assert "experiment" in col_names

    def test_sra_file_columns(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        with duckdb.connect(str(db_path), read_only=True) as conn:
            cols = conn.execute("DESCRIBE dra_sra_file").fetchall()
            col_names = [c[0] for c in cols]

        assert "run" in col_names


class TestQueryFastqDirsBulk:
    """query_fastq_dirs_bulk のテスト。"""

    def test_normal_query(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(
            db_path,
            fastq_rows=[
                ("DRA000001", "DRX000001"),
                ("DRA000001", "DRX000002"),
                ("DRA000002", "DRX000003"),
            ],
        )

        result = query_fastq_dirs_bulk(config, ["DRA000001", "DRA000002"])

        assert result == {
            "DRA000001": {"DRX000001", "DRX000002"},
            "DRA000002": {"DRX000003"},
        }

    def test_empty_input(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        result = query_fastq_dirs_bulk(config, [])

        assert result == {}

    def test_no_matching_submissions(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(
            db_path,
            fastq_rows=[("DRA000001", "DRX000001")],
        )

        result = query_fastq_dirs_bulk(config, ["DRA999999"])

        assert result == {}

    def test_graceful_degradation_no_db(self, tmp_path: Path) -> None:
        """DB が存在しない場合、空の dict を返す。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)

        result = query_fastq_dirs_bulk(config, ["DRA000001"])

        assert result == {}


class TestQuerySraFilesBulk:
    """query_sra_files_bulk のテスト。"""

    def test_normal_query(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path, sra_rows=["DRR000001", "DRR000002", "DRR000003"])

        result = query_sra_files_bulk(config, ["DRR000001", "DRR000003", "DRR999999"])

        assert result == {"DRR000001", "DRR000003"}

    def test_empty_input(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        result = query_sra_files_bulk(config, [])

        assert result == set()

    def test_no_matching_runs(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path, sra_rows=["DRR000001"])

        result = query_sra_files_bulk(config, ["DRR999999"])

        assert result == set()

    def test_graceful_degradation_no_db(self, tmp_path: Path) -> None:
        """DB が存在しない場合、空の set を返す。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)

        result = query_sra_files_bulk(config, ["DRR000001"])

        assert result == set()


# hypothesis 用の accession 戦略
_dra_sub_st = st.from_regex(r"DRA[0-9]{6}", fullmatch=True)
_drx_exp_st = st.from_regex(r"DRX[0-9]{6}", fullmatch=True)
_drr_run_st = st.from_regex(r"DRR[0-9]{6}", fullmatch=True)


class TestDraFileIndexPBT:
    """Property-based tests for DRA file index queries."""

    @given(
        submissions=st.lists(_dra_sub_st, min_size=1, max_size=5, unique=True),
        experiments=st.lists(_drx_exp_st, min_size=1, max_size=3, unique=True),
    )
    @settings(max_examples=30)
    def test_fastq_roundtrip(self, submissions: list[str], experiments: list[str]) -> None:
        """挿入した (submission, experiment) ペアがクエリで取得できる。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            config = Config(result_dir=tmp_path, const_dir=tmp_path)
            db_path = get_dra_file_index_db_path(config)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            rows = [(sub, exp) for sub in submissions for exp in experiments]
            _create_test_db(db_path, fastq_rows=rows)

            result = query_fastq_dirs_bulk(config, submissions)

            for sub in submissions:
                assert sub in result
                assert result[sub] == set(experiments)

    @given(runs=st.lists(_drr_run_st, min_size=1, max_size=10, unique=True))
    @settings(max_examples=30)
    def test_sra_roundtrip(self, runs: list[str]) -> None:
        """挿入した run がクエリで取得できる。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            config = Config(result_dir=tmp_path, const_dir=tmp_path)
            db_path = get_dra_file_index_db_path(config)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            _create_test_db(db_path, sra_rows=runs)

            result = query_sra_files_bulk(config, runs)

            assert result == set(runs)
