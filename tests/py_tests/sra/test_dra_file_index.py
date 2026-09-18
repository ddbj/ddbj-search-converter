"""Tests for ddbj_search_converter.sra.dra_file_index module."""

import tempfile
import time
from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra import dra_file_index
from ddbj_search_converter.sra.dra_file_index import (
    build_dra_file_index,
    dra_file_index_exists,
    get_dra_file_index_db_path,
    query_analysis_dirs_bulk,
    query_fastq_dirs_bulk,
    query_sra_files_bulk,
)


def _create_test_db(
    db_path: Path,
    fastq_rows: list[tuple[str, str]] | None = None,
    analysis_rows: list[tuple[str, str]] | None = None,
    sra_rows: list[str] | None = None,
) -> None:
    """テスト用の DRA ファイルインデックス DB を作成するヘルパー。"""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE dra_fastq_dir (submission TEXT NOT NULL, experiment TEXT NOT NULL)")
        conn.execute("CREATE TABLE dra_fastq_analysis_dir (submission TEXT NOT NULL, analysis TEXT NOT NULL)")
        conn.execute("CREATE TABLE dra_sra_file (run TEXT NOT NULL)")
        conn.execute("CREATE INDEX idx_dra_fastq_sub ON dra_fastq_dir(submission)")
        conn.execute("CREATE INDEX idx_dra_fastq_analysis_sub ON dra_fastq_analysis_dir(submission)")
        conn.execute("CREATE INDEX idx_dra_sra_run ON dra_sra_file(run)")

        if fastq_rows:
            conn.executemany("INSERT INTO dra_fastq_dir VALUES (?, ?)", fastq_rows)
        if analysis_rows:
            conn.executemany("INSERT INTO dra_fastq_analysis_dir VALUES (?, ?)", analysis_rows)
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
        assert "dra_fastq_analysis_dir" in table_names
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

    def test_fastq_analysis_dir_columns(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        with duckdb.connect(str(db_path), read_only=True) as conn:
            cols = conn.execute("DESCRIBE dra_fastq_analysis_dir").fetchall()
            col_names = [c[0] for c in cols]

        assert "submission" in col_names
        assert "analysis" in col_names

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


class TestQueryAnalysisDirsBulk:
    """query_analysis_dirs_bulk のテスト。"""

    def test_normal_query(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(
            db_path,
            analysis_rows=[
                ("DRA016427", "DRZ138937"),
                ("DRA016427", "DRZ138938"),
                ("DRA000002", "DRZ000003"),
            ],
        )

        result = query_analysis_dirs_bulk(config, ["DRA016427", "DRA000002"])

        assert result == {
            "DRA016427": {"DRZ138937", "DRZ138938"},
            "DRA000002": {"DRZ000003"},
        }

    def test_empty_input(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(db_path)

        result = query_analysis_dirs_bulk(config, [])

        assert result == {}

    def test_no_matching_submissions(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(
            db_path,
            analysis_rows=[("DRA016427", "DRZ138937")],
        )

        result = query_analysis_dirs_bulk(config, ["DRA999999"])

        assert result == {}

    def test_graceful_degradation_no_db(self, tmp_path: Path) -> None:
        """DB が存在しない場合、空の dict を返す。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)

        result = query_analysis_dirs_bulk(config, ["DRA016427"])

        assert result == {}

    def test_isolated_from_fastq_table(self, tmp_path: Path) -> None:
        """experiment (DRX) を fastq テーブルに、analysis (DRZ) を analysis テーブルに分離して保持する。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        db_path = get_dra_file_index_db_path(config)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _create_test_db(
            db_path,
            fastq_rows=[("DRA016427", "DRX138937")],
            analysis_rows=[("DRA016427", "DRZ138937")],
        )

        fastq_result = query_fastq_dirs_bulk(config, ["DRA016427"])
        analysis_result = query_analysis_dirs_bulk(config, ["DRA016427"])

        assert fastq_result == {"DRA016427": {"DRX138937"}}
        assert analysis_result == {"DRA016427": {"DRZ138937"}}


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
_drz_ana_st = st.from_regex(r"DRZ[0-9]{6}", fullmatch=True)


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

    @given(
        submissions=st.lists(_dra_sub_st, min_size=1, max_size=5, unique=True),
        analyses=st.lists(_drz_ana_st, min_size=1, max_size=3, unique=True),
    )
    @settings(max_examples=30)
    def test_analysis_roundtrip(self, submissions: list[str], analyses: list[str]) -> None:
        """挿入した (submission, analysis) ペアが query_analysis_dirs_bulk で取得できる。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            config = Config(result_dir=tmp_path, const_dir=tmp_path)
            db_path = get_dra_file_index_db_path(config)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            rows = [(sub, ana) for sub in submissions for ana in analyses]
            _create_test_db(db_path, analysis_rows=rows)

            result = query_analysis_dirs_bulk(config, submissions)

            for sub in submissions:
                assert sub in result
                assert result[sub] == set(analyses)

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


def _read_index(config: Config) -> tuple[set[tuple[str, str]], set[tuple[str, str]], set[str]]:
    with duckdb.connect(str(get_dra_file_index_db_path(config)), read_only=True) as conn:
        fastq = set(conn.execute("SELECT submission, experiment FROM dra_fastq_dir").fetchall())
        analysis = set(conn.execute("SELECT submission, analysis FROM dra_fastq_analysis_dir").fetchall())
        sra = {row[0] for row in conn.execute("SELECT run FROM dra_sra_file").fetchall()}
    return fastq, analysis, sra


@pytest.mark.usefixtures("with_logger_isolated")
class TestBuildDraFileIndex:
    """FS を走査して index を作る。差し替えるのは対象 submission の取得 (DB) と走査の起点だけ。"""

    def _env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, submissions: list[str]) -> tuple[Config, Path]:
        base = tmp_path / "dra"
        base.mkdir()
        config = Config(result_dir=tmp_path / "result", const_dir=tmp_path / "const")
        monkeypatch.setattr(dra_file_index, "DRA_BASE_PATH", base)
        monkeypatch.setattr(dra_file_index, "iter_all_dra_submissions", lambda _config: iter(submissions))
        return config, base

    def test_only_drx_and_drz_directories_are_indexed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config, base = self._env(tmp_path, monkeypatch, ["DRA000001", "DRA000002", "DRA999999"])
        sub1 = base / "fastq" / "DRA000" / "DRA000001"
        (sub1 / "DRX000001").mkdir(parents=True)
        (sub1 / "DRX000002").mkdir()
        (sub1 / "DRZ000001").mkdir()
        (sub1 / "DRA000001.submission.xml").write_text("<x/>")
        (sub1 / "DRX000009").write_text("a regular file named like an experiment")
        (sub1 / "OTHER").mkdir()
        target = tmp_path / "elsewhere"
        target.mkdir()
        (sub1 / "DRX000003").symlink_to(target, target_is_directory=True)
        (sub1 / "DRX000004").symlink_to(tmp_path / "does-not-exist")
        sub2 = base / "fastq" / "DRA000" / "DRA000002"
        (sub2 / "DRZ000002").mkdir(parents=True)
        # DRA999999 は Accessions にはあるが FS にディレクトリが無い
        sra_dir = base / "sra" / "ByExp" / "sra" / "DRX" / "DRX000" / "DRX000001" / "DRR000001"
        sra_dir.mkdir(parents=True)
        (sra_dir / "DRR000001.sra").write_bytes(b"")
        (sra_dir / "DRR000001.txt").write_bytes(b"")

        build_dra_file_index(config)

        fastq, analysis, sra = _read_index(config)
        assert fastq == {("DRA000001", "DRX000001"), ("DRA000001", "DRX000002"), ("DRA000001", "DRX000003")}
        assert analysis == {("DRA000001", "DRZ000001"), ("DRA000002", "DRZ000002")}
        assert sra == {"DRR000001"}

    def test_sra_files_are_found_at_every_depth_without_following_symlinked_dirs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config, base = self._env(tmp_path, monkeypatch, [])
        drx = base / "sra" / "ByExp" / "sra" / "DRX"
        (drx / "DRX000" / "DRX000001" / "DRR000001").mkdir(parents=True)
        (drx / "DRX000" / "DRX000001" / "DRR000001" / "DRR000001.sra").write_bytes(b"")
        (drx / "DRX000" / "DRX000001" / "DRR000002").mkdir()
        (drx / "DRX000" / "DRX000001" / "DRR000002" / "DRR000002.sra").write_bytes(b"")
        (drx / "DRX935" / "DRX935001" / "DRR999999").mkdir(parents=True)
        (drx / "DRX935" / "DRX935001" / "DRR999999" / "DRR999999.sra").write_bytes(b"")
        (drx / "DRX935" / "DRX935001" / "DRR999999" / "DRR999999.sra.md5").write_bytes(b"")
        (drx / "TOP.sra").write_bytes(b"")
        outside = tmp_path / "outside"
        (outside / "DRR777777").mkdir(parents=True)
        (outside / "DRR777777" / "DRR777777.sra").write_bytes(b"")
        (drx / "DRX000" / "linked").symlink_to(outside, target_is_directory=True)

        build_dra_file_index(config)

        _, _, sra = _read_index(config)
        assert sra == {"DRR000001", "DRR000002", "DRR999999", "TOP"}

    def test_no_submissions_and_no_sra_tree_yields_empty_tables(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config, _ = self._env(tmp_path, monkeypatch, [])

        build_dra_file_index(config)

        assert _read_index(config) == (set(), set(), set())

    def test_rebuild_replaces_previous_index_and_leaves_no_temp_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config, base = self._env(tmp_path, monkeypatch, ["DRA000001"])
        sub1 = base / "fastq" / "DRA000" / "DRA000001"
        (sub1 / "DRX000001").mkdir(parents=True)
        build_dra_file_index(config)
        (sub1 / "DRX000001").rmdir()
        (sub1 / "DRX000002").mkdir()

        build_dra_file_index(config)

        fastq, _, _ = _read_index(config)
        assert fastq == {("DRA000001", "DRX000002")}
        assert [p.name for p in get_dra_file_index_db_path(config).parent.iterdir()] == ["dra_file_index.duckdb"]

    def test_queries_see_what_the_scan_found(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        config, base = self._env(tmp_path, monkeypatch, ["DRA000001"])
        (base / "fastq" / "DRA000" / "DRA000001" / "DRX000001").mkdir(parents=True)

        build_dra_file_index(config)

        assert query_fastq_dirs_bulk(config, ["DRA000001", "DRA000002"]) == {"DRA000001": {"DRX000001"}}

    def test_insert_is_not_row_by_row(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """実データは約 100 万行ある。行単位の INSERT だと 1 行あたり数百マイクロ秒かかり、時間単位になる。"""
        submissions = [f"DRA{i:06d}" for i in range(2000)]
        config, _ = self._env(tmp_path, monkeypatch, submissions)
        monkeypatch.setattr(
            dra_file_index,
            "_scan_submission_dir",
            lambda sub: (sub, [f"DRX{sub[3:]}{n:02d}" for n in range(50)], []),
        )

        started = time.monotonic()
        build_dra_file_index(config)
        elapsed = time.monotonic() - started

        fastq, _, _ = _read_index(config)
        assert len(fastq) == 100_000
        assert elapsed < 10
