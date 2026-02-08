"""Tests for ddbj_search_converter.sra_accessions_tab module.

DB 構築パイプライン、relation イテレータ、JSONL クエリ関数を検証する。
"""
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra_accessions_tab import (
    finalize_db,
    get_accession_info_bulk,
    get_submission_accessions,
    init_accession_db,
    iter_all_submissions,
    iter_bp_analysis_relations,
    iter_bp_bs_relations,
    iter_bp_experiment_relations,
    iter_bp_run_relations,
    iter_bp_study_relations,
    iter_bs_analysis_relations,
    iter_bs_experiment_relations,
    iter_bs_run_relations,
    iter_bs_sample_relations,
    iter_experiment_run_relations,
    iter_experiment_sample_relations,
    iter_run_sample_relations,
    iter_study_analysis_relations,
    iter_study_experiment_relations,
    iter_submission_analysis_relations,
    iter_submission_study_relations,
    iter_updated_submissions,
    load_tsv_to_tmp_db,
    lookup_submissions_for_accessions,
)
from tests.py_tests.strategies import st_sra_type, st_timestamp_str

ISO8601_PATTERN = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")

# SRA_Accessions.tab の実際の 20 カラムヘッダー
TSV_HEADER = (
    "Accession\tSubmission\tStatus\tUpdated\tPublished\tReceived\t"
    "Type\tCenter\tVisibility\tAlias\tExperiment\tSample\tStudy\t"
    "Loaded\tSpots\tBases\tMd5sum\tBioSample\tBioProject\tReplacedBy"
)

# TSV カラム名のリスト (順序保持)
_TSV_COLUMNS = TSV_HEADER.split("\t")

# デフォルト値 (全て `-` = NULL として扱われる)
_TSV_DEFAULTS = {col: "-" for col in _TSV_COLUMNS}


def _make_tsv_row(**kwargs: str) -> str:
    """20 カラムの TSV 行を生成する。指定カラムだけ上書き、残りは `-`。"""
    vals = dict(_TSV_DEFAULTS)
    vals.update(kwargs)
    return "\t".join(vals[col] for col in _TSV_COLUMNS)


def _write_tsv(path: Path, rows: List[str]) -> None:
    """ヘッダー + 行リストを TSV ファイルに書き出す。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(TSV_HEADER + "\n")
        for row in rows:
            f.write(row + "\n")


def _setup_accessions_db(db_path: Path, rows: list) -> None:
    """テスト用の accessions テーブルを作成しデータを挿入する。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE accessions (
                Accession   TEXT,
                Submission  TEXT,
                BioSample   TEXT,
                BioProject  TEXT,
                Study       TEXT,
                Experiment  TEXT,
                Sample      TEXT,
                Type        TEXT,
                Status      TEXT,
                Visibility  TEXT,
                Updated     TIMESTAMP,
                Published   TIMESTAMP,
                Received    TIMESTAMP
            )
        """)
        for row in rows:
            conn.execute(
                "INSERT INTO accessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )


def _make_config(tmp_path: Path) -> Config:
    """テスト用 Config を作成する。"""
    return Config(
        result_dir=tmp_path,
        const_dir=tmp_path / "const",
    )


def _make_config_with_db(
    tmp_path: Path,
    source: str,
    rows: list,
) -> Config:
    """Config 作成 + DB セットアップの一括ショートカット。"""
    config = _make_config(tmp_path)
    db_name = "sra_accessions.duckdb" if source == "sra" else "dra_accessions.duckdb"
    db_path = config.const_dir / "sra" / db_name
    _setup_accessions_db(db_path, rows)
    return config


# ============================================================
# Group 1: DB 構築パイプライン
# ============================================================


class TestInitAccessionDb:
    """init_accession_db のテスト。"""

    def test_creates_empty_table(self, tmp_path: Path) -> None:
        """空の accessions テーブルが 13 カラム・0 行で作成される。"""
        db_path = tmp_path / "test.duckdb"
        init_accession_db(db_path)

        with duckdb.connect(db_path) as conn:
            cols = conn.execute("DESCRIBE accessions").fetchall()
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()[0]

        assert len(cols) == 13
        assert count == 0

    def test_overwrites_existing(self, tmp_path: Path) -> None:
        """既存ファイルを上書き (再呼び出しでリセット)。"""
        db_path = tmp_path / "test.duckdb"
        init_accession_db(db_path)

        with duckdb.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO accessions VALUES "
                "('A', 'S', 'BS', 'BP', 'ST', 'EX', 'SA', 'RUN', 'live', 'public', NULL, NULL, NULL)"
            )

        init_accession_db(db_path)

        with duckdb.connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()[0]

        assert count == 0

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        """親ディレクトリが自動作成される。"""
        db_path = tmp_path / "a" / "b" / "c" / "test.duckdb"
        init_accession_db(db_path)
        assert db_path.exists()


class TestLoadTsvToTmpDb:
    """load_tsv_to_tmp_db のテスト。"""

    def test_basic_load(self, tmp_path: Path) -> None:
        """1 行 TSV が正しく DB にロードされる。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        _write_tsv(tsv_path, [
            _make_tsv_row(
                Accession="SRR000001",
                Submission="SRA000001",
                Type="RUN",
                Status="live",
                Visibility="public",
            ),
        ])
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            rows = conn.execute("SELECT Accession, Type FROM accessions").fetchall()

        assert rows == [("SRR000001", "RUN")]

    def test_dash_is_null(self, tmp_path: Path) -> None:
        """`-` は NULL として扱われる (SRA 形式)。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        _write_tsv(tsv_path, [
            _make_tsv_row(Accession="SRR000001", BioProject="-"),
        ])
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            row = conn.execute("SELECT BioProject FROM accessions").fetchone()

        assert row[0] is None

    def test_empty_string_is_null(self, tmp_path: Path) -> None:
        """空文字列は NULL として扱われる (DRA 形式)。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        # 空文字列を直接 TSV に書き込む
        tsv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tsv_path, "w", encoding="utf-8") as f:
            f.write(TSV_HEADER + "\n")
            cols = ["SRR000001"] + [""] * 19
            f.write("\t".join(cols) + "\n")

        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            row = conn.execute("SELECT Submission, BioProject FROM accessions").fetchone()

        assert row[0] is None
        assert row[1] is None

    def test_sra_datetime_format(self, tmp_path: Path) -> None:
        """SRA 日付 `2023-06-01T21:15:36Z` が TIMESTAMP に変換される。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        _write_tsv(tsv_path, [
            _make_tsv_row(
                Accession="SRR000001",
                Updated="2023-06-01T21:15:36Z",
                Published="2023-07-01T00:00:00Z",
                Received="2023-05-01T12:30:45Z",
            ),
        ])
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            row = conn.execute(
                "SELECT Updated, Published, Received FROM accessions"
            ).fetchone()

        assert row[0] == datetime(2023, 6, 1, 21, 15, 36)
        assert row[1] == datetime(2023, 7, 1, 0, 0, 0)
        assert row[2] == datetime(2023, 5, 1, 12, 30, 45)

    def test_dra_date_format(self, tmp_path: Path) -> None:
        """DRA 日付 `2015-01-28` が TIMESTAMP に変換される (midnight)。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        _write_tsv(tsv_path, [
            _make_tsv_row(
                Accession="DRR000001",
                Updated="2015-01-28",
                Published="2015-02-01",
                Received="2015-01-01",
            ),
        ])
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            row = conn.execute(
                "SELECT Updated, Published, Received FROM accessions"
            ).fetchone()

        assert row[0] == datetime(2015, 1, 28, 0, 0, 0)
        assert row[1] == datetime(2015, 2, 1, 0, 0, 0)
        assert row[2] == datetime(2015, 1, 1, 0, 0, 0)

    def test_null_date_preserved(self, tmp_path: Path) -> None:
        """NULL 日付 (`-`) は NULL のまま保持される。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        _write_tsv(tsv_path, [
            _make_tsv_row(Accession="SRR000001", Updated="-", Published="-", Received="-"),
        ])
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            row = conn.execute(
                "SELECT Updated, Published, Received FROM accessions"
            ).fetchone()

        assert row[0] is None
        assert row[1] is None
        assert row[2] is None

    def test_multiple_rows(self, tmp_path: Path) -> None:
        """複数行が正しくロードされる。"""
        db_path = tmp_path / "test.duckdb"
        tsv_path = tmp_path / "test.tsv"

        init_accession_db(db_path)
        _write_tsv(tsv_path, [
            _make_tsv_row(Accession="SRR000001", Type="RUN"),
            _make_tsv_row(Accession="SRR000002", Type="EXPERIMENT"),
            _make_tsv_row(Accession="SRR000003", Type="STUDY"),
        ])
        load_tsv_to_tmp_db(tsv_path, db_path)

        with duckdb.connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()[0]

        assert count == 3

    @given(ts=st_timestamp_str())
    @settings(deadline=2000, max_examples=20)
    def test_pbt_timestamp_roundtrip(self, ts: str) -> None:
        """任意の TIMESTAMP 文字列が CAST ラウンドトリップで保持される。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            db_path = tmp_path / "test.duckdb"
            tsv_path = tmp_path / "test.tsv"

            init_accession_db(db_path)
            _write_tsv(tsv_path, [
                _make_tsv_row(Accession="SRR000001", Updated=ts),
            ])
            load_tsv_to_tmp_db(tsv_path, db_path)

            with duckdb.connect(db_path) as conn:
                row = conn.execute("SELECT Updated FROM accessions").fetchone()

            assert row[0] is not None

    @given(
        acc=st.text(
            alphabet=st.characters(categories=["L", "N"]),
            min_size=1,
            max_size=20,
        ),
        sra_type=st_sra_type(),
    )
    @settings(deadline=2000, max_examples=20)
    def test_pbt_text_roundtrip(self, acc: str, sra_type: str) -> None:
        """任意の Accession と Type テキストが TSV → DB ラウンドトリップで保持される。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            db_path = tmp_path / "test.duckdb"
            tsv_path = tmp_path / "test.tsv"

            init_accession_db(db_path)
            _write_tsv(tsv_path, [
                _make_tsv_row(Accession=acc, Type=sra_type),
            ])
            load_tsv_to_tmp_db(tsv_path, db_path)

            with duckdb.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT Accession, Type FROM accessions"
                ).fetchone()

            assert row[0] == acc
            assert row[1] == sra_type


class TestFinalizeDb:
    """finalize_db のテスト。"""

    def test_creates_indexes(self, tmp_path: Path) -> None:
        """3 インデックス (idx_bp, idx_bs, idx_acc) が作成される。"""
        tmp_db = tmp_path / "tmp.duckdb"
        final_db = tmp_path / "final.duckdb"
        init_accession_db(tmp_db)

        finalize_db(tmp_db, final_db)

        with duckdb.connect(final_db) as conn:
            indexes = conn.execute(
                "SELECT index_name FROM duckdb_indexes()"
            ).fetchall()
            index_names = {row[0] for row in indexes}

        assert "idx_bp" in index_names
        assert "idx_bs" in index_names
        assert "idx_acc" in index_names

    def test_moves_file(self, tmp_path: Path) -> None:
        """tmp → final にファイルが移動する。"""
        tmp_db = tmp_path / "tmp.duckdb"
        final_db = tmp_path / "final.duckdb"
        init_accession_db(tmp_db)

        finalize_db(tmp_db, final_db)

        assert final_db.exists()
        assert not tmp_db.exists()

    def test_overwrites_existing_final(self, tmp_path: Path) -> None:
        """既存の final を上書きする。"""
        tmp_db = tmp_path / "tmp.duckdb"
        final_db = tmp_path / "final.duckdb"

        # 古い final を作成
        init_accession_db(final_db)

        # 新しい tmp を作成してデータ挿入
        init_accession_db(tmp_db)
        with duckdb.connect(tmp_db) as conn:
            conn.execute(
                "INSERT INTO accessions VALUES "
                "('NEW', 'S', 'BS', 'BP', 'ST', 'EX', 'SA', 'RUN', 'live', 'public', NULL, NULL, NULL)"
            )

        finalize_db(tmp_db, final_db)

        with duckdb.connect(final_db) as conn:
            row = conn.execute("SELECT Accession FROM accessions").fetchone()

        assert row[0] == "NEW"

    def test_data_preserved(self, tmp_path: Path) -> None:
        """データが finalize 後も保持される。"""
        tmp_db = tmp_path / "tmp.duckdb"
        final_db = tmp_path / "final.duckdb"
        init_accession_db(tmp_db)

        with duckdb.connect(tmp_db) as conn:
            conn.execute(
                "INSERT INTO accessions VALUES "
                "('SRR1', 'SRA1', 'BS1', 'BP1', 'ST1', 'EX1', 'SA1', "
                "'RUN', 'live', 'public', '2023-01-01', '2023-02-01', '2022-12-01')"
            )
            conn.execute(
                "INSERT INTO accessions VALUES "
                "('SRR2', 'SRA2', 'BS2', 'BP2', 'ST2', 'EX2', 'SA2', "
                "'EXPERIMENT', 'live', 'public', NULL, NULL, NULL)"
            )

        finalize_db(tmp_db, final_db)

        with duckdb.connect(final_db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM accessions").fetchone()[0]

        assert count == 2


# ============================================================
# Group 2: Relation イテレータ
# ============================================================

# 各 relation イテレータの仕様を定義
# (関数, Type フィルタ (None=フィルタなし), 出力の (col1, col2) カラム名)
_RELATION_SPECS: list[tuple] = [
    (iter_bp_bs_relations, None, "BioProject", "BioSample"),
    (iter_study_experiment_relations, "EXPERIMENT", "Study", "Accession"),
    (iter_experiment_run_relations, "RUN", "Experiment", "Accession"),
    (iter_experiment_sample_relations, "EXPERIMENT", "Accession", "Sample"),
    (iter_run_sample_relations, "RUN", "Accession", "Sample"),
    (iter_submission_study_relations, "STUDY", "Submission", "Accession"),
    (iter_study_analysis_relations, "ANALYSIS", "Study", "Accession"),
    (iter_submission_analysis_relations, "ANALYSIS", "Submission", "Accession"),
    # BioProject <-> SRA
    (iter_bp_study_relations, "STUDY", "BioProject", "Accession"),
    (iter_bp_experiment_relations, "EXPERIMENT", "BioProject", "Accession"),
    (iter_bp_run_relations, "RUN", "BioProject", "Accession"),
    (iter_bp_analysis_relations, "ANALYSIS", "BioProject", "Accession"),
    # BioSample <-> SRA
    (iter_bs_sample_relations, "SAMPLE", "BioSample", "Accession"),
    (iter_bs_experiment_relations, "EXPERIMENT", "BioSample", "Accession"),
    (iter_bs_run_relations, "RUN", "BioSample", "Accession"),
    (iter_bs_analysis_relations, "ANALYSIS", "BioSample", "Accession"),
]


def _relation_id(spec: tuple) -> str:
    return spec[0].__name__


def _make_row_for_relation(
    type_val: Optional[str],
    col1_name: str,
    col1_val: str,
    col2_name: str,
    col2_val: str,
) -> tuple:
    """relation テスト用の 13 カラム行を生成する。"""
    mapping = {
        "Accession": None,
        "Submission": None,
        "BioSample": None,
        "BioProject": None,
        "Study": None,
        "Experiment": None,
        "Sample": None,
    }
    mapping[col1_name] = col1_val
    mapping[col2_name] = col2_val
    return (
        mapping["Accession"],
        mapping["Submission"],
        mapping["BioSample"],
        mapping["BioProject"],
        mapping["Study"],
        mapping["Experiment"],
        mapping["Sample"],
        type_val,
        "live",
        "public",
        None,
        None,
        None,
    )


class TestRelationIterators:
    """全 8 relation イテレータの共通テスト。"""

    @pytest.mark.parametrize("spec", _RELATION_SPECS, ids=_relation_id)
    def test_matching_row_returned(self, tmp_path: Path, spec: tuple) -> None:
        """マッチする行が返される。"""
        func, type_filter, col1, col2 = spec
        row = _make_row_for_relation(type_filter, col1, "VAL1", col2, "VAL2")
        config = _make_config_with_db(tmp_path, "sra", [row])
        result = list(func(config, source="sra"))
        assert ("VAL1", "VAL2") in result

    @pytest.mark.parametrize(
        "spec",
        [s for s in _RELATION_SPECS if s[1] is not None],
        ids=lambda s: s[0].__name__,
    )
    def test_wrong_type_excluded(self, tmp_path: Path, spec: tuple) -> None:
        """異なる Type の行は除外される。"""
        func, type_filter, col1, col2 = spec
        wrong_type = "SUBMISSION" if type_filter != "SUBMISSION" else "RUN"
        row = _make_row_for_relation(wrong_type, col1, "VAL1", col2, "VAL2")
        config = _make_config_with_db(tmp_path, "sra", [row])
        result = list(func(config, source="sra"))
        assert result == []

    @pytest.mark.parametrize("spec", _RELATION_SPECS, ids=_relation_id)
    def test_null_col_excluded(self, tmp_path: Path, spec: tuple) -> None:
        """NULL カラムの行は除外される。"""
        func, type_filter, col1, col2 = spec
        # col2 が NULL の行
        mapping = {
            "Accession": None,
            "Submission": None,
            "BioSample": None,
            "BioProject": None,
            "Study": None,
            "Experiment": None,
            "Sample": None,
        }
        mapping[col1] = "VAL1"
        # col2 は None のまま
        row = (
            mapping["Accession"],
            mapping["Submission"],
            mapping["BioSample"],
            mapping["BioProject"],
            mapping["Study"],
            mapping["Experiment"],
            mapping["Sample"],
            type_filter,
            "live",
            "public",
            None,
            None,
            None,
        )
        config = _make_config_with_db(tmp_path, "sra", [row])
        result = list(func(config, source="sra"))
        assert result == []

    @pytest.mark.parametrize("spec", _RELATION_SPECS, ids=_relation_id)
    def test_distinct_dedup(self, tmp_path: Path, spec: tuple) -> None:
        """DISTINCT で重複が排除される。"""
        func, type_filter, col1, col2 = spec
        row = _make_row_for_relation(type_filter, col1, "VAL1", col2, "VAL2")
        config = _make_config_with_db(tmp_path, "sra", [row, row])
        result = list(func(config, source="sra"))
        assert len(result) == 1

    @pytest.mark.parametrize("spec", _RELATION_SPECS, ids=_relation_id)
    def test_empty_table(self, tmp_path: Path, spec: tuple) -> None:
        """空テーブル → 空リスト。"""
        func, _, _, _ = spec
        config = _make_config_with_db(tmp_path, "sra", [])
        result = list(func(config, source="sra"))
        assert result == []

    @pytest.mark.parametrize("spec", _RELATION_SPECS, ids=_relation_id)
    @pytest.mark.parametrize("source", ["sra", "dra"])
    def test_both_sources(self, tmp_path: Path, spec: tuple, source: str) -> None:
        """sra/dra 両 source で動作する。"""
        func, type_filter, col1, col2 = spec
        row = _make_row_for_relation(type_filter, col1, "VAL1", col2, "VAL2")
        config = _make_config_with_db(tmp_path, source, [row])
        result = list(func(config, source=source))
        assert ("VAL1", "VAL2") in result


# ============================================================
# Group 3: JSONL クエリ関数
# ============================================================


class TestGetAccessionInfoBulk:
    """get_accession_info_bulk のテスト。"""

    def test_dates_are_iso8601(self, tmp_path: Path) -> None:
        """TIMESTAMP が 'YYYY-MM-DDTHH:MM:SSZ' に変換される。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                "RUN", "live", "public",
                "2014-05-12 10:30:00", "2014-06-01 00:00:00", "2014-04-01 12:00:00",
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000001"])

        assert "SRR000001" in result
        _, _, received, updated, published, _ = result["SRR000001"]
        assert received == "2014-04-01T12:00:00Z"
        assert updated == "2014-05-12T10:30:00Z"
        assert published == "2014-06-01T00:00:00Z"

    def test_null_dates_remain_none(self, tmp_path: Path) -> None:
        """NULL の TIMESTAMP は None のまま返される。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000002", "SRA000002", None, None, None, None, None,
                "RUN", "live", "public",
                None, None, None,
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000002"])

        assert "SRR000002" in result
        _, _, received, updated, published, _ = result["SRR000002"]
        assert received is None
        assert updated is None
        assert published is None

    @given(
        year=st.integers(min_value=2000, max_value=2030),
        month=st.integers(min_value=1, max_value=12),
        day=st.integers(min_value=1, max_value=28),
        hour=st.integers(min_value=0, max_value=23),
        minute=st.integers(min_value=0, max_value=59),
        second=st.integers(min_value=0, max_value=59),
    )
    @settings(deadline=2000, max_examples=20)
    def test_arbitrary_timestamps_are_iso8601(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
    ) -> None:
        """任意の TIMESTAMP が ISO 8601 形式に変換される。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            ts = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
            config = _make_config_with_db(tmp_path, "sra", [
                (
                    "SRR999999", "SRA999999", None, None, None, None, None,
                    "RUN", "live", "public",
                    ts, ts, ts,
                ),
            ])
            result = get_accession_info_bulk(config, "sra", ["SRR999999"])

            assert "SRR999999" in result
            _, _, received, updated, published, _ = result["SRR999999"]
            for date_val in [received, updated, published]:
                assert date_val is not None
                assert ISO8601_PATTERN.match(date_val), f"'{date_val}' is not ISO 8601"

    def test_empty_accessions_returns_empty(self, tmp_path: Path) -> None:
        """空リストを渡すと空 dict が返る。"""
        config = _make_config(tmp_path)
        result = get_accession_info_bulk(config, "sra", [])
        assert result == {}

    def test_dra_source(self, tmp_path: Path) -> None:
        """source='dra' でも同様に ISO 8601 形式で返される。"""
        config = _make_config_with_db(tmp_path, "dra", [
            (
                "DRR000001", "DRA000001", None, None, None, None, None,
                "RUN", "live", "public",
                "2020-02-20 14:00:00", "2020-03-10 00:00:00", "2020-01-15 09:30:45",
            ),
        ])
        result = get_accession_info_bulk(config, "dra", ["DRR000001"])

        assert "DRR000001" in result
        _, _, received, updated, published, _ = result["DRR000001"]
        assert received == "2020-01-15T09:30:45Z"
        assert updated == "2020-02-20T14:00:00Z"
        assert published == "2020-03-10T00:00:00Z"

    def test_null_status_defaults_to_public(self, tmp_path: Path) -> None:
        """NULL Status は 'public' にデフォルトされる。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                "RUN", None, "public",
                None, None, None,
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000001"])
        status, _, _, _, _, _ = result["SRR000001"]
        assert status == "public"

    def test_null_visibility_defaults_to_public(self, tmp_path: Path) -> None:
        """NULL Visibility は 'public' にデフォルトされる。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                "RUN", "live", None,
                None, None, None,
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000001"])
        _, visibility, _, _, _, _ = result["SRR000001"]
        assert visibility == "public"

    def test_null_type_defaults_to_empty(self, tmp_path: Path) -> None:
        """NULL Type は空文字列にデフォルトされる。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                None, "live", "public",
                None, None, None,
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000001"])
        _, _, _, _, _, type_ = result["SRR000001"]
        assert type_ == ""

    def test_nonexistent_accession_excluded(self, tmp_path: Path) -> None:
        """存在しない accession は結果に含まれない。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                "RUN", "live", "public",
                None, None, None,
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000001", "SRR999999"])
        assert "SRR000001" in result
        assert "SRR999999" not in result

    def test_batch_split_at_10001(self, tmp_path: Path) -> None:
        """10001 行でバッチ分割される。"""
        rows = [
            (
                f"SRR{i:06d}", f"SRA{i:06d}", None, None, None, None, None,
                "RUN", "live", "public",
                None, None, None,
            )
            for i in range(10001)
        ]
        config = _make_config_with_db(tmp_path, "sra", rows)
        accessions = [f"SRR{i:06d}" for i in range(10001)]
        result = get_accession_info_bulk(config, "sra", accessions)
        assert len(result) == 10001

    def test_batch_boundary_at_10000(self, tmp_path: Path) -> None:
        """10000 行境界 (ちょうど 1 バッチ)。"""
        rows = [
            (
                f"SRR{i:06d}", f"SRA{i:06d}", None, None, None, None, None,
                "RUN", "live", "public",
                None, None, None,
            )
            for i in range(10000)
        ]
        config = _make_config_with_db(tmp_path, "sra", rows)
        accessions = [f"SRR{i:06d}" for i in range(10000)]
        result = get_accession_info_bulk(config, "sra", accessions)
        assert len(result) == 10000

    def test_duplicate_accession_overwritten(self, tmp_path: Path) -> None:
        """同一 accession の複数行は dict 上書きで最後の値になる。"""
        config = _make_config_with_db(tmp_path, "sra", [
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                "RUN", "live", "public",
                None, None, None,
            ),
            (
                "SRR000001", "SRA000001", None, None, None, None, None,
                "RUN", "suppressed", "public",
                None, None, None,
            ),
        ])
        result = get_accession_info_bulk(config, "sra", ["SRR000001"])
        assert "SRR000001" in result
        # 結果は 2 行のうちいずれかで dict 上書きされている
        status, _, _, _, _, _ = result["SRR000001"]
        assert status in ("live", "suppressed")


class TestIterAllSubmissions:
    """iter_all_submissions のテスト。"""

    def test_returns_submission_type_only(self, tmp_path: Path) -> None:
        """SUBMISSION Type のみ返す。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRA000001", "SRA000001", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = list(iter_all_submissions(config, "sra"))
        assert result == ["SRA000001"]

    def test_sorted_ascending(self, tmp_path: Path) -> None:
        """Accession 昇順ソート。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRA000003", "SRA000003", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
            ("SRA000001", "SRA000001", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
            ("SRA000002", "SRA000002", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
        ])
        result = list(iter_all_submissions(config, "sra"))
        assert result == ["SRA000001", "SRA000002", "SRA000003"]

    def test_distinct_dedup(self, tmp_path: Path) -> None:
        """DISTINCT 重複排除。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRA000001", "SRA000001", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
            ("SRA000001", "SRA000001", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
        ])
        result = list(iter_all_submissions(config, "sra"))
        assert result == ["SRA000001"]

    def test_empty_table(self, tmp_path: Path) -> None:
        """空テーブル → 空リスト。"""
        config = _make_config_with_db(tmp_path, "sra", [])
        result = list(iter_all_submissions(config, "sra"))
        assert result == []


class TestIterUpdatedSubmissions:
    """iter_updated_submissions のテスト。"""

    def test_returns_submissions_since_margin(self, tmp_path: Path) -> None:
        """since - margin_days 以降の Submission を返す。"""
        config = _make_config_with_db(tmp_path, "sra", [
            # Updated = 2026-01-20 (since=2026-01-25, margin=30 → cutoff=2025-12-26)
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", "2026-01-20 00:00:00", None, None),
            # Updated = 2025-12-01 (cutoff 以前)
            ("SRR000002", "SRA000002", None, None, None, None, None,
             "RUN", "live", "public", "2025-12-01 00:00:00", None, None),
        ])
        result = list(iter_updated_submissions(config, "sra", "2026-01-25T00:00:00Z", margin_days=30))
        assert "SRA000001" in result
        assert "SRA000002" not in result

    def test_margin_days_boundary(self, tmp_path: Path) -> None:
        """margin_days 境界値テスト (ちょうど cutoff 日の行が含まれる)。"""
        config = _make_config_with_db(tmp_path, "sra", [
            # Updated = 2026-01-10 (since=2026-01-20, margin=10 → cutoff=2026-01-10)
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", "2026-01-10 00:00:00", None, None),
        ])
        result = list(iter_updated_submissions(config, "sra", "2026-01-20T00:00:00Z", margin_days=10))
        assert "SRA000001" in result

    def test_null_submission_excluded(self, tmp_path: Path) -> None:
        """NULL Submission は除外される。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", None, None, None, None, None, None,
             "RUN", "live", "public", "2026-01-20 00:00:00", None, None),
        ])
        result = list(iter_updated_submissions(config, "sra", "2026-01-01T00:00:00Z", margin_days=0))
        assert result == []

    def test_null_updated_excluded(self, tmp_path: Path) -> None:
        """NULL Updated は除外される (WHERE Updated >= ? に NULL はマッチしない)。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = list(iter_updated_submissions(config, "sra", "2020-01-01T00:00:00Z", margin_days=0))
        assert result == []

    def test_sorted_ascending(self, tmp_path: Path) -> None:
        """Submission 昇順ソート。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000002", "SRA000003", None, None, None, None, None,
             "RUN", "live", "public", "2026-01-20 00:00:00", None, None),
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", "2026-01-20 00:00:00", None, None),
        ])
        result = list(iter_updated_submissions(config, "sra", "2026-01-01T00:00:00Z", margin_days=0))
        assert result == ["SRA000001", "SRA000003"]

    def test_since_iso8601_parse(self, tmp_path: Path) -> None:
        """since の ISO8601 形式が正しくパースされる。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", "2026-01-15 12:00:00", None, None),
        ])
        # since="2026-01-20T15:30:00Z" で margin_days=0 → cutoff=2026-01-20
        result = list(iter_updated_submissions(config, "sra", "2026-01-20T15:30:00Z", margin_days=0))
        assert result == []
        # margin_days=10 → cutoff=2026-01-10
        result = list(iter_updated_submissions(config, "sra", "2026-01-20T15:30:00Z", margin_days=10))
        assert "SRA000001" in result


class TestLookupSubmissionsForAccessions:
    """lookup_submissions_for_accessions のテスト。"""

    def test_basic_reverse_lookup(self, tmp_path: Path) -> None:
        """基本的な逆引き。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = lookup_submissions_for_accessions(config, "sra", ["SRR000001"])
        assert result == {"SRR000001": "SRA000001"}

    def test_empty_input(self, tmp_path: Path) -> None:
        """空入力 → 空結果。"""
        config = _make_config(tmp_path)
        result = lookup_submissions_for_accessions(config, "sra", [])
        assert result == {}

    def test_null_submission_excluded(self, tmp_path: Path) -> None:
        """NULL Submission は除外される。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", None, None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = lookup_submissions_for_accessions(config, "sra", ["SRR000001"])
        assert "SRR000001" not in result

    def test_nonexistent_accession_excluded(self, tmp_path: Path) -> None:
        """存在しない accession は結果にない。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = lookup_submissions_for_accessions(config, "sra", ["SRR999999"])
        assert result == {}


class TestGetSubmissionAccessions:
    """get_submission_accessions のテスト。"""

    def test_basic_forward_lookup(self, tmp_path: Path) -> None:
        """基本的な正引き。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = get_submission_accessions(config, "sra", {"SRA000001"})
        assert result == {"SRA000001": ["SRR000001"]}

    def test_empty_input(self, tmp_path: Path) -> None:
        """空入力 → 空結果。"""
        config = _make_config(tmp_path)
        result = get_submission_accessions(config, "sra", set())
        assert result == {}

    def test_nonexistent_submission(self, tmp_path: Path) -> None:
        """存在しない submission → 空リスト。"""
        config = _make_config_with_db(tmp_path, "sra", [])
        result = get_submission_accessions(config, "sra", {"SRA999999"})
        assert result == {"SRA999999": []}

    def test_sorted_accessions(self, tmp_path: Path) -> None:
        """結果のアクセッションがソートされている。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRR000003", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
            ("SRR000002", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
        ])
        result = get_submission_accessions(config, "sra", {"SRA000001"})
        assert result["SRA000001"] == ["SRR000001", "SRR000002", "SRR000003"]

    def test_multiple_accessions_per_submission(self, tmp_path: Path) -> None:
        """1 submission に複数 accession。"""
        config = _make_config_with_db(tmp_path, "sra", [
            ("SRA000001", "SRA000001", None, None, None, None, None,
             "SUBMISSION", "live", "public", None, None, None),
            ("SRR000001", "SRA000001", None, None, None, None, None,
             "RUN", "live", "public", None, None, None),
            ("SRX000001", "SRA000001", None, None, None, None, None,
             "EXPERIMENT", "live", "public", None, None, None),
        ])
        result = get_submission_accessions(config, "sra", {"SRA000001"})
        assert len(result["SRA000001"]) == 3
