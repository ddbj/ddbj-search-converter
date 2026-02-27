"""Tests for ddbj_search_converter.dblink.sra_internal module.

process_sra_internal_relations の統合テスト。
BioProject/BioSample <-> SRA 関連の抽出・フィルタを検証する。
"""
from pathlib import Path
from typing import Generator, List, Set, Tuple

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import init_dblink_db
from ddbj_search_converter.dblink.sra_internal import \
    process_sra_internal_relations
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


def _setup_accessions_db(db_path: Path, rows: list) -> None:  # type: ignore[type-arg]
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


def _make_config(tmp_path: Path, source: str, rows: list) -> Config:  # type: ignore[type-arg]
    """Config + accessions DB + dblink DB を準備する。"""
    config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
    db_name = "sra_accessions.duckdb" if source == "sra" else "dra_accessions.duckdb"
    db_path = config.const_dir / "sra" / db_name
    _setup_accessions_db(db_path, rows)
    init_dblink_db(config)
    return config


def _get_relations(config: Config) -> List[Tuple[str, str, str, str]]:
    """DBLink DB から全 relation を取得する。"""
    db_path = config.const_dir / "dblink" / "dblink.tmp.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT src_type, src_accession, dst_type, dst_accession FROM relation"
        ).fetchall()
    return rows


def _default_kwargs() -> dict:  # type: ignore[type-arg]
    return {
        "sra_blacklist": set(),
        "bp_blacklist": set(),
        "bs_blacklist": set(),
        "bp_id_to_accession": {},
        "bs_id_to_accession": {},
    }


class TestBpStudyRelation:
    """BioProject <-> Study 関連テスト。"""

    def test_bp_study_basic(self, tmp_path: Path, clean_ctx: None) -> None:
        """BioProject <-> Study が accession 形式で正しく登録される。"""
        rows = [
            ("SRP000001", "SRA000001", None, "PRJNA1", "SRP000001", None, None,
             "STUDY", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **_default_kwargs())

        relations = _get_relations(config)
        bp_study = [(s, sa, d, da) for s, sa, d, da in relations
                    if {s, d} == {"bioproject", "sra-study"}]
        assert len(bp_study) == 1
        accessions = {bp_study[0][1], bp_study[0][3]}
        assert "PRJNA1" in accessions
        assert "SRP000001" in accessions

    def test_bp_numeric_id_converted(self, tmp_path: Path, clean_ctx: None) -> None:
        """BioProject 数値 ID が accession に変換される。"""
        rows = [
            ("SRP000002", "SRA000001", None, "123", "SRP000002", None, None,
             "STUDY", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        kwargs = _default_kwargs()
        kwargs["bp_id_to_accession"] = {"123": "PRJNA123"}
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **kwargs)

        relations = _get_relations(config)
        bp_study = [(s, sa, d, da) for s, sa, d, da in relations
                    if {s, d} == {"bioproject", "sra-study"}]
        assert len(bp_study) == 1
        accessions = {bp_study[0][1], bp_study[0][3]}
        assert "PRJNA123" in accessions
        assert "SRP000002" in accessions


class TestBsSampleRelation:
    """BioSample <-> Sample 関連テスト。"""

    def test_bs_sample_basic(self, tmp_path: Path, clean_ctx: None) -> None:
        """BioSample <-> Sample が正しく登録される。"""
        rows = [
            ("SRS000001", "SRA000001", "SAMN00001", None, None, None, "SRS000001",
             "SAMPLE", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **_default_kwargs())

        relations = _get_relations(config)
        bs_sample = [(s, sa, d, da) for s, sa, d, da in relations
                     if {s, d} == {"biosample", "sra-sample"}]
        assert len(bs_sample) == 1
        accessions = {bs_sample[0][1], bs_sample[0][3]}
        assert "SAMN00001" in accessions
        assert "SRS000001" in accessions


class TestBlacklistFiltering:
    """Blacklist フィルタテスト。"""

    def test_bp_blacklist_filters(self, tmp_path: Path, clean_ctx: None) -> None:
        """BP blacklist に含まれる BioProject は除外される。"""
        rows = [
            ("SRP000001", "SRA000001", None, "PRJNA1", "SRP000001", None, None,
             "STUDY", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        kwargs = _default_kwargs()
        kwargs["bp_blacklist"] = {"PRJNA1"}
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **kwargs)

        relations = _get_relations(config)
        bp_study = [(s, sa, d, da) for s, sa, d, da in relations
                    if {s, d} == {"bioproject", "sra-study"}]
        assert len(bp_study) == 0

    def test_bs_blacklist_filters(self, tmp_path: Path, clean_ctx: None) -> None:
        """BS blacklist に含まれる BioSample は除外される。"""
        rows = [
            ("SRS000001", "SRA000001", "SAMN00001", None, None, None, "SRS000001",
             "SAMPLE", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        kwargs = _default_kwargs()
        kwargs["bs_blacklist"] = {"SAMN00001"}
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **kwargs)

        relations = _get_relations(config)
        bs_sample = [(s, sa, d, da) for s, sa, d, da in relations
                     if {s, d} == {"biosample", "sra-sample"}]
        assert len(bs_sample) == 0

    def test_sra_blacklist_filters_bp_sra(self, tmp_path: Path, clean_ctx: None) -> None:
        """SRA blacklist に含まれる SRA accession は BP ↔ SRA でも除外される。"""
        rows = [
            ("SRP000001", "SRA000001", None, "PRJNA1", "SRP000001", None, None,
             "STUDY", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        kwargs = _default_kwargs()
        kwargs["sra_blacklist"] = {"SRP000001"}
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **kwargs)

        relations = _get_relations(config)
        bp_study = [(s, sa, d, da) for s, sa, d, da in relations
                    if {s, d} == {"bioproject", "sra-study"}]
        assert len(bp_study) == 0


class TestInvalidAccessionSkipped:
    """無効な accession がスキップされるテスト。"""

    def test_invalid_sra_study_skipped(self, tmp_path: Path, clean_ctx: None) -> None:
        """無効な SRA Study accession はスキップされる。"""
        rows = [
            ("INVALID", "SRA000001", None, "PRJNA1", "SRP000001", None, None,
             "STUDY", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **_default_kwargs())

        relations = _get_relations(config)
        bp_study = [(s, sa, d, da) for s, sa, d, da in relations
                    if {s, d} == {"bioproject", "sra-study"}]
        assert len(bp_study) == 0

    def test_unconvertible_bp_id_skipped(self, tmp_path: Path, clean_ctx: None) -> None:
        """変換不能な BioProject 数値 ID はスキップされる。"""
        rows = [
            ("SRP000001", "SRA000001", None, "99999", "SRP000001", None, None,
             "STUDY", "live", "public", None, None, None),
        ]
        config = _make_config(tmp_path, "sra", rows)
        with run_logger(config=config):
            process_sra_internal_relations(config, source="sra", **_default_kwargs())

        relations = _get_relations(config)
        bp_study = [(s, sa, d, da) for s, sa, d, da in relations
                    if {s, d} == {"bioproject", "sra-study"}]
        assert len(bp_study) == 0
