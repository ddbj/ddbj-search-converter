"""Tests for ddbj_search_converter.dblink.bp_bs module.

bp_bs は外部 XML ファイルと DB に依存するため、
ここではユーティリティ関数のテストのみ行う。
"""
from pathlib import Path
from typing import Generator

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.bp_bs import (
    IdMapping,
    load_id_mapping_tsv,
    write_id_mapping_tsv,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


class TestIdMappingTsv:
    """Tests for write/load_id_mapping_tsv functions."""

    def test_roundtrip(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            mapping: IdMapping = {"1": "PRJDB1", "2": "PRJDB2", "100": "PRJNA100"}
            tsv_path = tmp_path / "mapping.tsv"

            write_id_mapping_tsv(mapping, tsv_path)
            loaded = load_id_mapping_tsv(tsv_path)

            assert loaded == mapping

    def test_empty_mapping(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            tsv_path = tmp_path / "empty.tsv"
            write_id_mapping_tsv({}, tsv_path)
            loaded = load_id_mapping_tsv(tsv_path)
            assert loaded == {}

    def test_load_nonexistent_returns_empty(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            tsv_path = tmp_path / "nonexistent.tsv"
            loaded = load_id_mapping_tsv(tsv_path)
            assert loaded == {}

    def test_numeric_id_to_accession_conversion(self, tmp_path: Path, clean_ctx: None) -> None:
        """数値 ID からアクセッションへの変換テスト。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path)
        with run_logger(config=config):
            mapping: IdMapping = {"12345": "PRJDB12345"}
            tsv_path = tmp_path / "mapping.tsv"

            write_id_mapping_tsv(mapping, tsv_path)
            loaded = load_id_mapping_tsv(tsv_path)

            assert loaded["12345"] == "PRJDB12345"
