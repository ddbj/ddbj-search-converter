"""Blacklist vs Preserved precedence の振る舞いを pin する。

SPEC: docs/data-architecture.md §Blacklist と Preserved の precedence
- blacklist と preserved の **両方** に含まれる accession を含むペアは除外。
- preserved だけにあるペアは追加されて残る。
- blacklist だけにある accession を含むペアは除外される。

実装は ``ddbj_search_converter/dblink/bp_bs.py::process_preserved_file``
+ ``filter_by_blacklist`` の合成で実現される。本テストでは内部関数を直接
組み合わせて合成挙動を verify する (full pipeline の XML I/O は別テスト)。
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import BP_BS_PRESERVED_REL_PATH, Config
from ddbj_search_converter.dblink.bp_bs import process_preserved_file
from ddbj_search_converter.dblink.db import IdPairs
from ddbj_search_converter.dblink.utils import (
    filter_by_blacklist,
    filter_pairs_by_blacklist,
)
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    yield
    _ctx.set(None)


def _write_preserved(const_dir: Path, lines: list[str]) -> None:
    path = const_dir / BP_BS_PRESERVED_REL_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestBpBsPrecedence:
    """``process_preserved_file`` → ``filter_by_blacklist`` の合成順を pin。"""

    def test_blacklist_overrides_preserved(self, tmp_path: Path, clean_ctx: None) -> None:
        const_dir = tmp_path / "const"
        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            # preserved に (SAMD00000001, PRJDB10001) を入れる
            _write_preserved(const_dir, ["SAMD00000001\tPRJDB10001"])

            bs_to_bp: IdPairs = set()
            # 元データに別のペアを 1 件
            bs_to_bp.add(("SAMD00000002", "PRJDB10002"))

            process_preserved_file(config, bs_to_bp)
            assert ("SAMD00000001", "PRJDB10001") in bs_to_bp

            # blacklist に preserved 側 PRJDB10001 を入れる
            bs_to_bp = filter_by_blacklist(bs_to_bp, {"PRJDB10001"}, set())

            assert ("SAMD00000001", "PRJDB10001") not in bs_to_bp, (
                "preserved にあっても blacklist にあるなら除外される (blacklist 優先)"
            )
            assert ("SAMD00000002", "PRJDB10002") in bs_to_bp

    def test_preserved_only_pair_is_kept(self, tmp_path: Path, clean_ctx: None) -> None:
        """preserved のみに含まれ、blacklist に無いペアはそのまま残る。"""
        const_dir = tmp_path / "const"
        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            _write_preserved(const_dir, ["SAMD00000010\tPRJDB10010"])
            bs_to_bp: IdPairs = set()
            process_preserved_file(config, bs_to_bp)
            bs_to_bp = filter_by_blacklist(bs_to_bp, set(), set())
            assert ("SAMD00000010", "PRJDB10010") in bs_to_bp

    def test_blacklist_only_pair_is_excluded(self, tmp_path: Path, clean_ctx: None) -> None:
        """preserved には無いが blacklist にある accession を含むペアは除外。"""
        const_dir = tmp_path / "const"
        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            _write_preserved(const_dir, [])
            bs_to_bp: IdPairs = {("SAMD00000020", "PRJDB10020")}
            process_preserved_file(config, bs_to_bp)
            bs_to_bp = filter_by_blacklist(bs_to_bp, {"PRJDB10020"}, set())
            assert ("SAMD00000020", "PRJDB10020") not in bs_to_bp

    def test_blacklist_overrides_preserved_via_biosample(self, tmp_path: Path, clean_ctx: None) -> None:
        """blacklist が biosample 側にあるケースでも preserved を上書きする。"""
        const_dir = tmp_path / "const"
        config = Config(result_dir=tmp_path, const_dir=const_dir)
        with run_logger(config=config):
            _write_preserved(const_dir, ["SAMD00000030\tPRJDB10030"])
            bs_to_bp: IdPairs = set()
            process_preserved_file(config, bs_to_bp)
            bs_to_bp = filter_by_blacklist(bs_to_bp, set(), {"SAMD00000030"})
            assert ("SAMD00000030", "PRJDB10030") not in bs_to_bp


class TestInsdcPrecedence:
    """INSDC 側の preserved → blacklist の順 (insdc.py::main 相当) も同じ仕様。"""

    def test_blacklist_overrides_insdc_preserved(self, tmp_path: Path, clean_ctx: None) -> None:
        # _load_insdc_preserved_file は file I/O を伴うので、ここでは
        # 合成された IdPairs を直接組み立てて挙動を pin する。
        # SPEC: insdc.py の main で「preserved 読み込み → filter_pairs_by_blacklist」順
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        with run_logger(config=config):
            insdc_to_bp: IdPairs = {("AB000001", "PRJDB99001"), ("AB000002", "PRJDB99002")}
            filtered = filter_pairs_by_blacklist(insdc_to_bp, {"PRJDB99001"}, "right")
            assert ("AB000001", "PRJDB99001") not in filtered
            assert ("AB000002", "PRJDB99002") in filtered

    def test_no_blacklist_preserves_all(self, tmp_path: Path, clean_ctx: None) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        with run_logger(config=config):
            insdc_to_bp: IdPairs = {("AB000010", "PRJDB99010"), ("AB000020", "PRJDB99020")}
            filtered = filter_pairs_by_blacklist(insdc_to_bp, set(), "right")
            assert filtered == insdc_to_bp
