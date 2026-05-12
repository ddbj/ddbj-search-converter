"""``finalize_dblink_db`` 後の ``dbxref`` 半辺化不変条件を end-to-end で verify。

``raw_edges`` への書き込み (各 dblink モジュールが行う) は ``load_to_db`` を
直接呼ぶことで模倣し、``finalize_dblink_db`` を実行して
``assert_dbxref_symmetric`` で構造的不変条件を assert する。

SPEC: docs/data-architecture.md §DBLink DB の半辺化スキーマ。
本テストは「実 dblink モジュール pipeline 全体」の代理ではなく、
**半辺化スキーマの contract** を pin することが目的。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import (
    finalize_dblink_db,
    init_dblink_db,
    load_to_db,
)
from ddbj_search_converter.logging.logger import _ctx, init_logger

from ._dbxref_assertions import assert_dbxref_symmetric, count_canonical_edges

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def dblink_config(tmp_path: Path) -> Config:
    config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
    config.const_dir.joinpath("dblink").mkdir(parents=True, exist_ok=True)
    return config


@pytest.fixture(autouse=True)
def _setup_logger(dblink_config: Config) -> "Iterator[None]":
    init_logger(run_name="test_dbxref_invariant_e2e", config=dblink_config)
    yield
    _ctx.set(None)


class TestSimpleTwoEdges:
    """2 つの canonical edge → 4 行 dbxref (対称展開)。"""

    def test_insdc_bioproject(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("AB000001", "PRJDB12345"), ("CP035466", "PRJNA999999")},
            "insdc",
            "bioproject",
        )
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)
        assert count_canonical_edges(dblink_config) == 2

    def test_bioproject_biosample(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("SAMD00000001", "PRJDB1"), ("SAMN12345678", "PRJNA2"), ("SAME001", "PRJEB3")},
            "biosample",
            "bioproject",
        )
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)
        assert count_canonical_edges(dblink_config) == 3

    def test_jga_humandbs(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("JGAS000001", "hum0001"), ("JGAS000002", "hum0002")},
            "jga-study",
            "humandbs",
        )
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)
        assert count_canonical_edges(dblink_config) == 2

    def test_gea_bioproject(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("E-GEAD-1", "PRJDB1"), ("E-GEAD-2", "PRJDB2")},
            "gea",
            "bioproject",
        )
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)

    def test_metabobank_biosample(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("MTBKS1", "SAMD00000001"), ("MTBKS2", "SAMN12345678")},
            "metabobank",
            "biosample",
        )
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)


class TestMultipleEdgeTypes:
    """複数の edge type を混在させても対称性 + 重複なし。"""

    def test_combined_insdc_jga_gea(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(dblink_config, {("AB000001", "PRJDB1")}, "insdc", "bioproject")
        load_to_db(dblink_config, {("AB000001", "SAMD00000001")}, "insdc", "biosample")
        load_to_db(dblink_config, {("JGAS000001", "hum0001")}, "jga-study", "humandbs")
        load_to_db(dblink_config, {("E-GEAD-1", "SAMD00000001")}, "gea", "biosample")
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)
        assert count_canonical_edges(dblink_config) == 4

    def test_duplicate_canonical_edges_deduped(self, dblink_config: Config) -> None:
        """同じ canonical edge を複数回 load しても dbxref には 2 行だけ。"""
        init_dblink_db(dblink_config)
        load_to_db(dblink_config, {("AB000001", "PRJDB1")}, "insdc", "bioproject")
        # 同じ edge を再度 load (load_to_db は idempotent ではないが DISTINCT で集約される)
        load_to_db(dblink_config, {("AB000001", "PRJDB1")}, "insdc", "bioproject")
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)
        # 結果は 1 unique edge → 2 行
        assert count_canonical_edges(dblink_config) == 1


class TestSelfLoopHandling:
    """同 type 同 accession の self-loop は許容ケース付きで検証する。"""

    def test_bioproject_self_loop_via_umbrella_like(self, dblink_config: Config) -> None:
        # umbrella DAG は別 DB に持つので dbxref では出現しないはずだが、
        # 仮に同 type 同 accession が出現したときの挙動を pin する。
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("PRJDB1", "PRJDB1")},
            "bioproject",
            "bioproject",
        )
        finalize_dblink_db(dblink_config)
        # self-loop が存在するシナリオ
        assert_dbxref_symmetric(dblink_config, allow_self_loops=True)

    def test_no_self_loops_when_all_distinct(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        load_to_db(
            dblink_config,
            {("AB000001", "PRJDB1"), ("AB000002", "PRJDB2")},
            "insdc",
            "bioproject",
        )
        finalize_dblink_db(dblink_config)
        # self-loop なしで pass する
        assert_dbxref_symmetric(dblink_config, allow_self_loops=False)


class TestEmptyRawEdges:
    """raw_edges 空でも finalize は通り、dbxref は 0 行。"""

    def test_empty_finalize(self, dblink_config: Config) -> None:
        init_dblink_db(dblink_config)
        finalize_dblink_db(dblink_config)
        assert_dbxref_symmetric(dblink_config)
        assert count_canonical_edges(dblink_config) == 0
