"""Tests for ddbj_search_converter.es.status_sync module."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_mock import MockerFixture

from ddbj_search_converter.config import Config
from ddbj_search_converter.es import status_sync
from ddbj_search_converter.es.status_sync import (
    load_ssot_statuses,
    resolve_indexes,
    sync_index_status,
    sync_status,
)


def _make_config(tmp_path: Path) -> Config:
    return Config(result_dir=tmp_path, const_dir=tmp_path / "const")


def _setup_status_cache(config: Config, bp_rows: list[tuple[str, str]], bs_rows: list[tuple[str, str]]) -> None:
    db_path = config.result_dir / "bp_bs_status.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
        for table, rows in (("bp_status", bp_rows), ("bs_status", bs_rows)):
            conn.execute(f"CREATE TABLE {table} (accession TEXT NOT NULL, status TEXT NOT NULL)")
            if rows:
                conn.executemany(f"INSERT INTO {table} VALUES (?, ?)", rows)


def _setup_dra_db(config: Config, rows: list[tuple[str, str, str | None]]) -> None:
    """(Accession, Type, Status) から dra_accessions.duckdb を作る。"""
    db_path = config.const_dir / "sra" / "dra_accessions.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as conn:
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
                Updated     TIMESTAMPTZ,
                Published   TIMESTAMPTZ,
                Received    TIMESTAMPTZ
            )
        """)
        conn.executemany(
            "INSERT INTO accessions VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, NULL, NULL)",
            rows,
        )


class TestResolveIndexes:
    def test_group_expands(self) -> None:
        assert resolve_indexes("sra") == [
            "sra-submission",
            "sra-study",
            "sra-experiment",
            "sra-run",
            "sra-sample",
            "sra-analysis",
        ]

    def test_single_index(self) -> None:
        assert resolve_indexes("sra-run") == ["sra-run"]

    def test_all_includes_bp_bs_and_sra(self) -> None:
        result = resolve_indexes("all")
        assert "bioproject" in result
        assert "biosample" in result
        assert "sra-run" in result

    def test_unsupported_index_raises(self) -> None:
        """jga / gea は status の SSOT を持たないので対象外。"""
        with pytest.raises(ValueError, match="unsupported index"):
            resolve_indexes("jga-study")


class TestLoadSsotStatuses:
    def test_bp_returns_non_public_and_all(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        _setup_status_cache(
            config,
            bp_rows=[("PRJDB1", "public"), ("PRJDB2", "suppressed"), ("PRJDB3", "withdrawn")],
            bs_rows=[],
        )
        non_public, all_accessions = load_ssot_statuses(config, "bioproject")

        assert non_public == {"PRJDB2": "suppressed", "PRJDB3": "withdrawn"}
        assert all_accessions == {"PRJDB1", "PRJDB2", "PRJDB3"}

    def test_sra_normalizes_status(self, tmp_path: Path) -> None:
        """Accessions.tab の値は JSONL 生成と同じ normalize_status を通す。"""
        config = _make_config(tmp_path)
        _setup_dra_db(
            config,
            [
                ("DRA000001", "SUBMISSION", "public"),
                ("DRA000002", "SUBMISSION", "suppressed"),
                ("DRA000003", "SUBMISSION", "withdrawn"),
                ("DRA000004", "SUBMISSION", "unpublished"),
                ("DRR000001", "RUN", "suppressed"),
            ],
        )
        non_public, all_accessions = load_ssot_statuses(config, "sra-submission")

        assert non_public == {
            "DRA000002": "suppressed",
            "DRA000003": "withdrawn",
            "DRA000004": "private",
        }
        # RUN 行は sra-submission の対象外
        assert all_accessions == {"DRA000001", "DRA000002", "DRA000003", "DRA000004"}

    def test_sra_duplicate_status_uses_priority(self, tmp_path: Path) -> None:
        """同一 accession が複数 status で現れたら JSONL 生成と同じ priority で決める。"""
        config = _make_config(tmp_path)
        _setup_dra_db(
            config,
            [
                ("DRA000001", "SUBMISSION", "suppressed"),
                ("DRA000001", "SUBMISSION", "public"),
            ],
        )
        non_public, _ = load_ssot_statuses(config, "sra-submission")

        # public のほうが強いので non-public には出ない
        assert non_public == {}

    def test_missing_status_cache_raises(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        with pytest.raises(FileNotFoundError):
            load_ssot_statuses(config, "bioproject")

    def test_missing_dra_db_raises(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        with pytest.raises(FileNotFoundError):
            load_ssot_statuses(config, "sra-run")


def _patch_es(
    mocker: MockerFixture,
    es_non_public: dict[str, str],
    mget_docs: dict[str, str | None],
) -> tuple[Any, list[list[dict[str, Any]]]]:
    """fetch_es_non_public / mget / bulk を差し替える。

    戻り値は (es_client mock, bulk に渡された actions のリスト)。
    """
    mocker.patch.object(status_sync, "fetch_es_non_public", return_value=es_non_public)

    docs = []
    for accession, status in mget_docs.items():
        if status is None:
            docs.append({"_id": accession, "found": False})
        else:
            docs.append({"_id": accession, "found": True, "_source": {"status": status}})

    client = mocker.MagicMock()
    client.mget.return_value = {"docs": docs}
    client.options.return_value = client
    mocker.patch.object(status_sync, "get_es_client", return_value=client)

    captured: list[list[dict[str, Any]]] = []

    def fake_bulk(_client: Any, actions: Any, **_kwargs: Any) -> tuple[int, list[Any]]:
        materialized = list(actions)
        captured.append(materialized)
        return len(materialized), []

    mocker.patch.object(status_sync.helpers, "bulk", side_effect=fake_bulk)

    return client, captured


@pytest.mark.usefixtures("with_logger_isolated")
class TestSyncIndexStatus:
    def test_updates_when_es_is_stale(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """SSOT が suppressed で ES が public なら update する。"""
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB1", "public"), ("PRJDB2", "suppressed")], bs_rows=[])
        _, captured = _patch_es(mocker, es_non_public={}, mget_docs={"PRJDB2": "public"})

        result = sync_index_status(config, "bioproject")

        assert result.updated == 1
        assert captured[0] == [
            {"_op_type": "update", "_index": "bioproject", "_id": "PRJDB2", "doc": {"status": "suppressed"}}
        ]

    def test_no_update_when_already_in_sync(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "suppressed")], bs_rows=[])
        _, captured = _patch_es(mocker, es_non_public={"PRJDB2": "suppressed"}, mget_docs={})

        result = sync_index_status(config, "bioproject")

        assert result.updated == 0
        assert captured == []

    def test_restores_public_when_ssot_went_back(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """ES が suppressed で SSOT が public に戻っていれば public に直す。

        SSOT 側の non-public だけを見ていると取りこぼすケース。
        """
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "public")], bs_rows=[])
        _, captured = _patch_es(mocker, es_non_public={"PRJDB2": "suppressed"}, mget_docs={})

        result = sync_index_status(config, "bioproject")

        assert result.updated == 1
        assert captured[0] == [
            {"_op_type": "update", "_index": "bioproject", "_id": "PRJDB2", "doc": {"status": "public"}}
        ]

    def test_ignores_accession_absent_from_ssot(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """SSOT に無い accession は触らない (消すか private にするかは別の判断)。"""
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB1", "public")], bs_rows=[])
        _, captured = _patch_es(mocker, es_non_public={"PRJDB999": "suppressed"}, mget_docs={})

        result = sync_index_status(config, "bioproject")

        assert result.checked == 0
        assert result.updated == 0
        assert captured == []

    def test_counts_missing_docs(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """ES に doc が無いものは update せず missing として数える。"""
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "suppressed")], bs_rows=[])
        _, captured = _patch_es(mocker, es_non_public={}, mget_docs={"PRJDB2": None})

        result = sync_index_status(config, "bioproject")

        assert result.missing == 1
        assert result.updated == 0
        assert captured == []

    def test_doc_without_status_is_updated(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """status フィールドを持たない doc も SSOT の値に合わせる。"""
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "suppressed")], bs_rows=[])
        client = mocker.MagicMock()
        client.mget.return_value = {"docs": [{"_id": "PRJDB2", "found": True, "_source": {}}]}
        client.options.return_value = client
        mocker.patch.object(status_sync, "get_es_client", return_value=client)
        mocker.patch.object(status_sync, "fetch_es_non_public", return_value={})
        captured: list[list[dict[str, Any]]] = []
        mocker.patch.object(
            status_sync.helpers,
            "bulk",
            side_effect=lambda _c, actions, **_k: (len(captured.append(list(actions)) or captured[-1]), []),
        )

        result = sync_index_status(config, "bioproject")

        assert result.updated == 1
        assert captured[0][0]["doc"] == {"status": "suppressed"}

    def test_dry_run_does_not_write(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "suppressed")], bs_rows=[])
        _, captured = _patch_es(mocker, es_non_public={}, mget_docs={"PRJDB2": "public"})

        result = sync_index_status(config, "bioproject", dry_run=True)

        assert result.updated == 1  # 差分件数として報告する
        assert captured == []

    def test_target_suffix_is_used_for_writes(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Blue-Green では alias ではなく日付付きの物理 index を更新する。"""
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "suppressed")], bs_rows=[])
        client, captured = _patch_es(mocker, es_non_public={}, mget_docs={"PRJDB2": "public"})

        sync_index_status(config, "bioproject", target_suffix="20260525")

        assert captured[0][0]["_index"] == "bioproject-20260525"
        assert client.mget.call_args.kwargs["index"] == "bioproject-20260525"


@pytest.mark.usefixtures("with_logger_isolated")
class TestSyncStatus:
    def test_runs_each_index(self, tmp_path: Path, mocker: MockerFixture) -> None:
        config = _make_config(tmp_path)
        _setup_status_cache(config, bp_rows=[("PRJDB2", "suppressed")], bs_rows=[("SAMD1", "suppressed")])
        _patch_es(mocker, es_non_public={}, mget_docs={"PRJDB2": "public", "SAMD1": "public"})

        results = sync_status(config, "bioproject")
        assert [r.index for r in results] == ["bioproject"]
