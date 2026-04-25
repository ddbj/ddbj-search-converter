"""Tests for ddbj_search_converter.logging.db module."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from ddbj_search_converter.config import LOG_DB_FILE_NAME, Config
from ddbj_search_converter.logging import db as db_module
from ddbj_search_converter.logging.db import (
    get_last_successful_run_date,
    init_log_db,
    insert_log_records,
)


def _make_config(result_dir: Path) -> Config:
    return Config(result_dir=result_dir)


def _record(
    *,
    timestamp: str = "2026-04-25T12:00:00+09:00",
    run_date: str = "2026-04-25",
    run_id: str = "20260425_test_aaaa",
    run_name: str = "test_run",
    source: str = "tests.module",
    log_level: str = "INFO",
    message: str | None = "msg",
    error: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "timestamp": timestamp,
        "run_date": run_date,
        "run_id": run_id,
        "run_name": run_name,
        "source": source,
        "log_level": log_level,
    }
    if message is not None:
        rec["message"] = message
    if error is not None:
        rec["error"] = error
    if extra is not None:
        rec["extra"] = extra
    return rec


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec))
            f.write("\n")


class TestInitLogDb:
    """init_log_db のスキーマと冪等性。"""

    def test_creates_table_with_expected_columns(self, tmp_path: Path) -> None:
        """log_records テーブルが想定 9 カラムで作成される。"""
        config = _make_config(tmp_path)
        init_log_db(config)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            cols = conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'log_records'"
            ).fetchall()

        col_names = {row[0] for row in cols}
        assert col_names == {
            "timestamp",
            "run_date",
            "run_id",
            "run_name",
            "source",
            "log_level",
            "message",
            "error",
            "extra",
        }

    def test_creates_indexes_on_lookup_columns(self, tmp_path: Path) -> None:
        """run_name / run_date / log_level の index が登録される。"""
        config = _make_config(tmp_path)
        init_log_db(config)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            rows = conn.execute("SELECT index_name FROM duckdb_indexes() WHERE table_name = 'log_records'").fetchall()

        index_names = {row[0] for row in rows}
        assert "idx_run_name" in index_names
        assert "idx_run_date" in index_names
        assert "idx_log_level" in index_names

    def test_idempotent(self, tmp_path: Path) -> None:
        """2 回呼んでも例外にならない (CREATE TABLE/INDEX IF NOT EXISTS)。"""
        config = _make_config(tmp_path)

        init_log_db(config)
        init_log_db(config)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM log_records").fetchone()
        assert count is not None
        assert count[0] == 0

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        """db_path.parent が存在しなくても作成する。"""
        nested = tmp_path / "nested" / "deeper"
        config = _make_config(nested)

        init_log_db(config)

        assert (nested / LOG_DB_FILE_NAME).exists()


class TestInsertLogRecords:
    """insert_log_records の I/O と JSON カラムの永続化。"""

    def test_empty_file_creates_db_but_no_records(self, tmp_path: Path) -> None:
        """空ファイルでも DB は init される (実装の挙動を固定)。レコードは 0。"""
        config = _make_config(tmp_path)
        jsonl = tmp_path / "empty.jsonl"
        jsonl.write_text("", encoding="utf-8")

        insert_log_records(config, jsonl)

        assert (tmp_path / LOG_DB_FILE_NAME).exists()
        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM log_records").fetchone()
        assert count is not None
        assert count[0] == 0

    def test_blank_lines_are_skipped(self, tmp_path: Path) -> None:
        """空行 / 空白行は skip し、有効行のみ insert される。"""
        config = _make_config(tmp_path)
        jsonl = tmp_path / "blanks.jsonl"
        rec = _record(run_name="r1")
        jsonl.write_text(
            "\n  \n" + json.dumps(rec) + "\n\n",
            encoding="utf-8",
        )

        insert_log_records(config, jsonl)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM log_records").fetchone()
        assert count is not None
        assert count[0] == 1

    def test_persists_error_and_extra_as_json(self, tmp_path: Path) -> None:
        """error / extra ありのレコードは JSON で保持され、再 parse で同値。"""
        config = _make_config(tmp_path)
        jsonl = tmp_path / "rec.jsonl"
        rec = _record(
            run_name="r1",
            error={"type": "ValueError", "message": "boom"},
            extra={"lifecycle": "start"},
        )
        _write_jsonl(jsonl, [rec])

        insert_log_records(config, jsonl)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            row = conn.execute("SELECT message, error, extra FROM log_records").fetchone()
        assert row is not None
        message, error_json, extra_json = row
        assert message == "msg"
        assert json.loads(error_json) == {"type": "ValueError", "message": "boom"}
        assert json.loads(extra_json) == {"lifecycle": "start"}

    def test_null_error_extra_become_db_null(self, tmp_path: Path) -> None:
        """error / extra が None のレコードは DB で NULL (json 文字列 'null' にしない)。"""
        config = _make_config(tmp_path)
        jsonl = tmp_path / "rec.jsonl"
        _write_jsonl(jsonl, [_record(run_name="r1")])

        insert_log_records(config, jsonl)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            row = conn.execute("SELECT error, extra FROM log_records").fetchone()
        assert row is not None
        assert row[0] is None
        assert row[1] is None

    def test_db_initialized_on_first_insert(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """DB ファイル不存在時に init_log_db が 1 回だけ自動で走る。"""
        config = _make_config(tmp_path)
        assert not (tmp_path / LOG_DB_FILE_NAME).exists()

        jsonl = tmp_path / "rec.jsonl"
        _write_jsonl(jsonl, [_record()])

        spy = mocker.spy(db_module, "init_log_db")
        insert_log_records(config, jsonl)

        assert (tmp_path / LOG_DB_FILE_NAME).exists()
        # 不存在時に 1 回だけ init される
        assert spy.call_count == 1

    def test_db_existing_skips_init(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """DB が既に存在するとき insert_log_records は init を呼ばない。"""
        config = _make_config(tmp_path)
        init_log_db(config)
        assert (tmp_path / LOG_DB_FILE_NAME).exists()

        jsonl = tmp_path / "rec.jsonl"
        _write_jsonl(jsonl, [_record()])

        spy = mocker.spy(db_module, "init_log_db")
        insert_log_records(config, jsonl)

        assert spy.call_count == 0

    @pytest.mark.parametrize(
        "tricky_message",
        [
            "改行\nを含む",
            'クォート"を含む',
            "バックスラッシュ\\を含む",
            "絵文字 🦜 と 𓀀",
            "タブ\tと CR\r",
        ],
    )
    def test_unicode_and_special_chars_round_trip(self, tmp_path: Path, tricky_message: str) -> None:
        """error/extra に改行/Unicode/エスケープ対象を含めても DB 経由で round-trip 可能。"""
        config = _make_config(tmp_path)
        jsonl = tmp_path / "rec.jsonl"
        rec = _record(
            run_name="r1",
            error={"type": "ValueError", "message": tricky_message},
            extra={"file": tricky_message, "lifecycle": "start"},
        )
        _write_jsonl(jsonl, [rec])

        insert_log_records(config, jsonl)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            row = conn.execute("SELECT error, extra FROM log_records").fetchone()
        assert row is not None
        error_obj = json.loads(row[0])
        extra_obj = json.loads(row[1])
        assert error_obj["message"] == tricky_message
        assert extra_obj["file"] == tricky_message

    def test_appends_to_existing_db_with_distinct_run_ids(self, tmp_path: Path) -> None:
        """既存 DB に追記すると count が増え、run_id 識別が保たれる (truncate されない)。"""
        config = _make_config(tmp_path)
        jsonl_a = tmp_path / "a.jsonl"
        jsonl_b = tmp_path / "b.jsonl"
        _write_jsonl(jsonl_a, [_record(run_id="id_a")])
        _write_jsonl(jsonl_b, [_record(run_id="id_b")])

        insert_log_records(config, jsonl_a)
        insert_log_records(config, jsonl_b)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM log_records").fetchone()
            ids = conn.execute("SELECT run_id FROM log_records ORDER BY run_id").fetchall()
        assert count is not None
        assert count[0] == 2
        assert [r[0] for r in ids] == ["id_a", "id_b"]

    @given(n=st.integers(min_value=0, max_value=50))
    @settings(max_examples=10, deadline=None)
    def test_count_matches_input(self, tmp_path_factory: pytest.TempPathFactory, n: int) -> None:
        """N 件書き込んだら N 件入る (PBT)。"""
        tmp_path = tmp_path_factory.mktemp("count")
        config = _make_config(tmp_path)
        jsonl = tmp_path / "rec.jsonl"
        records = [_record(run_id=f"id_{i}") for i in range(n)]
        _write_jsonl(jsonl, records)

        insert_log_records(config, jsonl)

        with duckdb.connect(str(tmp_path / LOG_DB_FILE_NAME), read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM log_records").fetchone()
        assert count is not None
        assert count[0] == n


class TestGetLastSuccessfulRunDate:
    """get_last_successful_run_date のフィルタ条件。"""

    def _populate(self, config: Config, records: list[dict[str, Any]]) -> None:
        jsonl = config.result_dir / "tmp.jsonl"
        _write_jsonl(jsonl, records)
        insert_log_records(config, jsonl)

    def test_db_not_exists_returns_none(self, tmp_path: Path) -> None:
        """DB ファイル不存在なら None (init を走らせない)。"""
        config = _make_config(tmp_path)

        result = get_last_successful_run_date(config, "anything")

        assert result is None
        assert not (tmp_path / LOG_DB_FILE_NAME).exists()

    def test_picks_only_lifecycle_end_with_info(self, tmp_path: Path) -> None:
        """lifecycle=end かつ log_level=INFO のレコードだけが対象。start/failed/CRITICAL は除外。"""
        config = _make_config(tmp_path)
        self._populate(
            config,
            [
                _record(
                    run_name="my_run",
                    run_date="2026-04-01",
                    log_level="INFO",
                    extra={"lifecycle": "start"},
                ),
                _record(
                    run_name="my_run",
                    run_date="2026-04-02",
                    log_level="INFO",
                    extra={"lifecycle": "end"},
                ),
                _record(
                    run_name="my_run",
                    run_date="2026-04-03",
                    log_level="CRITICAL",
                    extra={"lifecycle": "failed"},
                ),
            ],
        )

        result = get_last_successful_run_date(config, "my_run")

        assert result == date(2026, 4, 2)

    def test_excludes_other_run_names(self, tmp_path: Path) -> None:
        """別 run_name の成功レコードは対象外。"""
        config = _make_config(tmp_path)
        self._populate(
            config,
            [
                _record(
                    run_name="other_run",
                    run_date="2026-04-15",
                    log_level="INFO",
                    extra={"lifecycle": "end"},
                ),
            ],
        )

        result = get_last_successful_run_date(config, "my_run")

        assert result is None

    def test_returns_max_date_among_successes(self, tmp_path: Path) -> None:
        """複数の成功 run があれば最新の run_date を返す。"""
        config = _make_config(tmp_path)
        self._populate(
            config,
            [
                _record(run_name="my_run", run_date="2026-04-01", extra={"lifecycle": "end"}),
                _record(run_name="my_run", run_date="2026-04-15", extra={"lifecycle": "end"}),
                _record(run_name="my_run", run_date="2026-04-10", extra={"lifecycle": "end"}),
            ],
        )

        result = get_last_successful_run_date(config, "my_run")

        assert result == date(2026, 4, 15)

    def test_no_successful_run_returns_none(self, tmp_path: Path) -> None:
        """failed のみだと None。"""
        config = _make_config(tmp_path)
        self._populate(
            config,
            [
                _record(
                    run_name="my_run",
                    run_date="2026-04-02",
                    log_level="CRITICAL",
                    extra={"lifecycle": "failed"},
                ),
            ],
        )

        result = get_last_successful_run_date(config, "my_run")

        assert result is None

    def test_warning_level_with_lifecycle_end_excluded(self, tmp_path: Path) -> None:
        """log_level=WARNING で lifecycle=end でも除外される (INFO のみが対象)。"""
        config = _make_config(tmp_path)
        self._populate(
            config,
            [
                _record(
                    run_name="my_run",
                    run_date="2026-04-05",
                    log_level="WARNING",
                    extra={"lifecycle": "end"},
                ),
            ],
        )

        result = get_last_successful_run_date(config, "my_run")

        assert result is None
