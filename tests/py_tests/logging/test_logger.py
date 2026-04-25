"""Tests for ddbj_search_converter.logging.logger module."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from ddbj_search_converter.config import LOG_DIR_NAME, TODAY, TODAY_STR, Config
from ddbj_search_converter.logging import logger as logger_module
from ddbj_search_converter.logging.logger import (
    _convert_path_to_str,
    _ctx,
    _emit_stderr,
    _infer_run_name,
    finalize_logger,
    init_logger,
    log_debug,
    log_end,
    log_error,
    log_failed,
    log_info,
    log_start,
    log_warn,
    run_logger,
)
from ddbj_search_converter.logging.schema import Extra, LogRecord


def _make_config(result_dir: Path) -> Config:
    return Config(result_dir=result_dir)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _make_log_record(
    *,
    log_level: str = "INFO",
    message: str | None = "hello",
    extra: Extra | None = None,
) -> LogRecord:
    return LogRecord(
        timestamp=datetime(2026, 4, 25, 12, 0, 0),
        run_date=date(2026, 4, 25),
        run_id="20260425_my_run_aaaa",
        run_name="my_run",
        source="tests.module",
        log_level=log_level,  # type: ignore[arg-type]
        message=message,
        error=None,
        extra=extra or Extra(),
    )


class TestInitLogger:
    """init_logger の run_id / log_file 生成。"""

    def test_run_id_format(self, tmp_path: Path, clean_ctx: None) -> None:
        """run_id は {TODAY_STR}_{run_name}_{4 hex chars} 形式。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        ctx = _ctx.get()
        assert ctx is not None
        assert re.fullmatch(rf"{TODAY_STR}_my_run_[0-9a-f]{{4}}", ctx.run_id)

    def test_log_file_path(self, tmp_path: Path, clean_ctx: None) -> None:
        """log_file は result_dir/logs/{TODAY_STR}/{run_name}_{4 hex}.log.jsonl。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        ctx = _ctx.get()
        assert ctx is not None
        expected_dir = tmp_path / LOG_DIR_NAME / TODAY_STR
        assert ctx.log_file.parent == expected_dir
        assert ctx.log_file.name.startswith("my_run_")
        assert ctx.log_file.name.endswith(".log.jsonl")
        assert expected_dir.exists()

    def test_run_date_is_today(self, tmp_path: Path, clean_ctx: None) -> None:
        """run_date は TODAY と一致。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        ctx = _ctx.get()
        assert ctx is not None
        assert ctx.run_date == TODAY

    def test_repeated_init_changes_run_id(self, tmp_path: Path, clean_ctx: None) -> None:
        """同じ run_name で連続 init すると異なる run_id になる (token 衝突確率は無視)。"""
        config = _make_config(tmp_path)

        init_logger(run_name="my_run", config=config)
        ctx_a = _ctx.get()
        assert ctx_a is not None
        run_id_a = ctx_a.run_id

        init_logger(run_name="my_run", config=config)
        ctx_b = _ctx.get()
        assert ctx_b is not None

        assert run_id_a != ctx_b.run_id


class TestLogBeforeInit:
    """init していない状態で log を呼ぶと RuntimeError。"""

    def test_log_info_before_init_raises(self, clean_ctx: None) -> None:
        with pytest.raises(RuntimeError, match="logger is not initialized"):
            log_info("hello")


class TestRunLoggerLifecycle:
    """run_logger の context manager 挙動。"""

    def test_normal_run_emits_start_and_end(self, tmp_path: Path, clean_ctx: None) -> None:
        """正常終了で start / end が記録され、failed は出ない。"""
        config = _make_config(tmp_path)

        with run_logger(run_name="my_run", config=config):
            log_info("doing work")

        log_files = list((tmp_path / LOG_DIR_NAME / TODAY_STR).glob("my_run_*.log.jsonl"))
        assert len(log_files) == 1
        records = _read_jsonl(log_files[0])

        lifecycles = [r.get("extra", {}).get("lifecycle") for r in records]
        assert "start" in lifecycles
        assert "end" in lifecycles
        assert "failed" not in lifecycles

    def test_exception_emits_failed_and_reraises(self, tmp_path: Path, clean_ctx: None) -> None:
        """例外時に lifecycle=failed が記録され、元の例外が re-raise される。end は記録されない。"""
        config = _make_config(tmp_path)

        with pytest.raises(ValueError, match="boom"), run_logger(run_name="my_run", config=config):
            raise ValueError("boom")

        log_files = list((tmp_path / LOG_DIR_NAME / TODAY_STR).glob("my_run_*.log.jsonl"))
        assert len(log_files) == 1
        records = _read_jsonl(log_files[0])

        failed = [r for r in records if r.get("extra", {}).get("lifecycle") == "failed"]
        assert len(failed) == 1
        assert failed[0]["log_level"] == "CRITICAL"
        assert failed[0]["error"]["type"] == "ValueError"
        assert "boom" in failed[0]["error"]["message"]
        assert failed[0]["error"]["traceback"]

        ends = [r for r in records if r.get("extra", {}).get("lifecycle") == "end"]
        assert ends == []

    def test_finalize_called_on_exception(
        self,
        tmp_path: Path,
        clean_ctx: None,
        mocker: MockerFixture,
    ) -> None:
        """例外時も finally で finalize_logger が呼ばれる。"""
        config = _make_config(tmp_path)
        spy = mocker.spy(logger_module, "finalize_logger")

        with pytest.raises(RuntimeError), run_logger(run_name="my_run", config=config):
            raise RuntimeError("x")

        assert spy.call_count == 1

    def test_systemexit_skips_log_failed_but_finalizes(
        self,
        tmp_path: Path,
        clean_ctx: None,
    ) -> None:
        """SystemExit は except Exception では catch されない (failed 出ない)、finally は走る。"""
        config = _make_config(tmp_path)

        with pytest.raises(SystemExit), run_logger(run_name="my_run", config=config):
            raise SystemExit(1)

        log_files = list((tmp_path / LOG_DIR_NAME / TODAY_STR).glob("my_run_*.log.jsonl"))
        assert len(log_files) == 1
        records = _read_jsonl(log_files[0])
        failed = [r for r in records if r.get("extra", {}).get("lifecycle") == "failed"]
        ends = [r for r in records if r.get("extra", {}).get("lifecycle") == "end"]
        assert failed == []
        assert ends == []

    def test_run_logger_inferred_run_name_is_nonempty(self, tmp_path: Path, clean_ctx: None) -> None:
        """run_name 省略時に inferred name で動作する (空文字でない)。"""
        config = _make_config(tmp_path)

        with run_logger(config=config):
            ctx = _ctx.get()
            assert ctx is not None
            assert ctx.run_name


class TestLogStartEndFailed:
    """log_start / log_end / log_failed の出力フォーマット。"""

    def test_log_start_default_message(self, tmp_path: Path, clean_ctx: None) -> None:
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_start()

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        starts = [r for r in records if r.get("extra", {}).get("lifecycle") == "start"]
        assert len(starts) == 1
        assert starts[0]["message"] == "my_run started"
        assert starts[0]["log_level"] == "INFO"

    def test_log_end_default_message(self, tmp_path: Path, clean_ctx: None) -> None:
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_end()

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        ends = [r for r in records if r.get("extra", {}).get("lifecycle") == "end"]
        assert len(ends) == 1
        assert ends[0]["message"] == "my_run completed"

    def test_log_failed_carries_error_and_critical_level(self, tmp_path: Path, clean_ctx: None) -> None:
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        try:
            raise FileNotFoundError("xfile")
        except FileNotFoundError as e:
            log_failed(e)

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        failed = [r for r in records if r.get("extra", {}).get("lifecycle") == "failed"]
        assert len(failed) == 1
        assert failed[0]["log_level"] == "CRITICAL"
        assert failed[0]["message"] == "my_run failed"
        assert failed[0]["error"]["type"] == "FileNotFoundError"
        assert "xfile" in failed[0]["error"]["message"]


class TestLogLevelHelpers:
    """log_debug / log_info / log_warn / log_error の Path 変換と extra。"""

    def test_log_info_path_to_str(self, tmp_path: Path, clean_ctx: None) -> None:
        """file=Path(...) を渡すと JSONL 上は str。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_info("msg", file=Path("/some/path/file.xml"))

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        assert any(r.get("extra", {}).get("file") == "/some/path/file.xml" for r in records)

    def test_log_warn_uses_warning_level(self, tmp_path: Path, clean_ctx: None) -> None:
        """log_warn は WARNING (Python logging と同じ表記)。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_warn("careful")

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        warnings = [r for r in records if r["log_level"] == "WARNING"]
        assert len(warnings) == 1
        assert warnings[0]["message"] == "careful"

    def test_log_error_serializes_exception(self, tmp_path: Path, clean_ctx: None) -> None:
        """log_error(error=Exception(...)) は ErrorInfo に変換される。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        try:
            raise ValueError("oops")
        except ValueError as e:
            log_error("during something", error=e)

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        errors = [r for r in records if r["log_level"] == "ERROR"]
        assert len(errors) == 1
        assert errors[0]["message"] == "during something"
        assert errors[0]["error"]["type"] == "ValueError"
        assert "oops" in errors[0]["error"]["message"]

    def test_log_debug_no_kwargs_yields_empty_extra(self, tmp_path: Path, clean_ctx: None) -> None:
        """kwargs なしでは extra フィールドはすべて None なので JSONL の extra は空 dict。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_debug("dbg")

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        debugs = [r for r in records if r["log_level"] == "DEBUG"]
        assert len(debugs) == 1
        # exclude_none=True なので Extra の reserved field は全て省略され、extra は {} になる
        assert debugs[0].get("extra") == {}


class TestConvertPathToStr:
    """_convert_path_to_str: kwargs の file が Path → str。"""

    def test_path_converted(self) -> None:
        kwargs: dict[str, Any] = {"file": Path("/x/y")}
        _convert_path_to_str(kwargs)
        assert kwargs["file"] == "/x/y"
        assert isinstance(kwargs["file"], str)

    def test_string_unchanged(self) -> None:
        kwargs: dict[str, Any] = {"file": "/x/y"}
        _convert_path_to_str(kwargs)
        assert kwargs["file"] == "/x/y"

    def test_no_file_key_unchanged(self) -> None:
        kwargs: dict[str, Any] = {"accession": "PRJDB1"}
        _convert_path_to_str(kwargs)
        assert kwargs == {"accession": "PRJDB1"}


class TestEmitStderr:
    """_emit_stderr の出力フォーマット。"""

    def test_debug_not_emitted(self, capsys: pytest.CaptureFixture[str]) -> None:
        rec = _make_log_record(log_level="DEBUG", message="quiet")
        _emit_stderr(rec)
        err = capsys.readouterr().err
        assert err == ""

    def test_info_format(self, capsys: pytest.CaptureFixture[str]) -> None:
        """INFO は '<ts> - <run_name> - INFO - <msg>' 形式で改行で終わる。"""
        rec = _make_log_record(log_level="INFO", message="hello")
        _emit_stderr(rec)
        err = capsys.readouterr().err
        assert "my_run" in err
        assert "INFO" in err
        assert "hello" in err
        assert err.endswith("\n")

    def test_extra_file_accession_appended(self, capsys: pytest.CaptureFixture[str]) -> None:
        """extra.file / accession があれば末尾に [file=..., accession=...] が付く。"""
        rec = _make_log_record(
            log_level="INFO",
            extra=Extra(file="/x/y", accession="PRJDB1"),
        )
        _emit_stderr(rec)
        err = capsys.readouterr().err
        assert "file=/x/y" in err
        assert "accession=PRJDB1" in err

    def test_other_extras_not_in_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        """file / accession 以外の extra は stderr 行に出さない。"""
        rec = _make_log_record(
            log_level="INFO",
            message="hi",
            extra=Extra(table="t", row=3),
        )
        _emit_stderr(rec)
        err = capsys.readouterr().err
        assert "table=" not in err
        assert "row=" not in err


class TestDetectSource:
    """_detect_source の skip ルール。"""

    def test_skips_logging_module_and_returns_caller(self, tmp_path: Path, clean_ctx: None) -> None:
        """source は ddbj_search_converter.logging.* で始まらず、テストモジュール名が拾われる。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_info("from test")

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        sources = [r["source"] for r in records if r.get("message") == "from test"]
        assert sources
        for src in sources:
            assert not src.startswith("ddbj_search_converter.logging")
            # positive: pytest がこのモジュールを import するときの末尾が "test_logger" であること
            assert src.endswith("test_logger")


class TestAppendJsonl:
    """JSONL ファイルへの append と exclude_none。"""

    def test_exclude_none_keys_omitted(self, tmp_path: Path, clean_ctx: None) -> None:
        """None フィールドは JSONL に出ない。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_info("hi")

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        rec = next(r for r in records if r.get("message") == "hi")
        assert "error" not in rec
        # extra は空 dict (Extra() の全 reserved field は None なので)
        assert rec.get("extra") == {}

    def test_appends_one_line_per_record(self, tmp_path: Path, clean_ctx: None) -> None:
        """各 log() 呼び出しで 1 行追加 (truncate されない)。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_info("a")
        log_info("b")
        log_info("c")

        ctx = _ctx.get()
        assert ctx is not None
        lines = ctx.log_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 3
        for line in lines:
            assert line.strip()
            json.loads(line)


class TestPathConversionAcrossLevels:
    """log_debug / log_info / log_warn / log_error 全てで Path → str 変換が共通動作する。"""

    @pytest.mark.parametrize(
        ("log_func", "expected_level"),
        [
            (log_debug, "DEBUG"),
            (log_info, "INFO"),
            (log_warn, "WARNING"),
            (log_error, "ERROR"),
        ],
    )
    def test_path_to_str_for_all_levels(
        self,
        tmp_path: Path,
        clean_ctx: None,
        log_func: Callable[..., None],
        expected_level: str,
    ) -> None:
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)

        log_func("msg", file=Path("/abc/def.txt"))

        ctx = _ctx.get()
        assert ctx is not None
        records = _read_jsonl(ctx.log_file)
        target = [r for r in records if r["log_level"] == expected_level]
        assert len(target) == 1
        assert target[0].get("extra", {}).get("file") == "/abc/def.txt"


class TestEmitStderrSilentExceptions:
    """_emit_stderr は内部例外を pass で握りつぶす (logger 自身が落ちないこと)。"""

    def test_isoformat_failure_is_silent(self, capsys: pytest.CaptureFixture[str]) -> None:
        """timestamp.isoformat が raise しても _emit_stderr は raise しない。"""

        class BrokenTimestamp:
            def isoformat(self, **kwargs: Any) -> str:
                raise RuntimeError("broken")

        rec = LogRecord.model_construct(
            timestamp=BrokenTimestamp(),
            run_date=date(2026, 4, 25),
            run_id="20260425_my_run_aaaa",
            run_name="my_run",
            source="tests.module",
            log_level="INFO",
            message="hi",
            error=None,
            extra=Extra(),
        )

        # 例外を raise しないこと自体が assertion (raise されたら test が fail)
        _emit_stderr(rec)

        # 部分書き込みが flush されていないことを保証 (try ブロックの早い段階で死ぬ実装)
        err = capsys.readouterr().err
        assert err == ""


class TestInferRunName:
    """_infer_run_name の 3 段階 fallback。"""

    def test_uses_sys_argv_when_meaningful(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """sys.argv[0] のファイル名がブラックリスト (pytest/python/__main__/'') 以外ならそれを返す。"""
        monkeypatch.setattr("sys.argv", ["/usr/local/bin/my_tool"])

        assert _infer_run_name() == "my_tool"

    @pytest.mark.parametrize("blocked", ["pytest", "python", "python3", "__main__", ""])
    def test_skips_blocked_argv_names(
        self, monkeypatch: pytest.MonkeyPatch, blocked: str
    ) -> None:
        """sys.argv[0] が block list なら branch 1 を skip し、別経路の値を返す。"""
        monkeypatch.setattr("sys.argv", [blocked])

        result = _infer_run_name()

        # branch 1 で返した値ではない
        assert result != blocked
        # かつ空ではない (branch 2 か branch 3 の値)
        assert result

    def test_falls_back_to_adhoc(
        self, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """sys.argv が pytest かつ inspect.currentframe が None を返す場合 "adhoc"。"""
        monkeypatch.setattr("sys.argv", ["pytest"])
        mocker.patch(
            "ddbj_search_converter.logging.logger.inspect.currentframe",
            return_value=None,
        )

        assert _infer_run_name() == "adhoc"


class TestRunIdProperty:
    """run_id は任意の run_name に対して {TODAY_STR}_{run_name}_{4 hex} 形式 (PBT)。"""

    @given(
        run_name=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_-"),
            min_size=1,
            max_size=30,
        )
    )
    @settings(max_examples=20, deadline=None)
    def test_run_id_format_for_any_run_name(
        self, tmp_path_factory: pytest.TempPathFactory, run_name: str
    ) -> None:
        tmp_path = tmp_path_factory.mktemp("pbt_run_id")
        config = _make_config(tmp_path)
        try:
            init_logger(run_name=run_name, config=config)

            ctx = _ctx.get()
            assert ctx is not None
            prefix = f"{TODAY_STR}_{run_name}_"
            assert ctx.run_id.startswith(prefix)
            suffix = ctx.run_id[len(prefix) :]
            assert re.fullmatch(r"[0-9a-f]{4}", suffix)
        finally:
            _ctx.set(None)


class TestFinalizeLogger:
    """finalize_logger の DuckDB 連携。"""

    def test_calls_insert_when_log_file_exists(
        self,
        tmp_path: Path,
        clean_ctx: None,
        mocker: MockerFixture,
    ) -> None:
        """log_file 存在時に insert_log_records が呼ばれる。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)
        log_info("write something")

        mock_insert = mocker.patch("ddbj_search_converter.logging.db.insert_log_records")

        finalize_logger()

        assert mock_insert.call_count == 1

    def test_skips_insert_when_log_file_missing(
        self,
        tmp_path: Path,
        clean_ctx: None,
        mocker: MockerFixture,
    ) -> None:
        """log_file 不存在 (一度も log されてない) なら insert_log_records は呼ばれない。"""
        config = _make_config(tmp_path)
        init_logger(run_name="my_run", config=config)
        ctx = _ctx.get()
        assert ctx is not None
        assert not ctx.log_file.exists()

        mock_insert = mocker.patch("ddbj_search_converter.logging.db.insert_log_records")

        finalize_logger()

        mock_insert.assert_not_called()

    def test_finalize_without_init_raises(self, clean_ctx: None) -> None:
        """init せずに finalize すると RuntimeError。"""
        with pytest.raises(RuntimeError, match="logger is not initialized"):
            finalize_logger()
