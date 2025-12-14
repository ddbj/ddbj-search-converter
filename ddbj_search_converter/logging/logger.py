from __future__ import annotations

import inspect
import sys
import traceback
from contextvars import ContextVar
from datetime import date, datetime
from pathlib import Path
from secrets import token_hex
from typing import Any, Dict, Optional

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.schema import (ErrorInfo, Event, LogRecord,
                                                  RunName, Target)

_run_name: ContextVar[Optional[RunName]] = ContextVar("_run_name", default=None)
_run_id: ContextVar[Optional[str]] = ContextVar("_run_id", default=None)
_run_date: ContextVar[Optional[date]] = ContextVar("_run_date", default=None)
_log_file: ContextVar[Optional[Path]] = ContextVar("_log_file", default=None)


def init_logger(
    *,
    run_name: RunName,
    config: Optional[Config] = None,
    run_date: Optional[date] = None,
) -> None:
    if run_date is None:
        run_date = date.today()

    run_id = f"{run_date:%Y%m%d}_{run_name}_{token_hex(2)}"

    _run_name.set(run_name)
    _run_id.set(run_id)
    _run_date.set(run_date)
    _log_file.set(log_file)

    log_file.parent.mkdir(parents=True, exist_ok=True)

    log(event="start", message=f"run started: {run_name}")


# ----------------------------------------------------------------------
# Logging API
# ----------------------------------------------------------------------

def log(
    *,
    event: Event,
    message: Optional[str] = None,
    target: Optional[Dict[str, Any] | Target] = None,
    error: Optional[BaseException | ErrorInfo] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    run_name = _run_name.get()
    run_id = _run_id.get()
    run_date = _run_date.get()
    log_file = _log_file.get()

    if not all([run_name, run_id, run_date, log_file]):
        raise RuntimeError("logger is not initialized (init_run not called)")

    source = _detect_source()

    if target is not None and not isinstance(target, Target):
        target = Target(**target)

    error_info: Optional[ErrorInfo] = None
    if error is not None:
        if isinstance(error, ErrorInfo):
            error_info = error
        else:
            error_info = ErrorInfo(
                type=type(error).__name__,
                message=str(error),
                traceback=traceback.format_exc(),
            )

    record = LogRecord(
        timestamp=datetime.utcnow(),
        run_date=run_date,
        run_id=run_id,
        run_name=run_name,
        source=source,
        event=event,
        message=message,
        target=target,
        error=error_info,
        extra=extra or {},
    )

    _append_jsonl(log_file, record)
    _emit_stderr(record)


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------

def _append_jsonl(path: Path, record: LogRecord) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(record.model_dump_json(ensure_ascii=False))
        f.write("\n")


def _emit_stderr(record: LogRecord) -> None:
    """
    人間向けの簡易ログを stderr に出す。
    失敗しても例外は投げない。
    """
    try:
        parts = [
            record.timestamp.isoformat(timespec="seconds"),
            record.run_name,
            record.run_id,
            record.event,
            record.source,
        ]

        if record.message:
            parts.append(record.message)

        if record.target:
            t = record.target
            target_bits = []
            if t.file:
                target_bits.append(f"file={t.file}")
            if t.accession:
                target_bits.append(f"accession={t.accession}")
            if t.index:
                target_bits.append(f"index={t.index}")
            if target_bits:
                parts.append(f"[{' '.join(target_bits)}]")

        if record.error:
            parts.append(f"{record.error.type}: {record.error.message}")

        sys.stderr.write(" | ".join(parts) + "\n")
        sys.stderr.flush()
    except Exception:
        # stderr 出力失敗では処理を止めない
        pass


def _detect_source() -> str:
    frame = inspect.currentframe()
    try:
        caller = frame.f_back.f_back
        module = inspect.getmodule(caller)
        if module and module.__name__:
            return module.__name__
        return "<unknown>"
    finally:
        del frame
