import inspect
import sys
import traceback
from contextvars import ContextVar
from datetime import date, datetime
from pathlib import Path
from secrets import token_hex
from typing import Any, Dict, Optional

from ddbj_search_converter.config import (LOCAL_TZ, LOG_DIR_NAME, TODAY,
                                          TODAY_STR, Config, default_config)
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
) -> None:
    if config is None:
        config = default_config
    run_id = f"{TODAY_STR}_{run_name}_{token_hex(2)}"
    log_file = config.work_dir.joinpath(LOG_DIR_NAME, f"{run_id}.log.jsonl")

    _run_name.set(run_name)
    _run_id.set(run_id)
    _run_date.set(TODAY)
    _log_file.set(log_file)

    log_file.parent.mkdir(parents=True, exist_ok=True)


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
        timestamp=datetime.now(LOCAL_TZ),
        run_date=run_date,  # type: ignore
        run_id=run_id,  # type: ignore
        run_name=run_name,  # type: ignore
        source=source,
        event=event,
        message=message,
        target=target,
        error=error_info,
        extra=extra or {},
    )

    _append_jsonl(log_file, record)  # type: ignore
    _emit_stderr(record)


def _append_jsonl(path: Path, record: LogRecord) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(record.model_dump_json())
        f.write("\n")


def _emit_stderr(record: LogRecord) -> None:
    try:
        event = record.event
        if event in ["start", "end", "failed", "progress"]:
            return

        ts = record.timestamp.isoformat(timespec="seconds")
        run = record.run_name
        msg = record.message or ""

        line = f"{ts} - {run} - {event}"
        if msg:
            line += f" - {msg}"

        sys.stderr.write(line + "\n")
        sys.stderr.flush()

    except Exception:
        pass


def _detect_source() -> str:
    frame = inspect.currentframe()
    try:
        if frame is None:
            return "<unknown>"

        caller = frame.f_back
        if caller is None:
            return "<unknown>"

        module = inspect.getmodule(caller)
        if module and module.__name__:
            return module.__name__

        return "<unknown>"
    finally:
        del frame
