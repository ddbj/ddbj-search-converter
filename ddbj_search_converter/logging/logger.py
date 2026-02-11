import inspect
import sys
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from secrets import token_hex
from typing import Any

from ddbj_search_converter.config import LOCAL_TZ, LOG_DIR_NAME, TODAY, TODAY_STR, Config, default_config
from ddbj_search_converter.logging.schema import ErrorInfo, Extra, LoggerContext, LogLevel, LogRecord

_ctx: ContextVar[LoggerContext | None] = ContextVar("_ctx", default=None)


def _get_ctx() -> LoggerContext:
    ctx = _ctx.get()
    if ctx is None:
        raise RuntimeError("logger is not initialized")
    return ctx


def _infer_run_name() -> str:
    """
    run_name を推測する。
    1. sys.argv[0] から取得を試みる
    2. ダメなら inspect でモジュール名から推測
    3. それでもダメなら "adhoc"
    """
    # 1. sys.argv から
    if sys.argv:
        name = Path(sys.argv[0]).name
        if name not in ("pytest", "python", "python3", "__main__", ""):
            return name

    # 2. inspect でモジュール名から
    frame = inspect.currentframe()
    try:
        for _ in range(3):
            if frame is not None:
                frame = frame.f_back
        if frame is not None:
            module = inspect.getmodule(frame)
            if module and module.__name__:
                name = module.__name__.split(".")[-1]
                if name != "__main__":
                    return name
    finally:
        del frame

    # 3. fallback
    return "adhoc"


def init_logger(
    *,
    run_name: str,
    config: Config | None = None,
) -> None:
    if config is None:
        config = default_config
    hex_token = token_hex(2)
    run_id = f"{TODAY_STR}_{run_name}_{hex_token}"
    log_file = config.result_dir.joinpath(LOG_DIR_NAME, TODAY_STR, f"{run_name}_{hex_token}.log.jsonl")

    ctx = LoggerContext(
        run_name=run_name,
        run_id=run_id,
        run_date=TODAY,
        log_file=log_file,
        config=config,
    )
    _ctx.set(ctx)

    log_file.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def run_logger(
    *,
    run_name: str | None = None,
    config: Config | None = None,
) -> Iterator[None]:
    """
    Context manager for logging a run.
    Automatically handles start/end/failed logging and finalization.

    Args:
        run_name: Run name. If omitted, inferred automatically.
        config: Config. If omitted, default config is used.
    """
    if run_name is None:
        run_name = _infer_run_name()

    init_logger(run_name=run_name, config=config)
    log_start()
    try:
        yield
        log_end()
    except Exception as e:
        log_failed(e)
        raise
    finally:
        finalize_logger()


def log(
    *,
    log_level: LogLevel,
    message: str | None = None,
    error: BaseException | ErrorInfo | None = None,
    extra: dict[str, Any] | Extra | None = None,
) -> None:
    ctx = _get_ctx()
    source = _detect_source()

    if extra is not None and not isinstance(extra, Extra):
        extra = Extra(**extra)

    error_info: ErrorInfo | None = None
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
        run_date=ctx.run_date,
        run_id=ctx.run_id,
        run_name=ctx.run_name,
        source=source,
        log_level=log_level,
        message=message,
        error=error_info,
        extra=extra or Extra(),
    )

    _append_jsonl(ctx.log_file, record)
    _emit_stderr(record)


def _append_jsonl(path: Path, record: LogRecord) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(record.model_dump_json(exclude_none=True))
        f.write("\n")


def _emit_stderr(record: LogRecord) -> None:
    """INFO 以上のログを stderr に出力する。DEBUG は出力しない。"""
    try:
        if record.log_level == "DEBUG":
            return

        ts = record.timestamp.isoformat(timespec="seconds")
        run = record.run_name
        level = record.log_level
        msg = record.message or ""

        line = f"{ts} - {run} - {level}"
        if msg:
            line += f" - {msg}"

        # extra から file, accession を抽出して追記
        extras = []
        if record.extra.file:
            extras.append(f"file={record.extra.file}")
        if record.extra.accession:
            extras.append(f"accession={record.extra.accession}")
        if extras:
            line += f" [{', '.join(extras)}]"

        sys.stderr.write(line + "\n")
        sys.stderr.flush()

    except Exception:
        pass


def _detect_source() -> str:
    frame = inspect.currentframe()
    try:
        while frame:
            module = inspect.getmodule(frame)
            if module and module.__name__:
                name = module.__name__
                # Skip logging module and stdlib contextlib
                if not name.startswith("ddbj_search_converter.logging") and name != "contextlib":
                    return name
            frame = frame.f_back
        return "<unknown>"
    finally:
        del frame


def _convert_path_to_str(kwargs: dict[str, Any]) -> None:
    """Convert Path objects in kwargs to strings."""
    if "file" in kwargs and hasattr(kwargs["file"], "__fspath__"):
        kwargs["file"] = str(kwargs["file"])


def log_debug(message: str, **kwargs: Any) -> None:
    """DEBUG level log. Pass file, accession, etc. via kwargs."""
    _convert_path_to_str(kwargs)
    extra = Extra(**kwargs) if kwargs else None
    log(log_level="DEBUG", message=message, extra=extra)


def log_info(message: str, **kwargs: Any) -> None:
    """INFO level log. Pass file, accession, etc. via kwargs."""
    _convert_path_to_str(kwargs)
    extra = Extra(**kwargs) if kwargs else None
    log(log_level="INFO", message=message, extra=extra)


def log_warn(message: str, **kwargs: Any) -> None:
    """WARNING level log. Pass file, accession, etc. via kwargs."""
    _convert_path_to_str(kwargs)
    extra = Extra(**kwargs) if kwargs else None
    log(log_level="WARNING", message=message, extra=extra)


def log_error(
    message: str,
    error: BaseException | None = None,
    **kwargs: Any,
) -> None:
    """ERROR level log. Pass file, accession, etc. via kwargs."""
    _convert_path_to_str(kwargs)
    extra = Extra(**kwargs) if kwargs else None
    log(log_level="ERROR", message=message, error=error, extra=extra)


def log_start(message: str = "") -> None:
    """Log run start. Sets lifecycle=start in extra."""
    ctx = _get_ctx()
    log(
        log_level="INFO",
        message=message or f"{ctx.run_name} started",
        extra=Extra(lifecycle="start"),
    )


def log_end(message: str = "") -> None:
    """Log run success. Sets lifecycle=end in extra."""
    ctx = _get_ctx()
    log(
        log_level="INFO",
        message=message or f"{ctx.run_name} completed",
        extra=Extra(lifecycle="end"),
    )


def log_failed(error: BaseException, message: str = "") -> None:
    """Log run failure. Sets lifecycle=failed in extra."""
    ctx = _get_ctx()
    log(
        log_level="CRITICAL",
        message=message or f"{ctx.run_name} failed",
        error=error,
        extra=Extra(lifecycle="failed"),
    )


def finalize_logger() -> None:
    """
    Bulk insert JSONL file to DuckDB.
    Call at run end.
    """
    from ddbj_search_converter.logging.db import insert_log_records

    ctx = _ctx.get()
    if ctx is None:
        raise RuntimeError("logger is not initialized")

    if ctx.log_file.exists():
        insert_log_records(ctx.config, ctx.log_file)
