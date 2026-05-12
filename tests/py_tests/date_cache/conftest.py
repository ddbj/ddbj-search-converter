"""Shared fixtures for date_cache tests."""

from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import _ctx, run_logger


@pytest.fixture(autouse=True)
def _with_logger(tmp_path: Path) -> Generator[None, None, None]:
    """Ensure logger is initialized for all date_cache tests.

    `_ctx.set(None)` in the finally block guarantees the ContextVar is reset
    even if the test body or `run_logger.__exit__` leaves it dirty — required
    for `-n auto` parallel safety.
    """
    config = Config(result_dir=tmp_path)
    try:
        with run_logger(config=config):
            yield
    finally:
        _ctx.set(None)
