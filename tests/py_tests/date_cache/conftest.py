"""Shared fixtures for date_cache tests."""

from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import run_logger


@pytest.fixture(autouse=True)
def _with_logger(tmp_path: Path) -> Generator[None, None, None]:
    """Ensure logger is initialized for all date_cache tests."""
    config = Config(result_dir=tmp_path)
    with run_logger(config=config):
        yield
