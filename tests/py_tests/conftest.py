"""Shared fixtures for tests."""

from collections.abc import Generator
from pathlib import Path

import pytest
from hypothesis import settings

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import _ctx, run_logger

settings.register_profile("default", deadline=None)
settings.load_profile("default")


@pytest.fixture
def test_config(tmp_path: Path) -> Config:
    """Create a Config with tmp_path as result_dir."""
    return Config(
        result_dir=tmp_path,
        const_dir=tmp_path.joinpath("const"),
    )


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


@pytest.fixture
def with_logger_isolated(tmp_path: Path) -> Generator[None, None, None]:
    """Wrap a test body in ``run_logger`` and reset ``_ctx`` afterwards.

    Use this for tests / sub-conftests that need ``log_info`` / ``log_debug``
    callable in their body.  The explicit ``_ctx.set(None)`` after ``yield``
    is critical for ``-n auto`` parallel runs: an exception in the test body
    would otherwise leave the ContextVar polluted for the next test on the
    same worker (run_logger's __exit__ runs but a stale finalize path could
    leave ``_ctx`` set with the wrong run_id).
    """
    config = Config(result_dir=tmp_path)
    try:
        with run_logger(config=config):
            yield
    finally:
        _ctx.set(None)
