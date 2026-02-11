"""Shared fixtures for tests."""

from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import _ctx


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
