"""Common fixtures for tests."""
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config


@pytest.fixture
def test_config(tmp_path: Path) -> Config:
    """Create a Config with tmp_path as result_dir."""
    return Config(
        result_dir=tmp_path,
        const_dir=tmp_path.joinpath("const"),
    )
