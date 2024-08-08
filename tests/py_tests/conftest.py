import os
import shutil
import sys
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Generator

import pytest


@lru_cache(maxsize=None)
def package_root() -> Path:
    root = Path(__file__).parent
    while not root.joinpath("pyproject.toml").exists():
        root = root.parent
    return root


@pytest.fixture(scope="session", autouse=True)
def reset_argv() -> Generator[None, None, None]:
    original_argv = sys.argv[:]
    sys.argv = ["ddbj_search_converter"]

    yield

    sys.argv = original_argv


@pytest.fixture(scope="session", autouse=True)
def reset_os_env() -> Generator[None, None, None]:
    original_os_env = {k: v for k, v in os.environ.items() if k.startswith("DDBJ_SEARCH_CONVERTER_")}
    keys = original_os_env.keys()
    for k in keys:
        del os.environ[k]

    yield

    for k in keys:
        os.environ[k] = original_os_env[k]


@pytest.fixture()
def tmpdir() -> Generator[Path, None, None]:
    tempdir = tempfile.mkdtemp()
    yield Path(tempdir)
    try:
        shutil.rmtree(tempdir)
    except Exception:
        pass
