"""sra/paths.py のテスト。"""
from pathlib import Path
from unittest.mock import MagicMock

from ddbj_search_converter.sra.paths import get_sra_tar_dir


class TestGetSraTarDir:
    """get_sra_tar_dir 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.const_dir = Path("/data/const")

        result = get_sra_tar_dir(mock_config)

        assert result == Path("/data/const/sra")

    def test_path_is_path_object(self) -> None:
        mock_config = MagicMock()
        mock_config.const_dir = Path("/tmp/test")

        result = get_sra_tar_dir(mock_config)

        assert isinstance(result, Path)
