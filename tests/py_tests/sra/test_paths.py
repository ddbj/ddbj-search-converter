"""sra/paths.py のテスト。

実 Config を使う方針。MagicMock(config) は本物の属性契約 (result_dir / const_dir)
を bypass して任意の属性アクセスを許してしまうので、API 表面が変わったときの
回帰検出が効かない。
"""

from pathlib import Path

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra.paths import get_sra_tar_dir


class TestGetSraTarDir:
    """get_sra_tar_dir 関数のテスト。"""

    def test_returns_result_dir_joined_with_sra_tar(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        result = get_sra_tar_dir(config)
        assert result == tmp_path / "sra_tar"

    def test_path_is_path_object(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        result = get_sra_tar_dir(config)
        assert isinstance(result, Path)

    def test_path_includes_only_sra_tar_suffix(self, tmp_path: Path) -> None:
        """``sra_tar`` 以外の prefix / suffix が混入していない。"""
        config = Config(result_dir=tmp_path)
        result = get_sra_tar_dir(config)
        assert result.name == "sra_tar"
        assert result.parent == tmp_path
