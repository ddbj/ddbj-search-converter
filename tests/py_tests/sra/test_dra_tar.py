"""sra/dra_tar.py のテスト。

実 ``Config`` を使う方針 (MagicMock(config) は属性契約の検証を bypass する)。
"""

from pathlib import Path

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra.dra_tar import (
    get_dra_accessions_db_path,
    get_dra_last_updated_path,
    get_dra_tar_path,
    get_dra_xml_dir_path,
)


class TestGetDraTarPath:
    def test_returns_result_dir_suffixed(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        result = get_dra_tar_path(config)
        assert result == tmp_path / "sra_tar" / "DRA_Metadata.tar"


class TestGetDraLastUpdatedPath:
    def test_returns_result_dir_suffixed(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        result = get_dra_last_updated_path(config)
        assert result == tmp_path / "sra_tar" / "dra_last_updated.txt"


class TestGetDraAccessionsDbPath:
    def test_returns_const_dir_suffixed(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        result = get_dra_accessions_db_path(config)
        assert result == tmp_path / "const" / "sra" / "dra_accessions.duckdb"


class TestGetDraXmlDirPath:
    """get_dra_xml_dir_path は global 固定 path を返す (Config を取らない)。"""

    def test_returns_correct_path(self) -> None:
        result = get_dra_xml_dir_path("DRA000001")
        assert result == Path("/usr/local/resources/dra/fastq/DRA000/DRA000001")

    def test_different_submission(self) -> None:
        result = get_dra_xml_dir_path("DRA123456")
        assert result == Path("/usr/local/resources/dra/fastq/DRA123/DRA123456")
