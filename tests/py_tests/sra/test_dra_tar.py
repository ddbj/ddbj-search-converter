"""sra/dra_tar.py のテスト。"""

from pathlib import Path
from unittest.mock import MagicMock

from ddbj_search_converter.sra.dra_tar import (
    get_dra_accessions_db_path,
    get_dra_last_updated_path,
    get_dra_tar_path,
    get_dra_xml_dir_path,
)


class TestGetDraTarPath:
    """get_dra_tar_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.result_dir = Path("/data/result")

        result = get_dra_tar_path(mock_config)

        assert result == Path("/data/result/sra_tar/DRA_Metadata.tar")


class TestGetDraLastUpdatedPath:
    """get_dra_last_updated_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.result_dir = Path("/data/result")

        result = get_dra_last_updated_path(mock_config)

        assert result == Path("/data/result/sra_tar/dra_last_updated.txt")


class TestGetDraAccessionsDbPath:
    """get_dra_accessions_db_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.const_dir = Path("/data/const")

        result = get_dra_accessions_db_path(mock_config)

        assert result == Path("/data/const/sra/dra_accessions.duckdb")


class TestGetDraXmlDirPath:
    """get_dra_xml_dir_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        result = get_dra_xml_dir_path("DRA000001")

        assert result == Path("/usr/local/resources/dra/fastq/DRA000/DRA000001")

    def test_different_submission(self) -> None:
        result = get_dra_xml_dir_path("DRA123456")

        assert result == Path("/usr/local/resources/dra/fastq/DRA123/DRA123456")
