"""sra/ncbi_tar.py のテスト。"""

from pathlib import Path
from unittest.mock import MagicMock

from ddbj_search_converter.sra.ncbi_tar import (
    get_ncbi_daily_tar_gz_url,
    get_ncbi_full_tar_gz_url,
    get_ncbi_last_merged_path,
    get_ncbi_tar_path,
)


class TestGetNcbiFullTarGzUrl:
    """get_ncbi_full_tar_gz_url 関数のテスト。"""

    def test_returns_correct_url(self) -> None:
        result = get_ncbi_full_tar_gz_url("20240115")
        expected = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/NCBI_SRA_Metadata_Full_20240115.tar.gz"
        assert result == expected

    def test_different_date(self) -> None:
        result = get_ncbi_full_tar_gz_url("20231201")
        assert "20231201" in result
        assert "Full" in result


class TestGetNcbiDailyTarGzUrl:
    """get_ncbi_daily_tar_gz_url 関数のテスト。"""

    def test_returns_correct_url(self) -> None:
        result = get_ncbi_daily_tar_gz_url("20240115")
        expected = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/NCBI_SRA_Metadata_20240115.tar.gz"
        assert result == expected

    def test_daily_does_not_contain_full(self) -> None:
        result = get_ncbi_daily_tar_gz_url("20240115")
        assert "Full" not in result


class TestGetNcbiTarPath:
    """get_ncbi_tar_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.result_dir = Path("/data/result")

        result = get_ncbi_tar_path(mock_config)

        assert result == Path("/data/result/sra_tar/NCBI_SRA_Metadata.tar")


class TestGetNcbiLastMergedPath:
    """get_ncbi_last_merged_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.result_dir = Path("/data/result")

        result = get_ncbi_last_merged_path(mock_config)

        assert result == Path("/data/result/sra_tar/ncbi_last_merged.txt")
