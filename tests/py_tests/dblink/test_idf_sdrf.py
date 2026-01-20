"""Tests for ddbj_search_converter.dblink.idf_sdrf module."""
from pathlib import Path

from ddbj_search_converter.dblink.idf_sdrf import (parse_idf_file,
                                                   parse_sdrf_file)


class TestParseIdfFile:
    """Tests for parse_idf_file function."""

    def test_extracts_bioproject_id(self, tmp_path: Path) -> None:
        """BioProject ID を正しく抽出する。"""
        idf_content = """Comment[GEAAccession]\tE-GEAD-291
MAGE-TAB Version\t1.1
Investigation Title\tTest Investigation
Comment[BioProject]\tPRJDB7770
Public Release Date\t2022-12-08
"""
        idf_path = tmp_path / "E-GEAD-291.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        result = parse_idf_file(idf_path)
        assert result == "PRJDB7770"

    def test_returns_none_when_no_bioproject(self, tmp_path: Path) -> None:
        """BioProject 行がない場合は None を返す。"""
        idf_content = """Comment[GEAAccession]\tE-GEAD-100
MAGE-TAB Version\t1.1
Investigation Title\tTest
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        result = parse_idf_file(idf_path)
        assert result is None

    def test_returns_none_when_bioproject_empty(self, tmp_path: Path) -> None:
        """BioProject の値が空の場合は None を返す。"""
        idf_content = """Comment[BioProject]\t
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        result = parse_idf_file(idf_path)
        assert result is None


class TestParseSdrfFile:
    """Tests for parse_sdrf_file function."""

    def test_extracts_biosample_ids(self, tmp_path: Path) -> None:
        """BioSample ID を正しく抽出する。"""
        sdrf_content = """Source Name\tCharacteristics[organism]\tComment[BioSample]\tComment[description]
AF\tHomo sapiens\tSAMD00093430\tSample 1
F1\tHomo sapiens\tSAMD00093431\tSample 2
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result == {"SAMD00093430", "SAMD00093431"}

    def test_returns_empty_when_no_biosample_column(self, tmp_path: Path) -> None:
        """Comment[BioSample] カラムがない場合は空 set を返す。"""
        sdrf_content = """Source Name\tCharacteristics[organism]\tComment[description]
AF\tHomo sapiens\tSample 1
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result == set()

    def test_skips_empty_values(self, tmp_path: Path) -> None:
        """空の値はスキップする。"""
        sdrf_content = """Source Name\tComment[BioSample]
AF\tSAMD00001
F1\t
G2\tSAMD00002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result == {"SAMD00001", "SAMD00002"}
