"""Tests for ddbj_search_converter.dblink.bioproject module."""
from pathlib import Path

import pytest

from ddbj_search_converter.dblink.bioproject import (
    normalize_hum_id, process_bioproject_xml_file)


class TestNormalizeHumId:
    """Tests for normalize_hum_id function."""

    def test_simple_hum_id(self) -> None:
        """バージョンなしの hum-id はそのまま返す。"""
        assert normalize_hum_id("hum0001") == "hum0001"
        assert normalize_hum_id("hum0257") == "hum0257"
        assert normalize_hum_id("hum9999") == "hum9999"

    def test_hum_id_with_version(self) -> None:
        """バージョン付きの hum-id はバージョンを除去する。"""
        assert normalize_hum_id("hum0001.v2") == "hum0001"
        assert normalize_hum_id("hum0257.v1") == "hum0257"
        assert normalize_hum_id("hum0123.v10") == "hum0123"

    def test_hum_id_with_other_suffix(self) -> None:
        """その他のサフィックスも除去する。"""
        assert normalize_hum_id("hum0001.abc") == "hum0001"


class TestProcessBioprojectXmlFile:
    """Tests for process_bioproject_xml_file function."""

    def test_extracts_hum_id_from_local_id(self, tmp_path: Path) -> None:
        """LocalID から hum-id を抽出する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB11024" archive="DDBJ" />
                    <LocalID submission_id="J-DS000307-005">NBDC</LocalID>
                    <LocalID submission_id="hum0257">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.hum_id) == 1
        assert ("PRJDB11024", "hum0257") in result.hum_id
        assert len(result.umbrella) == 0
        assert len(result.skipped_accessions) == 0

    def test_extracts_umbrella_relations(self, tmp_path: Path) -> None:
        """TopAdmin リンクから umbrella 関連を抽出する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB00001" archive="DDBJ" />
                </ProjectID>
            </Project>
        </Project>
        <ProjectLinks>
            <Link>
                <Hierarchical type="TopAdmin">
                    <ProjectIDRef accession="PRJDB99999" />
                    <MemberID accession="PRJDB00001" />
                </Hierarchical>
            </Link>
        </ProjectLinks>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.umbrella) == 1
        assert ("PRJDB00001", "PRJDB99999") in result.umbrella
        assert len(result.hum_id) == 0

    def test_extracts_both_relations(self, tmp_path: Path) -> None:
        """1回のパースで umbrella と hum-id の両方を抽出する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB00001" archive="DDBJ" />
                    <LocalID submission_id="hum0001">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
        <ProjectLinks>
            <Link>
                <Hierarchical type="TopAdmin">
                    <ProjectIDRef accession="PRJDB99999" />
                    <MemberID accession="PRJDB00001" />
                </Hierarchical>
            </Link>
        </ProjectLinks>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.umbrella) == 1
        assert ("PRJDB00001", "PRJDB99999") in result.umbrella
        assert len(result.hum_id) == 1
        assert ("PRJDB00001", "hum0001") in result.hum_id

    def test_ignores_top_single(self, tmp_path: Path) -> None:
        """TopSingle リンクは無視する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB00001" archive="DDBJ" />
                </ProjectID>
            </Project>
        </Project>
        <ProjectLinks>
            <Link>
                <Hierarchical type="TopSingle">
                    <ProjectIDRef accession="PRJDB99999" />
                    <MemberID accession="PRJDB00001" />
                </Hierarchical>
            </Link>
        </ProjectLinks>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.umbrella) == 0

    def test_normalizes_hum_id_with_version(self, tmp_path: Path) -> None:
        """バージョン付き hum-id を正規化する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB99999" archive="DDBJ" />
                    <LocalID submission_id="hum0001.v2">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.hum_id) == 1
        assert ("PRJDB99999", "hum0001") in result.hum_id

    def test_processes_multiple_packages(self, tmp_path: Path) -> None:
        """複数の Package を処理する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB00001" archive="DDBJ" />
                    <LocalID submission_id="hum0001">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB00002" archive="DDBJ" />
                    <LocalID submission_id="hum0002">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.hum_id) == 2
        assert ("PRJDB00001", "hum0001") in result.hum_id
        assert ("PRJDB00002", "hum0002") in result.hum_id

    def test_skips_invalid_accession(self, tmp_path: Path) -> None:
        """PRJ で始まらない accession はスキップする。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="INVALID123" archive="DDBJ" />
                    <LocalID submission_id="hum0001">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.hum_id) == 0
        assert "INVALID123" in result.skipped_accessions

    def test_case_insensitive_hum_prefix(self, tmp_path: Path) -> None:
        """hum の大文字小文字を区別しない。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJDB33333" archive="DDBJ" />
                    <LocalID submission_id="HUM0001">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.hum_id) == 1
        assert ("PRJDB33333", "HUM0001") in result.hum_id

    def test_empty_package_set(self, tmp_path: Path) -> None:
        """空の PackageSet を処理する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.hum_id) == 0
        assert len(result.umbrella) == 0
        assert len(result.skipped_accessions) == 0
