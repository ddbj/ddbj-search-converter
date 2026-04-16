"""Tests for ddbj_search_converter.dblink.bioproject module."""

from collections.abc import Generator
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.bioproject import normalize_humandbs, process_bioproject_xml_file
from ddbj_search_converter.logging.logger import _ctx, init_logger


@pytest.fixture(autouse=True)
def _init_logger(tmp_path: Path) -> Generator[None, None, None]:
    """Initialize logger for tests that call log_debug."""
    config = Config()
    config.result_dir = tmp_path
    init_logger(run_name="test", config=config)
    yield
    _ctx.set(None)


class TestNormalizeHumandbs:
    """Tests for normalize_humandbs function."""

    def test_simple_humandbs(self) -> None:
        """バージョンなしの humandbs はそのまま返す。"""
        assert normalize_humandbs("hum0001") == "hum0001"
        assert normalize_humandbs("hum0257") == "hum0257"
        assert normalize_humandbs("hum9999") == "hum9999"

    def test_humandbs_with_version(self) -> None:
        """バージョン付きの humandbs はバージョンを除去する。"""
        assert normalize_humandbs("hum0001.v2") == "hum0001"
        assert normalize_humandbs("hum0257.v1") == "hum0257"
        assert normalize_humandbs("hum0123.v10") == "hum0123"

    def test_humandbs_with_other_suffix(self) -> None:
        """その他のサフィックスも除去する。"""
        assert normalize_humandbs("hum0001.abc") == "hum0001"


class TestProcessBioprojectXmlFile:
    """Tests for process_bioproject_xml_file function."""

    def test_extracts_humandbs_from_local_id(self, tmp_path: Path) -> None:
        """LocalID から humandbs を抽出する。"""
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

        assert len(result.humandbs) == 1
        assert ("PRJDB11024", "hum0257") in result.humandbs
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
        assert len(result.humandbs) == 0

    def test_extracts_both_relations(self, tmp_path: Path) -> None:
        """1回のパースで umbrella と humandbs の両方を抽出する。"""
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
        assert len(result.humandbs) == 1
        assert ("PRJDB00001", "hum0001") in result.humandbs

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

    def test_normalizes_humandbs_with_version(self, tmp_path: Path) -> None:
        """バージョン付き humandbs を正規化する。"""
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

        assert len(result.humandbs) == 1
        assert ("PRJDB99999", "hum0001") in result.humandbs

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

        assert len(result.humandbs) == 2
        assert ("PRJDB00001", "hum0001") in result.humandbs
        assert ("PRJDB00002", "hum0002") in result.humandbs

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

        assert len(result.humandbs) == 0
        assert "INVALID123" in result.skipped_accessions

    def test_case_insensitive_hum_prefix(self, tmp_path: Path) -> None:
        """hum の大文字小文字を区別しない（小文字に正規化される）。"""
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

        assert len(result.humandbs) == 1
        assert ("PRJDB33333", "hum0001") in result.humandbs

    def test_rejects_invalid_humandbs_format(self, tmp_path: Path) -> None:
        """hum で始まるが humandbs パターンに合致しない値はスキップする。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJEB29894" archive="DDBJ" />
                    <LocalID submission_id="human skin metagenome">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJEB51075" archive="DDBJ" />
                    <LocalID submission_id="human_gut_euk_paper">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
    <Package>
        <Project>
            <Project>
                <ProjectID>
                    <ArchiveID accession="PRJEB8013" archive="DDBJ" />
                    <LocalID submission_id="Humanisation_of_the_mouse">NBDC</LocalID>
                </ProjectID>
            </Project>
        </Project>
    </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.humandbs) == 0

    def test_empty_package_set(self, tmp_path: Path) -> None:
        """空の PackageSet を処理する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")

        result = process_bioproject_xml_file(xml_path)

        assert len(result.humandbs) == 0
        assert len(result.umbrella) == 0
        assert len(result.skipped_accessions) == 0
