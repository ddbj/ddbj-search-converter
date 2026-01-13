from pathlib import Path
from typing import Set

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.bp_umbrella import filter_by_blacklist, process_xml_file
from ddbj_search_converter.logging.logger import run_logger


class TestProcessXmlFile:
    """process_xml_file のテスト"""

    def test_extract_topadmin_relations(self, tmp_path: Path) -> None:
        """TopAdmin 関連が正しく抽出されること"""
        xml_content = """<?xml version="1.0"?>
<PackageSet>
  <Package>
    <Project>
      <ProjectDescr>
        <Link>
          <Hierarchical type="TopAdmin">
            <ProjectIDRef accession="PRJNA123" />
            <MemberID accession="PRJNA456" />
          </Hierarchical>
        </Link>
      </ProjectDescr>
    </Project>
  </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content)

        results = process_xml_file(xml_path)
        assert results == [("PRJNA123", "PRJNA456")]

    def test_skip_topsingle_relations(self, tmp_path: Path) -> None:
        """TopSingle 関連がスキップされること"""
        xml_content = """<?xml version="1.0"?>
<PackageSet>
  <Package>
    <Project>
      <ProjectDescr>
        <Link>
          <Hierarchical type="TopSingle">
            <ProjectIDRef accession="PRJNA123" />
            <MemberID accession="PRJNA456" />
          </Hierarchical>
        </Link>
      </ProjectDescr>
    </Project>
  </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content)

        results = process_xml_file(xml_path)
        assert results == []

    def test_multiple_relations(self, tmp_path: Path) -> None:
        """複数の関連が正しく抽出されること"""
        xml_content = """<?xml version="1.0"?>
<PackageSet>
  <Package>
    <Project>
      <ProjectDescr>
        <Link>
          <Hierarchical type="TopAdmin">
            <ProjectIDRef accession="PRJNA100" />
            <MemberID accession="PRJNA001" />
          </Hierarchical>
        </Link>
        <Link>
          <Hierarchical type="TopAdmin">
            <ProjectIDRef accession="PRJNA200" />
            <MemberID accession="PRJNA001" />
          </Hierarchical>
        </Link>
      </ProjectDescr>
    </Project>
  </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content)

        results = process_xml_file(xml_path)
        assert len(results) == 2
        assert ("PRJNA100", "PRJNA001") in results
        assert ("PRJNA200", "PRJNA001") in results

    def test_empty_xml(self, tmp_path: Path) -> None:
        """Link 要素がない XML でも例外が発生しないこと"""
        xml_content = """<?xml version="1.0"?>
<PackageSet>
  <Package>
    <Project>
      <ProjectDescr>
        <Name>Test Project</Name>
      </ProjectDescr>
    </Project>
  </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content)

        results = process_xml_file(xml_path)
        assert results == []

    def test_missing_accession_attributes(self, tmp_path: Path) -> None:
        """accession 属性がない場合はスキップされること"""
        xml_content = """<?xml version="1.0"?>
<PackageSet>
  <Package>
    <Project>
      <ProjectDescr>
        <Link>
          <Hierarchical type="TopAdmin">
            <ProjectIDRef />
            <MemberID accession="PRJNA456" />
          </Hierarchical>
        </Link>
      </ProjectDescr>
    </Project>
  </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content)

        results = process_xml_file(xml_path)
        assert results == []

    def test_mixed_relations(self, tmp_path: Path) -> None:
        """TopAdmin と TopSingle が混在する場合、TopAdmin のみ抽出されること"""
        xml_content = """<?xml version="1.0"?>
<PackageSet>
  <Package>
    <Project>
      <ProjectDescr>
        <Link>
          <Hierarchical type="TopAdmin">
            <ProjectIDRef accession="PRJNA100" />
            <MemberID accession="PRJNA001" />
          </Hierarchical>
        </Link>
        <Link>
          <Hierarchical type="TopSingle">
            <ProjectIDRef accession="PRJNA200" />
            <MemberID accession="PRJNA002" />
          </Hierarchical>
        </Link>
        <Link>
          <Hierarchical type="TopAdmin">
            <ProjectIDRef accession="PRJNA300" />
            <MemberID accession="PRJNA003" />
          </Hierarchical>
        </Link>
      </ProjectDescr>
    </Project>
  </Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_text(xml_content)

        results = process_xml_file(xml_path)
        assert len(results) == 2
        assert ("PRJNA100", "PRJNA001") in results
        assert ("PRJNA300", "PRJNA003") in results
        assert ("PRJNA200", "PRJNA002") not in results


class TestFilterByBlacklist:
    """filter_by_blacklist のテスト"""

    def test_filter_umbrella_in_blacklist(self, test_config: Config) -> None:
        """umbrella が blacklist に含まれる場合は除外されること"""
        primary_to_umbrella: Set[tuple[str, str]] = {
            ("PRJNA100", "PRJNA001"),
            ("PRJNA200", "PRJNA002"),
        }
        bp_blacklist: Set[str] = {"PRJNA100"}

        with run_logger(config=test_config):
            result = filter_by_blacklist(primary_to_umbrella, bp_blacklist)
        assert result == {("PRJNA200", "PRJNA002")}

    def test_filter_primary_in_blacklist(self, test_config: Config) -> None:
        """primary が blacklist に含まれる場合は除外されること"""
        primary_to_umbrella: Set[tuple[str, str]] = {
            ("PRJNA100", "PRJNA001"),
            ("PRJNA200", "PRJNA002"),
        }
        bp_blacklist: Set[str] = {"PRJNA001"}

        with run_logger(config=test_config):
            result = filter_by_blacklist(primary_to_umbrella, bp_blacklist)
        assert result == {("PRJNA200", "PRJNA002")}

    def test_filter_both_in_blacklist(self, test_config: Config) -> None:
        """umbrella と primary 両方が blacklist に含まれる場合は除外されること"""
        primary_to_umbrella: Set[tuple[str, str]] = {
            ("PRJNA100", "PRJNA001"),
            ("PRJNA200", "PRJNA002"),
            ("PRJNA300", "PRJNA003"),
        }
        bp_blacklist: Set[str] = {"PRJNA100", "PRJNA002"}

        with run_logger(config=test_config):
            result = filter_by_blacklist(primary_to_umbrella, bp_blacklist)
        assert result == {("PRJNA300", "PRJNA003")}

    def test_empty_blacklist(self, test_config: Config) -> None:
        """空の blacklist の場合は何も除外されないこと"""
        primary_to_umbrella: Set[tuple[str, str]] = {
            ("PRJNA100", "PRJNA001"),
            ("PRJNA200", "PRJNA002"),
        }
        bp_blacklist: Set[str] = set()

        with run_logger(config=test_config):
            result = filter_by_blacklist(primary_to_umbrella, bp_blacklist)
        assert result == primary_to_umbrella

    def test_all_filtered(self, test_config: Config) -> None:
        """全ての関連が blacklist でフィルタされる場合"""
        primary_to_umbrella: Set[tuple[str, str]] = {
            ("PRJNA100", "PRJNA001"),
        }
        bp_blacklist: Set[str] = {"PRJNA100"}

        with run_logger(config=test_config):
            result = filter_by_blacklist(primary_to_umbrella, bp_blacklist)
        assert result == set()
