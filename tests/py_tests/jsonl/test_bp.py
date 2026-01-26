"""Tests for ddbj_search_converter.jsonl.bp module."""
from pathlib import Path
from typing import Any, Dict

import pytest

from ddbj_search_converter.jsonl.bp import (iterate_xml_packages,
                                            normalize_properties, parse_args,
                                            parse_description,
                                            parse_external_link, parse_grant,
                                            parse_object_type, parse_organism,
                                            parse_organization,
                                            parse_publication, parse_same_as,
                                            parse_status, parse_title,
                                            write_jsonl,
                                            xml_entry_to_bp_instance)
from ddbj_search_converter.schema import BioProject


class TestParseObjectType:
    """Tests for parse_object_type function."""

    def test_returns_bioproject_for_normal(self) -> None:
        """通常の BioProject を判定する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectType": {
                    "ProjectTypeSubmission": {}
                }
            }
        }
        assert parse_object_type(project) == "BioProject"

    def test_returns_umbrella_for_top_admin(self) -> None:
        """Umbrella BioProject を判定する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectType": {
                    "ProjectTypeTopAdmin": {"Organism": {}}
                }
            }
        }
        assert parse_object_type(project) == "UmbrellaBioProject"


class TestParseOrganism:
    """Tests for parse_organism function."""

    def test_parses_ncbi_organism(self) -> None:
        """NCBI 形式の Organism を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectType": {
                    "ProjectTypeSubmission": {
                        "Target": {
                            "Organism": {
                                "taxID": "9606",
                                "OrganismName": "Homo sapiens"
                            }
                        }
                    }
                }
            }
        }
        result = parse_organism(project, is_ddbj=False)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_parses_ddbj_organism(self) -> None:
        """DDBJ 形式の Organism を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectType": {
                    "ProjectTypeTopAdmin": {
                        "Organism": {
                            "taxID": "9606",
                            "OrganismName": "Homo sapiens"
                        }
                    }
                }
            }
        }
        result = parse_organism(project, is_ddbj=True)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_returns_none_when_missing(self) -> None:
        """Organism がない場合は None を返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectType": {}}}
        result = parse_organism(project, is_ddbj=False)
        assert result is None


class TestParseTitle:
    """Tests for parse_title function."""

    def test_parses_title(self) -> None:
        """タイトルを抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Title": "Test BioProject Title"
                }
            }
        }
        assert parse_title(project) == "Test BioProject Title"

    def test_returns_none_when_missing(self) -> None:
        """タイトルがない場合は None を返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectDescr": {}}}
        assert parse_title(project) is None


class TestParseDescription:
    """Tests for parse_description function."""

    def test_parses_description(self) -> None:
        """説明を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Description": "Test description"
                }
            }
        }
        assert parse_description(project) == "Test description"

    def test_returns_none_when_missing(self) -> None:
        """説明がない場合は None を返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectDescr": {}}}
        assert parse_description(project) is None


class TestParseOrganization:
    """Tests for parse_organization function."""

    def test_parses_ncbi_organization(self) -> None:
        """NCBI 形式の Organization を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Organization": {
                        "Name": "Test Organization",
                        "type": "center",
                        "role": "owner"
                    }
                }
            }
        }
        result = parse_organization(project, is_ddbj=False)
        assert len(result) == 1
        assert result[0].name == "Test Organization"
        assert result[0].organizationType == "center"
        assert result[0].role == "owner"

    def test_parses_ddbj_organization(self) -> None:
        """DDBJ 形式の Organization を抽出する。"""
        project: Dict[str, Any] = {
            "Submission": {
                "Submission": {
                    "Description": {
                        "Organization": {
                            "Name": "DDBJ Organization",
                            "type": "center"
                        }
                    }
                }
            }
        }
        result = parse_organization(project, is_ddbj=True)
        assert len(result) == 1
        assert result[0].name == "DDBJ Organization"

    def test_parses_organization_with_abbr(self) -> None:
        """abbreviation 付きの Organization を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Organization": {
                        "Name": {"content": "Full Name", "abbr": "FN"},
                        "type": "center"
                    }
                }
            }
        }
        result = parse_organization(project, is_ddbj=False)
        assert len(result) == 1
        assert result[0].name == "Full Name"
        assert result[0].abbreviation == "FN"

    def test_parses_multiple_organizations(self) -> None:
        """複数の Organization を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Organization": [
                        {"Name": "Org1"},
                        {"Name": "Org2"}
                    ]
                }
            }
        }
        result = parse_organization(project, is_ddbj=False)
        assert len(result) == 2


class TestParsePublication:
    """Tests for parse_publication function."""

    def test_parses_pubmed_publication(self) -> None:
        """PubMed Publication を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Publication": {
                        "id": "12345",
                        "DbType": "ePubmed",
                        "StructuredCitation": {"Title": "Paper Title"},
                        "status": "published"
                    }
                }
            }
        }
        result = parse_publication(project)
        assert len(result) == 1
        assert result[0].id_ == "12345"
        assert result[0].DbType == "ePubmed"
        assert result[0].url == "https://pubmed.ncbi.nlm.nih.gov/12345/"
        assert result[0].title == "Paper Title"

    def test_parses_doi_publication(self) -> None:
        """DOI Publication を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Publication": {
                        "id": "10.1234/example",
                        "DbType": "DOI"
                    }
                }
            }
        }
        result = parse_publication(project)
        assert len(result) == 1
        assert result[0].url == "https://doi.org/10.1234/example"

    def test_parses_multiple_publications(self) -> None:
        """複数の Publication を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Publication": [
                        {"id": "111", "DbType": "ePubmed"},
                        {"id": "10.1234/test", "DbType": "DOI"},
                    ]
                }
            }
        }
        result = parse_publication(project)
        assert len(result) == 2

    def test_handles_numeric_dbtype(self) -> None:
        """数字の DbType を ePubmed として扱う。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Publication": {
                        "id": "99999",
                        "DbType": "12345"  # 数字の DbType
                    }
                }
            }
        }
        result = parse_publication(project)
        assert len(result) == 1
        assert result[0].DbType == "ePubmed"
        assert result[0].url == "https://pubmed.ncbi.nlm.nih.gov/99999/"

    def test_returns_empty_for_missing(self) -> None:
        """Publication がない場合は空リストを返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectDescr": {}}}
        result = parse_publication(project)
        assert result == []


class TestParseGrant:
    """Tests for parse_grant function."""

    def test_parses_grant_with_string_agency(self) -> None:
        """文字列 Agency の Grant を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Grant": {
                        "GrantId": "GRANT001",
                        "Title": "Grant Title",
                        "Agency": "NIH"
                    }
                }
            }
        }
        result = parse_grant(project)
        assert len(result) == 1
        assert result[0].id_ == "GRANT001"
        assert result[0].title == "Grant Title"
        assert len(result[0].agency) == 1
        assert result[0].agency[0].name == "NIH"

    def test_parses_grant_with_dict_agency(self) -> None:
        """辞書 Agency の Grant を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Grant": {
                        "GrantId": "GRANT002",
                        "Agency": {"abbr": "NIH", "content": "National Institutes of Health"}
                    }
                }
            }
        }
        result = parse_grant(project)
        assert len(result) == 1
        assert result[0].agency[0].name == "National Institutes of Health"
        assert result[0].agency[0].abbreviation == "NIH"

    def test_parses_multiple_grants(self) -> None:
        """複数の Grant を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Grant": [
                        {"GrantId": "G1", "Agency": "NIH"},
                        {"GrantId": "G2", "Agency": "NSF"},
                    ]
                }
            }
        }
        result = parse_grant(project)
        assert len(result) == 2

    def test_returns_empty_for_missing(self) -> None:
        """Grant がない場合は空リストを返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectDescr": {}}}
        result = parse_grant(project)
        assert result == []


class TestParseExternalLink:
    """Tests for parse_external_link function."""

    def test_parses_url_link(self) -> None:
        """URL 形式の ExternalLink を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "ExternalLink": {
                        "URL": "https://example.com",
                        "label": "Example Link"
                    }
                }
            }
        }
        result = parse_external_link(project)
        assert len(result) == 1
        assert result[0].url == "https://example.com"
        assert result[0].label == "Example Link"

    def test_parses_geo_dbxref(self) -> None:
        """GEO dbXREF を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "ExternalLink": {
                        "dbXREF": {"db": "GEO", "ID": "GSE12345"}
                    }
                }
            }
        }
        result = parse_external_link(project)
        assert len(result) == 1
        assert "GSE12345" in result[0].url

    def test_parses_multiple_external_links(self) -> None:
        """複数の ExternalLink を抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "ExternalLink": [
                        {"URL": "https://example1.com", "label": "Link 1"},
                        {"dbXREF": {"db": "SRA", "ID": "SRA123"}},
                        {"URL": "https://example2.com"},
                    ]
                }
            }
        }
        result = parse_external_link(project)
        assert len(result) == 3

    def test_uses_url_as_label_when_missing(self) -> None:
        """label がない場合は URL を label として使う。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "ExternalLink": {
                        "URL": "https://example.com/path"
                    }
                }
            }
        }
        result = parse_external_link(project)
        assert len(result) == 1
        assert result[0].label == "https://example.com/path"

    def test_returns_empty_for_missing(self) -> None:
        """ExternalLink がない場合は空リストを返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectDescr": {}}}
        result = parse_external_link(project)
        assert result == []


class TestParseSameAs:
    """Tests for parse_same_as function."""

    def test_parses_geo_center_id(self) -> None:
        """GEO CenterID を sameAs として抽出する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectID": {
                    "CenterID": {"center": "GEO", "content": "GSE12345"}
                }
            }
        }
        result = parse_same_as(project)
        assert len(result) == 1
        assert result[0].identifier == "GSE12345"
        assert result[0].type_ == "geo"

    def test_ignores_non_geo_center_id(self) -> None:
        """GEO 以外の CenterID は無視する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectID": {
                    "CenterID": {"center": "OTHER", "content": "ID123"}
                }
            }
        }
        result = parse_same_as(project)
        assert len(result) == 0

    def test_parses_multiple_center_ids(self) -> None:
        """複数の CenterID を処理する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectID": {
                    "CenterID": [
                        {"center": "GEO", "content": "GSE111"},
                        {"center": "OTHER", "content": "ID999"},
                        {"center": "GEO", "content": "GSE222"},
                    ]
                }
            }
        }
        result = parse_same_as(project)
        assert len(result) == 2
        identifiers = [x.identifier for x in result]
        assert "GSE111" in identifiers
        assert "GSE222" in identifiers

    def test_returns_empty_for_missing_center_id(self) -> None:
        """CenterID がない場合は空リストを返す。"""
        project: Dict[str, Any] = {"Project": {"ProjectID": {}}}
        result = parse_same_as(project)
        assert result == []


class TestParseStatus:
    """Tests for parse_status function."""

    def test_returns_live_for_ddbj(self) -> None:
        """DDBJ は常に live を返す。"""
        project: Dict[str, Any] = {}
        assert parse_status(project, is_ddbj=True) == "live"

    def test_returns_live_for_ncbi(self) -> None:
        """NCBI も常に live を返す (BioProject には status 情報がない)。"""
        project: Dict[str, Any] = {
            "Submission": {
                "Description": {
                    "Access": "controlled"
                }
            }
        }
        assert parse_status(project, is_ddbj=False) == "live"

    def test_returns_live_always(self) -> None:
        """常に live を返す。"""
        project: Dict[str, Any] = {}
        assert parse_status(project, is_ddbj=False) == "live"


class TestNormalizeProperties:
    """Tests for normalize_properties function."""

    def test_normalizes_local_id_string(self) -> None:
        """LocalID の文字列を正規化する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectID": {
                    "LocalID": "local123"
                }
            }
        }
        normalize_properties(project)
        assert project["Project"]["ProjectID"]["LocalID"] == {"content": "local123"}

    def test_normalizes_local_id_list(self) -> None:
        """LocalID のリストを正規化する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectID": {
                    "LocalID": ["local1", "local2", {"content": "local3"}]
                }
            }
        }
        normalize_properties(project)
        assert project["Project"]["ProjectID"]["LocalID"][0] == {"content": "local1"}
        assert project["Project"]["ProjectID"]["LocalID"][1] == {"content": "local2"}
        assert project["Project"]["ProjectID"]["LocalID"][2] == {"content": "local3"}

    def test_normalizes_locus_tag_prefix(self) -> None:
        """LocusTagPrefix を正規化する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "LocusTagPrefix": "ABC"
                }
            }
        }
        normalize_properties(project)
        assert project["Project"]["ProjectDescr"]["LocusTagPrefix"] == {"content": "ABC"}

    def test_normalizes_organization_name_ddbj(self) -> None:
        """DDBJ 形式の Organization.Name を正規化する。"""
        project: Dict[str, Any] = {
            "Submission": {
                "Submission": {
                    "Description": {
                        "Organization": {
                            "Name": "Test Organization"
                        }
                    }
                }
            }
        }
        normalize_properties(project)
        expected = {"content": "Test Organization"}
        assert project["Submission"]["Submission"]["Description"]["Organization"]["Name"] == expected

    def test_normalizes_organization_name_ncbi(self) -> None:
        """NCBI 形式の Organization.Name を正規化する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Organization": {
                        "Name": "NCBI Organization"
                    }
                }
            }
        }
        normalize_properties(project)
        expected = {"content": "NCBI Organization"}
        assert project["Project"]["ProjectDescr"]["Organization"]["Name"] == expected

    def test_normalizes_organization_name_list(self) -> None:
        """複数の Organization.Name を正規化する。"""
        project: Dict[str, Any] = {
            "Project": {
                "ProjectDescr": {
                    "Organization": [
                        {"Name": "Org1"},
                        {"Name": {"content": "Org2", "abbr": "O2"}},
                    ]
                }
            }
        }
        normalize_properties(project)
        assert project["Project"]["ProjectDescr"]["Organization"][0]["Name"] == {"content": "Org1"}
        # 既に dict の場合は変更しない
        assert project["Project"]["ProjectDescr"]["Organization"][1]["Name"] == {"content": "Org2", "abbr": "O2"}


class TestIterateXmlPackages:
    """Tests for iterate_xml_packages function."""

    def test_extracts_packages(self, tmp_path: Path) -> None:
        """<Package> 要素を抽出する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
<Package>
  <Project>
    <Project>
      <ProjectID><ArchiveID accession="PRJNA1"/></ProjectID>
    </Project>
  </Project>
</Package>
<Package>
  <Project>
    <Project>
      <ProjectID><ArchiveID accession="PRJNA2"/></ProjectID>
    </Project>
  </Project>
</Package>
</PackageSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_bytes(xml_content.encode("utf-8"))

        packages = list(iterate_xml_packages(xml_path))
        assert len(packages) == 2
        assert b"PRJNA1" in packages[0]
        assert b"PRJNA2" in packages[1]


class TestXmlEntryToBpInstance:
    """Tests for xml_entry_to_bp_instance function."""

    def test_converts_entry(self) -> None:
        """XML エントリを BioProject インスタンスに変換する。"""
        entry: Dict[str, Any] = {
            "Project": {
                "Project": {
                    "ProjectID": {
                        "ArchiveID": {"accession": "PRJNA12345"}
                    },
                    "ProjectDescr": {
                        "Title": "Test Project",
                        "Description": "Test Description"
                    },
                    "ProjectType": {
                        "ProjectTypeSubmission": {
                            "Target": {
                                "Organism": {
                                    "taxID": "9606",
                                    "OrganismName": "Homo sapiens"
                                }
                            }
                        }
                    }
                }
            }
        }

        result = xml_entry_to_bp_instance(entry, is_ddbj=False)

        assert result.identifier == "PRJNA12345"
        assert result.title == "Test Project"
        assert result.description == "Test Description"
        assert result.objectType == "BioProject"
        assert result.accessibility == "public-access"


class TestWriteJsonl:
    """Tests for write_jsonl function."""

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        """JSONL ファイルを書き込む。"""
        bp = BioProject(
            identifier="PRJNA12345",
            properties={"Project": {}},
            distribution=[],
            isPartOf="BioProject",
            type="bioproject",
            objectType="BioProject",
            name=None,
            url="https://example.com",
            organism=None,
            title="Test",
            description=None,
            organization=[],
            publication=[],
            grant=[],
            externalLink=[],
            dbXref=[],
            sameAs=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [bp])

        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "PRJNA12345" in content
        assert '"type":"bioproject"' in content

    def test_writes_multiple_entries(self, tmp_path: Path) -> None:
        """複数エントリを書き込む。"""
        bp1 = BioProject(
            identifier="PRJNA1",
            properties={},
            distribution=[],
            isPartOf="BioProject",
            type="bioproject",
            objectType="BioProject",
            name=None,
            url="https://example.com/1",
            organism=None,
            title=None,
            description=None,
            organization=[],
            publication=[],
            grant=[],
            externalLink=[],
            dbXref=[],
            sameAs=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )
        bp2 = BioProject(
            identifier="PRJNA2",
            properties={},
            distribution=[],
            isPartOf="BioProject",
            type="bioproject",
            objectType="BioProject",
            name=None,
            url="https://example.com/2",
            organism=None,
            title=None,
            description=None,
            organization=[],
            publication=[],
            grant=[],
            externalLink=[],
            dbXref=[],
            sameAs=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [bp1, bp2])

        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert "PRJNA1" in lines[0]
        assert "PRJNA2" in lines[1]


class TestParseArgs:
    """Tests for parse_args function."""

    def test_default_args(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """デフォルト引数でパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, tmp_xml_dir, output_dir, parallel_num, full = parse_args([])
        assert config.result_dir == tmp_path
        assert "bioproject/tmp_xml" in str(tmp_xml_dir)
        assert "bioproject/jsonl" in str(output_dir)
        assert parallel_num == 64
        assert full is False

    def test_full_flag(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--full フラグをパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, tmp_xml_dir, output_dir, parallel_num, full = parse_args(["--full"])
        assert full is True

    def test_parallel_num(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--parallel-num オプションをパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, tmp_xml_dir, output_dir, parallel_num, full = parse_args(["--parallel-num", "32"])
        assert parallel_num == 32

    def test_result_dir(self, tmp_path: Path) -> None:
        """--result-dir オプションをパースする。"""
        result_dir = tmp_path / "custom_result"
        config, tmp_xml_dir, output_dir, parallel_num, full = parse_args(["--result-dir", str(result_dir)])
        assert config.result_dir == result_dir

    def test_date_option(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--date オプションをパースする。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, tmp_xml_dir, output_dir, parallel_num, full = parse_args(["--date", "20260115"])
        assert "20260115" in str(tmp_xml_dir)
        assert "20260115" in str(output_dir)
