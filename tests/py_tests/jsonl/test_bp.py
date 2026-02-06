"""Tests for ddbj_search_converter.jsonl.bp module."""
from typing import Any, Dict, List

import pytest

from ddbj_search_converter.jsonl.bp import (
    normalize_properties,
    parse_accessibility,
    parse_description,
    parse_external_link,
    parse_grant,
    parse_object_type,
    parse_organism,
    parse_organization,
    parse_publication,
    parse_same_as,
    parse_status,
    parse_title,
)


def _make_project(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Minimal valid project dict for testing."""
    base: Dict[str, Any] = {
        "Project": {
            "ProjectID": {"ArchiveID": {"accession": "PRJDB1"}},
            "ProjectType": {},
            "ProjectDescr": {},
        },
        "Submission": {
            "Description": {},
        },
    }
    if overrides:
        base.update(overrides)
    return base


class TestParseObjectType:
    """Tests for parse_object_type function."""

    def test_regular_bioproject(self) -> None:
        project = _make_project()
        assert parse_object_type(project) == "BioProject"

    def test_umbrella_bioproject(self) -> None:
        project = _make_project()
        project["Project"]["ProjectType"]["ProjectTypeTopAdmin"] = {"Organism": {}}
        assert parse_object_type(project) == "UmbrellaBioProject"


class TestParseOrganism:
    """Tests for parse_organism function."""

    def test_ncbi_organism(self) -> None:
        project = _make_project()
        project["Project"]["ProjectType"]["ProjectTypeSubmission"] = {
            "Target": {
                "Organism": {"taxID": "9606", "OrganismName": "Homo sapiens"}
            }
        }
        result = parse_organism(project, is_ddbj=False)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_ddbj_organism(self) -> None:
        project = _make_project()
        project["Project"]["ProjectType"]["ProjectTypeTopAdmin"] = {
            "Organism": {"taxID": "9606", "OrganismName": "Homo sapiens"}
        }
        result = parse_organism(project, is_ddbj=True)
        assert result is not None
        assert result.identifier == "9606"

    def test_no_organism(self) -> None:
        project = _make_project()
        result = parse_organism(project, is_ddbj=False)
        assert result is None

    def test_fallback_to_single_organism(self) -> None:
        project = _make_project()
        project["Project"]["ProjectType"]["ProjectTypeTopSingleOrganism"] = {
            "Organism": {"taxID": "10090", "OrganismName": "Mus musculus"}
        }
        result = parse_organism(project, is_ddbj=False)
        assert result is not None
        assert result.identifier == "10090"


class TestBug8TaxIdListDict:
    """Bug #8 (fixed): parse_organism の taxID が list/dict でも適切に処理。"""

    def test_taxid_list_should_handle_gracefully(self) -> None:
        project = _make_project()
        project["Project"]["ProjectType"]["ProjectTypeSubmission"] = {
            "Target": {
                "Organism": {"taxID": [9606, 10090], "OrganismName": "Mixed"}
            }
        }
        result = parse_organism(project, is_ddbj=False)
        assert result is not None
        assert result.identifier.isdigit()


class TestParseTitle:
    """Tests for parse_title function."""

    def test_normal_title(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Title"] = "My Project"
        assert parse_title(project) == "My Project"

    def test_no_title(self) -> None:
        project = _make_project()
        assert parse_title(project) is None

    def test_none_title(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Title"] = None
        assert parse_title(project) is None


class TestBug9TitleDict:
    """Bug #9 (fixed): parse_title の title が dict でも content を抽出。"""

    def test_dict_title_should_handle_gracefully(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Title"] = {"content": "Real Title", "lang": "en"}
        result = parse_title(project)
        assert result == "Real Title"


class TestParseDescription:
    """Tests for parse_description function."""

    def test_normal_description(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Description"] = "A description"
        assert parse_description(project) == "A description"

    def test_no_description(self) -> None:
        project = _make_project()
        assert parse_description(project) is None


class TestParseOrganization:
    """Tests for parse_organization function."""

    def test_single_organization(self) -> None:
        project = _make_project()
        project["Submission"]["Description"]["Organization"] = {
            "Name": "DDBJ Center", "type": "center", "role": "owner",
        }
        orgs = parse_organization(project)
        assert len(orgs) == 1
        assert orgs[0].name == "DDBJ Center"

    def test_multiple_organizations(self) -> None:
        project = _make_project()
        project["Submission"]["Description"]["Organization"] = [
            {"Name": "Org1", "type": "center", "role": "owner"},
            {"Name": "Org2", "type": "institute", "role": "participant"},
        ]
        orgs = parse_organization(project)
        assert len(orgs) == 2

    def test_no_organization(self) -> None:
        project = _make_project()
        assert parse_organization(project) == []

    def test_organization_with_dict_name(self) -> None:
        project = _make_project()
        project["Submission"]["Description"]["Organization"] = {
            "Name": {"content": "DDBJ Center", "abbr": "DDBJ"},
            "type": "center", "role": "owner",
        }
        orgs = parse_organization(project)
        assert len(orgs) == 1
        assert orgs[0].name == "DDBJ Center"
        assert orgs[0].abbreviation == "DDBJ"

    def test_ddbj_always_empty(self) -> None:
        """DDBJ BioProject には Submission がないため常に空。"""
        project: Dict[str, Any] = {"Project": {"ProjectType": {}, "ProjectDescr": {}}}
        orgs = parse_organization(project)
        assert orgs == []


class TestParsePublication:
    """Tests for parse_publication function."""

    def test_single_publication(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "12345", "DbType": "ePubmed",
            "StructuredCitation": {"Title": "A paper"},
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].id_ == "12345"
        assert pubs[0].url is not None and "pubmed" in pubs[0].url

    def test_publication_doi(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "10.1234/test", "DbType": "DOI",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].url is not None and "doi.org" in pubs[0].url

    def test_publication_numeric_dbtype(self) -> None:
        """DbType が数字の場合、ePubmed として扱う。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "12345", "DbType": "12345",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].DbType == "ePubmed"


class TestParseGrant:
    """Tests for parse_grant function."""

    def test_single_grant(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Grant"] = {
            "GrantId": "G001", "Title": "Grant Title",
            "Agency": "NIH",
        }
        grants = parse_grant(project)
        assert len(grants) == 1
        assert grants[0].id_ == "G001"

    def test_grant_with_dict_agency(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Grant"] = {
            "GrantId": "G001", "Title": "Grant Title",
            "Agency": {"abbr": "NIH", "content": "National Institutes of Health"},
        }
        grants = parse_grant(project)
        assert len(grants) == 1
        assert grants[0].agency[0].name == "National Institutes of Health"
        assert grants[0].agency[0].abbreviation == "NIH"


class TestParseExternalLink:
    """Tests for parse_external_link function."""

    def test_url_link(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["ExternalLink"] = {
            "URL": "https://example.com", "label": "Example",
        }
        links = parse_external_link(project)
        assert len(links) == 1
        assert links[0].url == "https://example.com"
        assert links[0].label == "Example"

    def test_dbxref_link(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["ExternalLink"] = {
            "dbXREF": {"db": "GEO", "ID": "GSE12345"},
        }
        links = parse_external_link(project)
        assert len(links) == 1
        assert "GSE12345" in links[0].url


class TestParseSameAs:
    """Tests for parse_same_as function."""

    def test_geo_center_id(self) -> None:
        project = _make_project()
        project["Project"]["ProjectID"] = {
            "ArchiveID": {"accession": "PRJDB1"},
            "CenterID": {"content": "GSE12345", "center": "GEO"},
        }
        xrefs = parse_same_as(project)
        assert len(xrefs) == 1
        assert xrefs[0].identifier == "GSE12345"
        assert xrefs[0].type_ == "geo"

    def test_non_geo_center_id(self) -> None:
        project = _make_project()
        project["Project"]["ProjectID"] = {
            "ArchiveID": {"accession": "PRJDB1"},
            "CenterID": {"content": "X001", "center": "OTHER"},
        }
        xrefs = parse_same_as(project)
        assert xrefs == []


class TestParseStatus:
    """Tests for parse_status function."""

    def test_always_returns_live(self) -> None:
        project = _make_project()
        assert parse_status(project, is_ddbj=True) == "live"
        assert parse_status(project, is_ddbj=False) == "live"


class TestParseAccessibility:
    """Tests for parse_accessibility function."""

    def test_ddbj_always_public(self) -> None:
        project = _make_project()
        assert parse_accessibility(project, is_ddbj=True) == "public-access"

    def test_ncbi_public(self) -> None:
        project = _make_project()
        project["Submission"]["Description"]["Access"] = "public"
        assert parse_accessibility(project, is_ddbj=False) == "public-access"

    def test_ncbi_controlled(self) -> None:
        project = _make_project()
        project["Submission"]["Description"]["Access"] = "controlled-access"
        assert parse_accessibility(project, is_ddbj=False) == "controlled-access"


class TestNormalizeProperties:
    """Tests for normalize_properties function."""

    def test_normalizes_biosample_set_id_string(self) -> None:
        project = _make_project()
        project["Project"]["ProjectType"]["ProjectTypeSubmission"] = {
            "Target": {"BioSampleSet": {"ID": "12345"}}
        }
        normalize_properties(project)
        assert project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] == {"content": "12345"}

    def test_normalizes_locus_tag_prefix_string(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["LocusTagPrefix"] = "ABC"
        normalize_properties(project)
        assert project["Project"]["ProjectDescr"]["LocusTagPrefix"] == {"content": "ABC"}

    def test_normalizes_local_id_string(self) -> None:
        project = _make_project()
        project["Project"]["ProjectID"] = {
            "ArchiveID": {"accession": "PRJDB1"},
            "LocalID": "local1",
        }
        normalize_properties(project)
        assert project["Project"]["ProjectID"]["LocalID"] == {"content": "local1"}

    def test_normalizes_organization_name(self) -> None:
        project = _make_project()
        project["Submission"]["Description"]["Organization"] = {"Name": "OrgName"}
        normalize_properties(project)
        assert project["Submission"]["Description"]["Organization"]["Name"] == {"content": "OrgName"}

    def test_normalizes_grant_agency(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Grant"] = {"Agency": "NIH"}
        normalize_properties(project)
        assert project["Project"]["ProjectDescr"]["Grant"]["Agency"] == {"abbr": "NIH", "content": "NIH"}

    def test_no_crash_on_empty_project(self) -> None:
        project: Dict[str, Any] = {"Project": {"ProjectType": {}, "ProjectDescr": {}}}
        normalize_properties(project)
