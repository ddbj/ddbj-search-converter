"""Tests for ddbj_search_converter.jsonl.bp module."""

from __future__ import annotations

from typing import Any

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl.bp import (
    normalize_properties,
    parse_accessibility,
    parse_description,
    parse_external_link,
    parse_grant,
    parse_name,
    parse_object_type,
    parse_organism,
    parse_organization,
    parse_publication,
    parse_same_as,
    parse_status,
    parse_title,
    xml_entry_to_bp_instance,
)
from ddbj_search_converter.schema import BioProject


def _make_project(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Minimal valid project dict for testing."""
    base: dict[str, Any] = {
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
            "Target": {"Organism": {"taxID": "9606", "OrganismName": "Homo sapiens"}}
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
            "Target": {"Organism": {"taxID": [9606, 10090], "OrganismName": "Mixed"}}
        }
        result = parse_organism(project, is_ddbj=False)
        assert result is not None
        assert result.identifier.isdigit()  # type: ignore[union-attr]


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


class TestParseName:
    def test_name_str(self) -> None:
        """ProjectDescr.Name が文字列の場合、そのまま返す。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Name"] = "Borreliella burgdorferi B31"
        assert parse_name(project) == "Borreliella burgdorferi B31"

    def test_no_name(self) -> None:
        """ProjectDescr.Name キーが無い場合 None（DDBJ XML の通常パターン）。"""
        project = _make_project()
        assert parse_name(project) is None

    def test_none_name(self) -> None:
        """ProjectDescr.Name が None 明示の場合 None。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Name"] = None
        assert parse_name(project) is None

    def test_dict_name(self) -> None:
        """Name が dict ({'content': ...}) の場合、content を返す。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Name"] = {"content": "Real Name", "abbr": "RN"}
        assert parse_name(project) == "Real Name"

    def test_dict_name_empty_content(self) -> None:
        """Name dict の content が None の場合 None を返す。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Name"] = {"content": None}
        assert parse_name(project) is None


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
            "Name": "DDBJ Center",
            "type": "center",
            "role": "owner",
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
            "type": "center",
            "role": "owner",
        }
        orgs = parse_organization(project)
        assert len(orgs) == 1
        assert orgs[0].name == "DDBJ Center"
        assert orgs[0].abbreviation == "DDBJ"

    def test_ddbj_always_empty(self) -> None:
        """DDBJ BioProject には Submission がないため常に空。"""
        project: dict[str, Any] = {"Project": {"ProjectType": {}, "ProjectDescr": {}}}
        orgs = parse_organization(project)
        assert orgs == []


class TestParsePublication:
    """Tests for parse_publication function."""

    def test_single_publication(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "12345",
            "DbType": "ePubmed",
            "StructuredCitation": {"Title": "A paper"},
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].id_ == "12345"
        assert pubs[0].url is not None
        assert "pubmed" in pubs[0].url

    def test_publication_edoi(self) -> None:
        """DbType が eDOI の場合、doi.org の URL を生成する。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "10.1271/bbb.60419",
            "DbType": "eDOI",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].url == "https://doi.org/10.1271/bbb.60419"

    def test_publication_epmc_with_prefix(self) -> None:
        """DbType が ePMC で PMC プレフィックス付き ID の場合。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "PMC3564981",
            "DbType": "ePMC",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].url == "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3564981/"

    def test_publication_epmc_numeric(self) -> None:
        """DbType が ePMC で数字のみの ID の場合、PMC プレフィックスを付与する。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "3594186",
            "DbType": "ePMC",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].url == "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3594186/"

    def test_publication_epmc_doi_format(self) -> None:
        """DbType が ePMC だが ID が DOI 形式の場合、DOI としてリンクする。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "10.1007/s10531-014-0684-8",
            "DbType": "ePMC",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].url == "https://doi.org/10.1007/s10531-014-0684-8"

    def test_publication_not_available(self) -> None:
        """DbType が eNotAvailable の場合、url は None。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "unpublished",
            "DbType": "eNotAvailable",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].url is None

    def test_publication_numeric_dbtype(self) -> None:
        """DbType が数字の場合、ePubmed として扱う。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {
            "id": "12345",
            "DbType": "12345",
        }
        pubs = parse_publication(project)
        assert len(pubs) == 1
        assert pubs[0].dbType == "ePubmed"


class TestParseGrant:
    """Tests for parse_grant function."""

    def test_single_grant(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Grant"] = {
            "GrantId": "G001",
            "Title": "Grant Title",
            "Agency": "NIH",
        }
        grants = parse_grant(project)
        assert len(grants) == 1
        assert grants[0].id_ == "G001"

    def test_grant_with_dict_agency(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Grant"] = {
            "GrantId": "G001",
            "Title": "Grant Title",
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
            "URL": "https://example.com",
            "label": "Example",
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

    def test_always_returns_public(self) -> None:
        project = _make_project()
        assert parse_status(project, is_ddbj=True) == "public"
        assert parse_status(project, is_ddbj=False) == "public"


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
        project["Project"]["ProjectType"]["ProjectTypeSubmission"] = {"Target": {"BioSampleSet": {"ID": "12345"}}}
        normalize_properties(project)
        assert project["Project"]["ProjectType"]["ProjectTypeSubmission"]["Target"]["BioSampleSet"]["ID"] == {
            "content": "12345"
        }

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
        project: dict[str, Any] = {"Project": {"ProjectType": {}, "ProjectDescr": {}}}
        normalize_properties(project)


def _make_bp_instance(identifier: str) -> BioProject:
    from ddbj_search_converter.schema import BioProject

    return BioProject(
        identifier=identifier,
        properties={},
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
        dbXrefs=[],
        parentBioProjects=[],
        childBioProjects=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


class TestEnrichUmbrellaRelations:
    """Tests for enrich_umbrella_relations function."""

    def _setup_umbrella_db(self, config: Config, relations: list[tuple[str, str]]) -> None:
        from ddbj_search_converter.dblink.db import (
            finalize_umbrella_db,
            init_umbrella_db,
            save_umbrella_relations,
        )
        from ddbj_search_converter.logging.logger import run_logger

        with run_logger(config=config):
            init_umbrella_db(config)
            save_umbrella_relations(config, set(relations))
            finalize_umbrella_db(config)

    def test_parent_child_xrefs_set(self, test_config: Config) -> None:
        """parent/child の Xref が parentBioProjects/childBioProjects に設定される。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        self._setup_umbrella_db(test_config, [("PRJDB999", "PRJDB100")])

        docs = {
            "PRJDB999": _make_bp_instance("PRJDB999"),
            "PRJDB100": _make_bp_instance("PRJDB100"),
        }
        enrich_umbrella_relations(test_config, docs)

        # PRJDB999 は parent → childBioProjects に PRJDB100
        assert len(docs["PRJDB999"].childBioProjects) == 1
        assert docs["PRJDB999"].childBioProjects[0].identifier == "PRJDB100"
        assert docs["PRJDB999"].parentBioProjects == []

        # PRJDB100 は child → parentBioProjects に PRJDB999
        assert len(docs["PRJDB100"].parentBioProjects) == 1
        assert docs["PRJDB100"].parentBioProjects[0].identifier == "PRJDB999"
        assert docs["PRJDB100"].childBioProjects == []

    def test_dbxrefs_not_modified_by_umbrella_relations(self, test_config: Config) -> None:
        """enrich_umbrella_relations は dbXrefs を変更しない。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        self._setup_umbrella_db(test_config, [("PRJDB999", "PRJDB100")])

        docs = {
            "PRJDB999": _make_bp_instance("PRJDB999"),
            "PRJDB100": _make_bp_instance("PRJDB100"),
        }
        enrich_umbrella_relations(test_config, docs)

        # dbXrefs は空のまま
        assert docs["PRJDB999"].dbXrefs == []
        assert docs["PRJDB100"].dbXrefs == []

        # parentBioProjects / childBioProjects は設定される
        assert len(docs["PRJDB999"].childBioProjects) == 1
        assert len(docs["PRJDB100"].parentBioProjects) == 1

    def test_no_umbrella_db_leaves_empty(self, test_config: Config) -> None:
        """Umbrella DB がない場合、parent/child は空のまま。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        docs = {"PRJDB100": _make_bp_instance("PRJDB100")}
        enrich_umbrella_relations(test_config, docs)

        assert docs["PRJDB100"].parentBioProjects == []
        assert docs["PRJDB100"].childBioProjects == []
        assert docs["PRJDB100"].dbXrefs == []

    def test_multiple_parents(self, test_config: Config) -> None:
        """DAG: 1 つの child が複数の parent を持つ場合。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        self._setup_umbrella_db(
            test_config,
            [
                ("PRJDB800", "PRJDB100"),
                ("PRJDB900", "PRJDB100"),
            ],
        )

        docs = {"PRJDB100": _make_bp_instance("PRJDB100")}
        enrich_umbrella_relations(test_config, docs)

        parent_ids = sorted([x.identifier for x in docs["PRJDB100"].parentBioProjects])
        assert parent_ids == ["PRJDB800", "PRJDB900"]

    def test_intermediate_node(self, test_config: Config) -> None:
        """DAG: 中間ノード (parent かつ child) で両方のフィールドが設定される。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        self._setup_umbrella_db(
            test_config,
            [
                ("PRJDB900", "PRJDB500"),
                ("PRJDB500", "PRJDB100"),
            ],
        )

        docs = {"PRJDB500": _make_bp_instance("PRJDB500")}
        enrich_umbrella_relations(test_config, docs)

        assert len(docs["PRJDB500"].parentBioProjects) == 1
        assert docs["PRJDB500"].parentBioProjects[0].identifier == "PRJDB900"
        assert len(docs["PRJDB500"].childBioProjects) == 1
        assert docs["PRJDB500"].childBioProjects[0].identifier == "PRJDB100"

    def test_xref_type_is_bioproject(self, test_config: Config) -> None:
        """parent/child の Xref の type が全て "bioproject" であること。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        self._setup_umbrella_db(test_config, [("PRJDB999", "PRJDB100")])

        docs = {
            "PRJDB999": _make_bp_instance("PRJDB999"),
            "PRJDB100": _make_bp_instance("PRJDB100"),
        }
        enrich_umbrella_relations(test_config, docs)

        for xref in docs["PRJDB999"].childBioProjects:
            assert xref.type_ == "bioproject"
        for xref in docs["PRJDB100"].parentBioProjects:
            assert xref.type_ == "bioproject"

    def test_empty_docs(self, test_config: Config) -> None:
        """空の docs でクラッシュしない。"""
        from ddbj_search_converter.jsonl.utils import enrich_umbrella_relations

        docs: dict[str, Any] = {}
        enrich_umbrella_relations(test_config, docs)
        assert docs == {}


class TestFetchStatuses:
    """Tests for _fetch_statuses function."""

    def test_fetch_statuses_overwrites_status(self, tmp_path):
        from ddbj_search_converter.config import Config
        from ddbj_search_converter.jsonl.bp import _fetch_statuses
        from ddbj_search_converter.logging.logger import run_logger
        from ddbj_search_converter.status_cache.db import (
            finalize_status_cache_db,
            init_status_cache_db,
            insert_bp_statuses,
        )

        config = Config(result_dir=tmp_path)
        with run_logger(config=config):
            init_status_cache_db(config)
            insert_bp_statuses(config, [("PRJDB1", "suppressed"), ("PRJDB2", "withdrawn")])
            finalize_status_cache_db(config)

            docs = {
                "PRJDB1": _make_bp_instance("PRJDB1"),
                "PRJDB2": _make_bp_instance("PRJDB2"),
            }
            _fetch_statuses(config, docs)

        assert docs["PRJDB1"].status == "suppressed"
        assert docs["PRJDB2"].status == "withdrawn"

    def test_fetch_statuses_skips_when_no_cache(self, tmp_path):
        from ddbj_search_converter.config import Config
        from ddbj_search_converter.jsonl.bp import _fetch_statuses

        config = Config(result_dir=tmp_path)
        docs = {"PRJDB1": _make_bp_instance("PRJDB1")}
        _fetch_statuses(config, docs)

        assert docs["PRJDB1"].status == "public"


class TestXmlEntryToBpInstanceProperties:
    """xml_entry_to_bp_instance 実行後の properties 構造の回帰テスト。

    BioProject は Attribute 相当フィールドを持たないため、
    properties 内の各要素が配列化されないことを検証する。
    """

    def test_project_descr_stays_dict(self) -> None:
        project = _make_project()
        project["Project"]["ProjectDescr"]["Title"] = "My Project"
        bp = xml_entry_to_bp_instance({"Project": project}, is_ddbj=True)
        descr = bp.properties["Project"]["Project"]["ProjectDescr"]
        assert isinstance(descr, dict)
        assert descr["Title"] == "My Project"

    def test_publication_single_stays_dict(self) -> None:
        """Publication が 1 件の場合 dict のまま（配列化されない）。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = {"id": "12345", "DbType": "ePubmed"}
        bp = xml_entry_to_bp_instance({"Project": project}, is_ddbj=True)
        pub = bp.properties["Project"]["Project"]["ProjectDescr"]["Publication"]
        assert isinstance(pub, dict)
        assert pub["id"] == "12345"

    def test_publication_list_stays_list(self) -> None:
        """Publication が 2 件以上の場合 list のまま。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Publication"] = [
            {"id": "1", "DbType": "ePubmed"},
            {"id": "2", "DbType": "ePubmed"},
        ]
        bp = xml_entry_to_bp_instance({"Project": project}, is_ddbj=True)
        pub = bp.properties["Project"]["Project"]["ProjectDescr"]["Publication"]
        assert isinstance(pub, list)
        assert len(pub) == 2

    def test_archive_id_stays_dict(self) -> None:
        project = _make_project()
        bp = xml_entry_to_bp_instance({"Project": project}, is_ddbj=True)
        archive_id = bp.properties["Project"]["Project"]["ProjectID"]["ArchiveID"]
        assert isinstance(archive_id, dict)
        assert archive_id["accession"] == "PRJDB1"

    def test_name_is_set_from_project_descr_name(self) -> None:
        """xml_entry_to_bp_instance が ProjectDescr.Name を bp.name に詰める。"""
        project = _make_project()
        project["Project"]["ProjectDescr"]["Name"] = "Test Organism"
        bp = xml_entry_to_bp_instance({"Project": project}, is_ddbj=True)
        assert bp.name == "Test Organism"

    def test_name_is_none_when_project_descr_name_missing(self) -> None:
        """ProjectDescr.Name が無い場合 bp.name は None。"""
        project = _make_project()
        bp = xml_entry_to_bp_instance({"Project": project}, is_ddbj=True)
        assert bp.name is None
