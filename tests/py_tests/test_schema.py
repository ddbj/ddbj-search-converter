"""Tests for ddbj_search_converter.schema module."""

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from ddbj_search_converter.schema import (
    GEA,
    JGA,
    SRA,
    BioProject,
    BioSample,
    BioSamplePackage,
    Distribution,
    MetaboBank,
    Organism,
    Organization,
    Publication,
    Xref,
)


class TestEncodingFormat:
    """Tests for EncodingFormat Literal type."""

    def test_valid_values(self) -> None:
        """全ての有効な値を受け入れる。"""
        for fmt in ("JSON", "JSON-LD", "XML", "FASTQ", "SRA"):
            dist = Distribution(type="DataDownload", encodingFormat=fmt, contentUrl="https://example.com")

            assert dist.encodingFormat == fmt

    def test_invalid_value_rejected(self) -> None:
        """無効な値を拒否する。"""
        with pytest.raises(ValidationError):
            Distribution(type="DataDownload", encodingFormat="INVALID", contentUrl="https://example.com")  # type: ignore[arg-type]


class TestDistribution:
    """Tests for Distribution model."""

    def test_create_distribution(self) -> None:
        """Distribution インスタンスを作成する。"""
        dist = Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl="https://example.com/data.json",
        )

        assert dist.type_ == "DataDownload"
        assert dist.encodingFormat == "JSON"
        assert dist.contentUrl == "https://example.com/data.json"

    def test_distribution_json_alias(self) -> None:
        """JSON 出力時に type_ が type になる。"""
        dist = Distribution(
            type="DataDownload",
            encodingFormat="JSON",
            contentUrl="https://example.com/data.json",
        )

        json_dict = dist.model_dump(by_alias=True)
        assert "type" in json_dict
        assert "type_" not in json_dict


class TestOrganism:
    """Tests for Organism model."""

    def test_create_organism(self) -> None:
        """Organism インスタンスを作成する。"""
        org = Organism(identifier="9606", name="Homo sapiens")

        assert org.identifier == "9606"
        assert org.name == "Homo sapiens"

    def test_organism_optional_fields(self) -> None:
        """Optional フィールドが None でも OK。"""
        org = Organism(identifier=None, name=None)

        assert org.identifier is None
        assert org.name is None


class TestXref:
    """Tests for Xref model."""

    def test_create_xref(self) -> None:
        """Xref インスタンスを作成する。"""
        xref = Xref(
            identifier="JGAS000001",
            type="jga-study",
            url="https://ddbj.nig.ac.jp/search/entry/jga-study/JGAS000001",
        )

        assert xref.identifier == "JGAS000001"
        assert xref.type_ == "jga-study"
        assert xref.url == "https://ddbj.nig.ac.jp/search/entry/jga-study/JGAS000001"

    def test_xref_json_alias(self) -> None:
        """JSON 出力時に type_ が type になる。"""
        xref = Xref(
            identifier="JGAS000001",
            type="jga-study",
            url="https://example.com",
        )

        json_dict = xref.model_dump(by_alias=True)
        assert "type" in json_dict
        assert "type_" not in json_dict


class TestBioProject:
    """Tests for BioProject model."""

    def test_parent_child_fields(self) -> None:
        """parentBioProjects / childBioProjects フィールドが正しく設定される。"""
        parent_xref = Xref(identifier="PRJDB999", type="bioproject", url="https://example.com")
        child_xref = Xref(identifier="PRJDB100", type="bioproject", url="https://example.com")

        bp = BioProject(
            identifier="PRJDB500",
            properties={},
            distribution=[],
            isPartOf="bioproject",
            type="bioproject",
            objectType="UmbrellaBioProject",
            name=None,
            url="https://example.com",
            organism=None,
            title=None,
            description=None,
            organization=[],
            publication=[],
            grant=[],
            externalLink=[],
            dbXrefs=[],
            parentBioProjects=[parent_xref],
            childBioProjects=[child_xref],
            sameAs=[],
            status="public",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        assert len(bp.parentBioProjects) == 1
        assert bp.parentBioProjects[0].identifier == "PRJDB999"
        assert len(bp.childBioProjects) == 1
        assert bp.childBioProjects[0].identifier == "PRJDB100"


class TestXrefTypeValidation:
    """Tests for XrefType validation."""

    def test_umbrella_bioproject_is_invalid(self) -> None:
        """umbrella-bioproject は無効な XrefType として拒否される。"""
        with pytest.raises(ValidationError):
            Xref(identifier="PRJDB1", type="umbrella-bioproject", url="https://example.com")  # type: ignore[arg-type]


class TestJGA:
    """Tests for JGA model."""

    def test_create_jga(self) -> None:
        """JGA インスタンスを作成する。"""
        jga = JGA(
            identifier="JGAS000001",
            properties={"accession": "JGAS000001"},
            distribution=[
                Distribution(
                    type="DataDownload",
                    encodingFormat="JSON",
                    contentUrl="https://example.com/JGAS000001.json",
                )
            ],
            isPartOf="jga",
            type="jga-study",
            name="Test Study",
            url="https://ddbj.nig.ac.jp/search/entry/jga-study/JGAS000001",
            organism=Organism(identifier="9606", name="Homo sapiens"),
            title="Test Title",
            description="Test Description",
            dbXrefs=[],
            sameAs=[],
            status="public",
            accessibility="controlled-access",
            dateCreated="2024-01-01T00:00:00Z",
            dateModified="2024-01-02T00:00:00Z",
            datePublished="2024-01-03T00:00:00Z",
        )

        assert jga.identifier == "JGAS000001"
        assert jga.type_ == "jga-study"
        assert jga.status == "public"
        assert jga.accessibility == "controlled-access"

    def test_jga_json_output(self) -> None:
        """JGA の JSON 出力が正しい形式になる。"""
        jga = JGA(
            identifier="JGAS000001",
            properties={},
            distribution=[],
            isPartOf="jga",
            type="jga-study",
            name=None,
            url="https://example.com",
            organism=None,
            title=None,
            description=None,
            dbXrefs=[],
            sameAs=[],
            status="public",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        json_str = jga.model_dump_json(by_alias=True)
        assert '"type":"jga-study"' in json_str
        assert '"isPartOf":"jga"' in json_str

    def test_new_list_fields_default_to_empty(self) -> None:
        """organization / publication / grant / externalLink / studyType / datasetType / vendor は
        default_factory=list で省略可能、全て空 list として生成される。"""
        jga = JGA(
            identifier="JGAS000001",
            properties={},
            distribution=[],
            isPartOf="jga",
            type="jga-study",
            name=None,
            url="https://example.com",
            organism=None,
            title=None,
            description=None,
            dbXrefs=[],
            sameAs=[],
            status="public",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )
        assert jga.organization == []
        assert jga.publication == []
        assert jga.grant == []
        assert jga.externalLink == []
        assert jga.studyType == []
        assert jga.datasetType == []
        assert jga.vendor == []

    def test_with_populated_common_types(self) -> None:
        """共通型 Organization / Publication と list[str] フィールドを渡して生成できる。"""
        jga = JGA(
            identifier="JGAS000001",
            properties={},
            distribution=[],
            isPartOf="jga",
            type="jga-study",
            name=None,
            url="https://example.com",
            organism=None,
            title=None,
            description=None,
            organization=[Organization(name="Individual")],
            publication=[Publication(id="24336570", dbType="pubmed")],
            studyType=["Exome Sequencing"],
            vendor=["Illumina"],
            dbXrefs=[],
            sameAs=[],
            status="public",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )
        assert jga.organization[0].name == "Individual"
        assert jga.publication[0].id_ == "24336570"
        assert jga.studyType == ["Exome Sequencing"]
        assert jga.vendor == ["Illumina"]


def _make_minimal_gea_kwargs() -> dict:
    return {
        "identifier": "E-GEAD-1005",
        "properties": {},
        "distribution": [],
        "isPartOf": "gea",
        "type": "gea",
        "url": "https://ddbj.nig.ac.jp/search/entry/gea/E-GEAD-1005",
        "status": "public",
        "accessibility": "public-access",
    }


class TestGEA:
    def test_minimal_instance(self) -> None:
        gea = GEA(**_make_minimal_gea_kwargs())
        assert gea.identifier == "E-GEAD-1005"
        assert gea.isPartOf == "gea"
        assert gea.type_ == "gea"
        assert gea.name is None
        assert gea.organism is None
        assert gea.title is None
        assert gea.description is None
        assert gea.organization == []
        assert gea.publication == []
        assert gea.experimentType == []
        assert gea.dbXrefs == []
        assert gea.sameAs == []
        assert gea.dateCreated is None
        assert gea.dateModified is None
        assert gea.datePublished is None

    def test_is_part_of_literal_rejects_other(self) -> None:
        kwargs = _make_minimal_gea_kwargs()
        kwargs["isPartOf"] = "jga"
        with pytest.raises(ValidationError):
            GEA(**kwargs)

    def test_type_literal_rejects_other(self) -> None:
        kwargs = _make_minimal_gea_kwargs()
        kwargs["type"] = "metabobank"
        with pytest.raises(ValidationError):
            GEA(**kwargs)

    def test_status_literal_rejects_private(self) -> None:
        kwargs = _make_minimal_gea_kwargs()
        kwargs["status"] = "private"
        with pytest.raises(ValidationError):
            GEA(**kwargs)

    def test_accessibility_literal_rejects_controlled_access(self) -> None:
        kwargs = _make_minimal_gea_kwargs()
        kwargs["accessibility"] = "controlled-access"
        with pytest.raises(ValidationError):
            GEA(**kwargs)

    def test_with_populated_common_types(self) -> None:
        kwargs = _make_minimal_gea_kwargs()
        kwargs["title"] = "Test study"
        kwargs["description"] = "A test description"
        kwargs["organization"] = [Organization(name="Kyushu University", role="submitter")]
        kwargs["publication"] = [
            Publication(id="21187441", dbType="pubmed", url="https://pubmed.ncbi.nlm.nih.gov/21187441/")
        ]
        kwargs["experimentType"] = ["transcription profiling by array", "RNA-seq of coding RNA"]
        kwargs["dateModified"] = "2025-01-31"
        kwargs["datePublished"] = "2025-01-31"
        gea = GEA(**kwargs)
        assert gea.organization[0].name == "Kyushu University"
        assert gea.organization[0].role == "submitter"
        assert gea.publication[0].id_ == "21187441"
        assert gea.publication[0].dbType == "pubmed"
        assert gea.experimentType == ["transcription profiling by array", "RNA-seq of coding RNA"]
        assert gea.dateCreated is None
        assert gea.dateModified == "2025-01-31"
        assert gea.datePublished == "2025-01-31"

    def test_json_output_uses_alias(self) -> None:
        gea = GEA(**_make_minimal_gea_kwargs())
        json_str = gea.model_dump_json(by_alias=True)
        assert '"type":"gea"' in json_str
        assert '"isPartOf":"gea"' in json_str


def _make_minimal_metabobank_kwargs() -> dict:
    return {
        "identifier": "MTBKS102",
        "properties": {},
        "distribution": [],
        "isPartOf": "metabobank",
        "type": "metabobank",
        "url": "https://ddbj.nig.ac.jp/search/entry/metabobank/MTBKS102",
        "status": "public",
        "accessibility": "public-access",
    }


class TestMetaboBank:
    def test_minimal_instance(self) -> None:
        mtb = MetaboBank(**_make_minimal_metabobank_kwargs())
        assert mtb.identifier == "MTBKS102"
        assert mtb.isPartOf == "metabobank"
        assert mtb.type_ == "metabobank"
        assert mtb.organization == []
        assert mtb.publication == []
        assert mtb.studyType == []
        assert mtb.experimentType == []
        assert mtb.submissionType == []
        assert mtb.dbXrefs == []
        assert mtb.dateCreated is None

    def test_is_part_of_literal_rejects_other(self) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs["isPartOf"] = "gea"
        with pytest.raises(ValidationError):
            MetaboBank(**kwargs)

    def test_type_literal_rejects_other(self) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs["type"] = "gea"
        with pytest.raises(ValidationError):
            MetaboBank(**kwargs)

    def test_status_literal_rejects_private(self) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs["status"] = "private"
        with pytest.raises(ValidationError):
            MetaboBank(**kwargs)

    def test_accessibility_literal_rejects_controlled_access(self) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs["accessibility"] = "controlled-access"
        with pytest.raises(ValidationError):
            MetaboBank(**kwargs)

    def test_with_populated_common_types(self) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs["title"] = "Arabidopsis thaliana leaf metabolite analysis"
        kwargs["organization"] = [Organization(name="Kazusa DNA Research Institute", role="submitter")]
        kwargs["publication"] = [Publication(id="10.1038/sample", dbType="doi", url="https://doi.org/10.1038/sample")]
        kwargs["studyType"] = ["untargeted metabolite profiling"]
        kwargs["experimentType"] = [
            "liquid chromatography-mass spectrometry",
            "fourier transform ion cyclotron resonance mass spectrometry",
        ]
        kwargs["submissionType"] = ["LC-DAD-MS"]
        kwargs["dateCreated"] = "2022-05-22"
        kwargs["dateModified"] = "2022-05-22"
        kwargs["datePublished"] = "2022-05-22"
        mtb = MetaboBank(**kwargs)
        assert mtb.title == "Arabidopsis thaliana leaf metabolite analysis"
        assert mtb.studyType == ["untargeted metabolite profiling"]
        assert len(mtb.experimentType) == 2
        assert mtb.submissionType == ["LC-DAD-MS"]
        assert mtb.dateCreated == "2022-05-22"

    def test_json_output_uses_alias(self) -> None:
        mtb = MetaboBank(**_make_minimal_metabobank_kwargs())
        json_str = mtb.model_dump_json(by_alias=True)
        assert '"type":"metabobank"' in json_str
        assert '"isPartOf":"metabobank"' in json_str


class TestMetaboBankExtensibleStringFields:
    """MetaboBank の studyType / experimentType / submissionType は MetaboBank 側の値追加に
    追従するため Literal 化せず list[str] として任意文字列を透過する。
    """

    @pytest.mark.parametrize(
        ("study", "experiment", "submission"),
        [
            (["untargeted metabolite profiling"], ["NMR"], ["LC-MS"]),
            (["future_study_type"], ["novel_experiment"], ["UNKNOWN-MS"]),
            (["Untargeted Metabolite Profiling"], ["direct infusion-mass spectrometry"], ["GC-MS"]),
        ],
    )
    def test_known_and_unknown_pass_through(
        self, study: list[str], experiment: list[str], submission: list[str]
    ) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs.update({"studyType": study, "experimentType": experiment, "submissionType": submission})
        mtb = MetaboBank(**kwargs)
        assert mtb.studyType == study
        assert mtb.experimentType == experiment
        assert mtb.submissionType == submission

    @given(
        study=st.lists(st.text(min_size=1, max_size=100), max_size=5),
        experiment=st.lists(st.text(min_size=1, max_size=100), max_size=5),
        submission=st.lists(st.text(min_size=1, max_size=100), max_size=5),
    )
    def test_arbitrary_strings_pass_through(
        self, study: list[str], experiment: list[str], submission: list[str]
    ) -> None:
        kwargs = _make_minimal_metabobank_kwargs()
        kwargs.update({"studyType": study, "experimentType": experiment, "submissionType": submission})
        mtb = MetaboBank(**kwargs)
        assert mtb.studyType == study
        assert mtb.experimentType == experiment
        assert mtb.submissionType == submission


class TestOrganization:
    def test_all_defaults_are_none(self) -> None:
        org = Organization()
        assert org.name is None
        assert org.abbreviation is None
        assert org.role is None
        assert org.organizationType is None
        assert org.department is None
        assert org.url is None

    @pytest.mark.parametrize("role", ["owner", "participant", "submitter", "broker"])
    def test_valid_role_values(self, role: str) -> None:
        org = Organization(role=role)  # type: ignore[arg-type]
        assert org.role == role

    @pytest.mark.parametrize("invalid_role", ["primary investigator", "OWNER", "submitterr", ""])
    def test_invalid_role_raises(self, invalid_role: str) -> None:
        with pytest.raises(ValidationError):
            Organization(role=invalid_role)  # type: ignore[arg-type]

    @pytest.mark.parametrize("org_type", ["institute", "center", "consortium", "lab"])
    def test_valid_organization_type_values(self, org_type: str) -> None:
        org = Organization(organizationType=org_type)  # type: ignore[arg-type]
        assert org.organizationType == org_type

    @pytest.mark.parametrize("invalid_type", ["university", "company", "INSTITUTE"])
    def test_invalid_organization_type_raises(self, invalid_type: str) -> None:
        with pytest.raises(ValidationError):
            Organization(organizationType=invalid_type)  # type: ignore[arg-type]


class TestPublication:
    def test_all_defaults_are_none(self) -> None:
        pub = Publication()
        assert pub.id_ is None
        assert pub.title is None
        assert pub.date is None
        assert pub.reference is None
        assert pub.url is None
        assert pub.dbType is None

    @pytest.mark.parametrize("dbtype", ["pubmed", "doi", "pmc"])
    def test_valid_dbtype_values(self, dbtype: str) -> None:
        pub = Publication(dbType=dbtype)  # type: ignore[arg-type]
        assert pub.dbType == dbtype

    @pytest.mark.parametrize("invalid_dbtype", ["PUBMED", "PubMed", "ePubmed", "eDOI", "ePMC", "eNotAvailable", ""])
    def test_invalid_dbtype_raises(self, invalid_dbtype: str) -> None:
        """旧 e-prefix 値や大小文字揺れは ValidationError (parse 側で正規化すること)."""
        with pytest.raises(ValidationError):
            Publication(dbType=invalid_dbtype)  # type: ignore[arg-type]

    def test_reference_field_name(self) -> None:
        pub = Publication(reference="Nature 2024")
        assert pub.reference == "Nature 2024"

    def test_dbtype_field_name(self) -> None:
        pub = Publication(dbType="pubmed")
        assert pub.dbType == "pubmed"

    def test_id_alias_in_input_and_output(self) -> None:
        pub = Publication(id="12345", dbType="pubmed")
        assert pub.id_ == "12345"
        dumped = pub.model_dump(by_alias=True)
        assert dumped["id"] == "12345"
        assert dumped["dbType"] == "pubmed"
        assert "id_" not in dumped
        assert "Reference" not in dumped
        assert "DbType" not in dumped

    def test_status_field_removed(self) -> None:
        """status フィールドは廃止済 (model に status 属性が定義されていない)."""
        pub = Publication()
        assert "status" not in Publication.model_fields
        assert not hasattr(pub, "status")


class TestBioSamplePackage:
    def test_name_is_required(self) -> None:
        with pytest.raises(ValidationError):
            BioSamplePackage()  # type: ignore[call-arg]

    def test_display_name_defaults_to_none(self) -> None:
        pkg = BioSamplePackage(name="Generic.1.0")
        assert pkg.name == "Generic.1.0"
        assert pkg.displayName is None

    def test_display_name_set(self) -> None:
        pkg = BioSamplePackage(name="Generic", displayName="Generic.1.0")
        assert pkg.displayName == "Generic.1.0"


def _make_minimal_bs_kwargs() -> dict:
    return {
        "identifier": "SAMD00000001",
        "properties": {},
        "distribution": [],
        "isPartOf": "biosample",
        "type": "biosample",
        "name": None,
        "url": "https://example.com",
        "organism": None,
        "title": None,
        "description": None,
        "organization": [],
        "model": [],
        "package": None,
        "dbXrefs": [],
        "sameAs": [],
        "status": "public",
        "accessibility": "public-access",
        "dateCreated": None,
        "dateModified": None,
        "datePublished": None,
    }


class TestBioSample:
    def test_minimal_instance(self) -> None:
        bs = BioSample(**_make_minimal_bs_kwargs())
        assert bs.identifier == "SAMD00000001"
        assert bs.organization == []
        assert bs.model == []
        assert bs.package is None

    def test_model_is_list_of_str(self) -> None:
        kwargs = _make_minimal_bs_kwargs()
        kwargs["model"] = ["MIGS.ba", "Generic"]
        bs = BioSample(**kwargs)
        assert bs.model == ["MIGS.ba", "Generic"]

    def test_package_is_biosamplepackage(self) -> None:
        kwargs = _make_minimal_bs_kwargs()
        kwargs["package"] = BioSamplePackage(name="Generic", displayName="Generic.1.0")
        bs = BioSample(**kwargs)
        assert bs.package is not None
        assert bs.package.name == "Generic"
        assert bs.package.displayName == "Generic.1.0"

    def test_organization_accepts_list_of_organization(self) -> None:
        kwargs = _make_minimal_bs_kwargs()
        kwargs["organization"] = [Organization(name="NCBI", abbreviation="NCBI")]
        bs = BioSample(**kwargs)
        assert bs.organization[0].name == "NCBI"
        assert bs.organization[0].abbreviation == "NCBI"
        # BS では role / organizationType は常に None
        assert bs.organization[0].role is None
        assert bs.organization[0].organizationType is None

    def test_old_attributes_field_is_rejected(self) -> None:
        """旧 attributes フィールドは schema から削除済のため extra field として扱われる。"""
        kwargs = _make_minimal_bs_kwargs()
        kwargs["attributes"] = []
        # BaseModel デフォルトは extra field を無視するので assignment はできる
        # が model_fields に attributes が存在しないことを保証する
        bs = BioSample(**kwargs)
        assert "attributes" not in BioSample.model_fields
        assert not hasattr(bs, "attributes")


def _make_minimal_sra_kwargs() -> dict:
    return {
        "identifier": "DRX000001",
        "properties": {},
        "distribution": [],
        "isPartOf": "sra",
        "type": "sra-experiment",
        "name": None,
        "url": "https://example.com",
        "organism": None,
        "title": None,
        "description": None,
        "organization": [],
        "publication": [],
        "libraryStrategy": [],
        "librarySource": [],
        "librarySelection": [],
        "libraryLayout": None,
        "platform": None,
        "instrumentModel": [],
        "analysisType": None,
        "dbXrefs": [],
        "sameAs": [],
        "status": "public",
        "accessibility": "public-access",
        "dateCreated": None,
        "dateModified": None,
        "datePublished": None,
    }


class TestSraExtensibleStringFields:
    """SRA の librarySource / libraryLayout / platform / analysisType は、INSDC 側の値追加に
    追従するため Literal 化せず任意文字列を透過する。既知値・未知値・PBT で検証する。
    """

    @pytest.mark.parametrize(
        "value",
        [
            "GENOMIC",
            "TRANSCRIPTOMIC SINGLE CELL",
            "SYNTHETIC",
            "FUTURE_LIBRARY_SOURCE",
            "genomic",
        ],
    )
    def test_library_source_pass_through(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["librarySource"] = [value]
        sra = SRA(**kwargs)
        assert sra.librarySource == [value]

    @pytest.mark.parametrize("value", ["PAIRED", "SINGLE", "MULTI", "unknown", "single"])
    def test_library_layout_pass_through(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["libraryLayout"] = value
        sra = SRA(**kwargs)
        assert sra.libraryLayout == value

    @pytest.mark.parametrize(
        "value",
        ["ILLUMINA", "SALUS", "FUTURE_SEQUENCER", "custom-vendor", "illumina"],
    )
    def test_platform_pass_through(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["platform"] = value
        sra = SRA(**kwargs)
        assert sra.platform == value

    @pytest.mark.parametrize(
        "value",
        ["DE_NOVO_ASSEMBLY", "VARIANT_CALLING", "novel_analysis", "de_novo_assembly"],
    )
    def test_analysis_type_pass_through(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["analysisType"] = value
        sra = SRA(**kwargs)
        assert sra.analysisType == value

    @given(
        library_sources=st.lists(st.text(min_size=1, max_size=100), max_size=5),
        library_layout=st.text(min_size=1, max_size=100),
        platform=st.text(min_size=1, max_size=100),
        analysis_type=st.text(min_size=1, max_size=100),
    )
    def test_arbitrary_strings_pass_through(
        self,
        library_sources: list[str],
        library_layout: str,
        platform: str,
        analysis_type: str,
    ) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs.update(
            {
                "librarySource": library_sources,
                "libraryLayout": library_layout,
                "platform": platform,
                "analysisType": analysis_type,
            }
        )
        sra = SRA(**kwargs)
        assert sra.librarySource == library_sources
        assert sra.libraryLayout == library_layout
        assert sra.platform == platform
        assert sra.analysisType == analysis_type


class TestSra:
    def test_minimal_instance(self) -> None:
        sra = SRA(**_make_minimal_sra_kwargs())
        assert sra.identifier == "DRX000001"
        assert sra.organization == []
        assert sra.publication == []
        assert sra.libraryStrategy == []
        assert sra.librarySource == []
        assert sra.librarySelection == []
        assert sra.libraryLayout is None
        assert sra.platform is None
        assert sra.instrumentModel == []
        assert sra.analysisType is None

    def test_organization_accepts_list(self) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["organization"] = [Organization(name="NIID")]
        sra = SRA(**kwargs)
        assert sra.organization[0].name == "NIID"
        assert sra.organization[0].role is None
        assert sra.organization[0].organizationType is None

    def test_publication_accepts_list(self) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["publication"] = [Publication(id="12345", dbType="pubmed")]
        sra = SRA(**kwargs)
        assert sra.publication[0].id_ == "12345"
        assert sra.publication[0].dbType == "pubmed"

    def test_experiment_technical_metadata(self) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs.update(
            {
                "libraryStrategy": ["WGS"],
                "librarySource": ["GENOMIC"],
                "librarySelection": ["RANDOM"],
                "libraryLayout": "PAIRED",
                "platform": "ILLUMINA",
                "instrumentModel": ["Illumina NovaSeq 6000"],
            }
        )
        sra = SRA(**kwargs)
        assert sra.libraryStrategy == ["WGS"]
        assert sra.librarySource == ["GENOMIC"]
        assert sra.librarySelection == ["RANDOM"]
        assert sra.libraryLayout == "PAIRED"
        assert sra.platform == "ILLUMINA"
        assert sra.instrumentModel == ["Illumina NovaSeq 6000"]

    def test_analysis_type_single(self) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["type"] = "sra-analysis"
        kwargs["analysisType"] = "DE_NOVO_ASSEMBLY"
        sra = SRA(**kwargs)
        assert sra.analysisType == "DE_NOVO_ASSEMBLY"
