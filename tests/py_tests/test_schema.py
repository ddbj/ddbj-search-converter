"""Tests for ddbj_search_converter.schema module."""

import pytest
from pydantic import ValidationError

from ddbj_search_converter.schema import (
    JGA,
    SRA,
    BioProject,
    BioSample,
    BioSamplePackage,
    Distribution,
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
            isPartOf="BioProject",
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


class TestOrganization:
    """Tests for Organization common type (Phase A §2.1)."""

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
    """Tests for Publication common type (Phase A §2.3)."""

    def test_all_defaults_are_none(self) -> None:
        pub = Publication()
        assert pub.id_ is None
        assert pub.title is None
        assert pub.date is None
        assert pub.reference is None
        assert pub.url is None
        assert pub.dbType is None
        assert pub.status is None

    @pytest.mark.parametrize("dbtype", ["ePubmed", "eDOI", "ePMC", "eNotAvailable"])
    def test_valid_dbtype_values(self, dbtype: str) -> None:
        pub = Publication(dbType=dbtype)  # type: ignore[arg-type]
        assert pub.dbType == dbtype

    @pytest.mark.parametrize("invalid_dbtype", ["PUBMED", "pubmed", "PubMed", "doi"])
    def test_invalid_dbtype_raises(self, invalid_dbtype: str) -> None:
        """大文字含む非正規値はすべて ValidationError (parse 側で正規化すること)."""
        with pytest.raises(ValidationError):
            Publication(dbType=invalid_dbtype)  # type: ignore[arg-type]

    @pytest.mark.parametrize("status", ["ePublished", "eUnpublished"])
    def test_valid_status_values(self, status: str) -> None:
        pub = Publication(status=status)  # type: ignore[arg-type]
        assert pub.status == status

    @pytest.mark.parametrize("invalid_status", ["published", "unpublished", "preprint"])
    def test_invalid_status_raises(self, invalid_status: str) -> None:
        with pytest.raises(ValidationError):
            Publication(status=invalid_status)  # type: ignore[arg-type]

    def test_reference_field_name(self) -> None:
        pub = Publication(reference="Nature 2024")
        assert pub.reference == "Nature 2024"

    def test_dbtype_field_name(self) -> None:
        pub = Publication(dbType="ePubmed")
        assert pub.dbType == "ePubmed"

    def test_id_alias_in_input_and_output(self) -> None:
        pub = Publication(id="12345", dbType="ePubmed")
        assert pub.id_ == "12345"
        dumped = pub.model_dump(by_alias=True)
        assert dumped["id"] == "12345"
        assert dumped["dbType"] == "ePubmed"
        assert "id_" not in dumped
        assert "Reference" not in dumped
        assert "DbType" not in dumped


class TestBioSamplePackage:
    """Tests for BioSamplePackage (Phase A §3.1)."""

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
    return dict(
        identifier="SAMD00000001",
        properties={},
        distribution=[],
        isPartOf="BioSample",
        type="biosample",
        name=None,
        url="https://example.com",
        organism=None,
        title=None,
        description=None,
        organization=[],
        model=[],
        package=None,
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


class TestBioSample:
    """Tests for BioSample model (Phase A §3.1 確定形)."""

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
    return dict(
        identifier="DRX000001",
        properties={},
        distribution=[],
        isPartOf="sra",
        type="sra-experiment",
        name=None,
        url="https://example.com",
        organism=None,
        title=None,
        description=None,
        organization=[],
        publication=[],
        libraryStrategy=[],
        librarySource=[],
        librarySelection=[],
        libraryLayout=None,
        platform=None,
        instrumentModel=[],
        analysisType=None,
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


class TestLibrarySourceLiteral:
    """Tests for LibrarySource Literal (Phase A §3.3, §4.9.2)."""

    @pytest.mark.parametrize(
        "value",
        [
            "GENOMIC",
            "METAGENOMIC",
            "TRANSCRIPTOMIC",
            "VIRAL RNA",
            "OTHER",
            "METATRANSCRIPTOMIC",
            "TRANSCRIPTOMIC SINGLE CELL",
            "GENOMIC SINGLE CELL",
            "SYNTHETIC",
        ],
    )
    def test_all_9_values_accepted(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["librarySource"] = [value]
        sra = SRA(**kwargs)
        assert sra.librarySource == [value]

    @pytest.mark.parametrize("invalid", ["genomic", "GENOMIC ", "OTHER_MIX", ""])
    def test_invalid_values_rejected(self, invalid: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["librarySource"] = [invalid]
        with pytest.raises(ValidationError):
            SRA(**kwargs)


class TestLibraryLayoutLiteral:
    """Tests for LibraryLayout Literal (Phase A §3.3, §4.9.1)."""

    @pytest.mark.parametrize("value", ["PAIRED", "SINGLE"])
    def test_valid_values(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["libraryLayout"] = value
        sra = SRA(**kwargs)
        assert sra.libraryLayout == value

    @pytest.mark.parametrize("invalid", ["paired", "single", "MULTI", ""])
    def test_invalid_values_rejected(self, invalid: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["libraryLayout"] = invalid
        with pytest.raises(ValidationError):
            SRA(**kwargs)


class TestPlatformLiteral:
    """Tests for Platform Literal (Phase A §3.3, §4.9.3 の 20 値全量)."""

    @pytest.mark.parametrize(
        "value",
        [
            "ILLUMINA",
            "OXFORD_NANOPORE",
            "PACBIO_SMRT",
            "ION_TORRENT",
            "LS454",
            "CAPILLARY",
            "DNBSEQ",
            "BGISEQ",
            "ELEMENT",
            "ABI_SOLID",
            "COMPLETE_GENOMICS",
            "HELICOS",
            "ULTIMA",
            "GENEMIND",
            "VELA_DIAGNOSTICS",
            "TAPESTRI",
            "GENAPSYS",
            "SINGULAR_GENOMICS",
            "GENEUS_TECH",
            "SALUS",
        ],
    )
    def test_all_20_values_accepted(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["platform"] = value
        sra = SRA(**kwargs)
        assert sra.platform == value

    @pytest.mark.parametrize("invalid", ["illumina", "Illumina", "NOVEL_VENDOR", ""])
    def test_invalid_values_rejected(self, invalid: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["platform"] = invalid
        with pytest.raises(ValidationError):
            SRA(**kwargs)


class TestAnalysisTypeLiteral:
    """Tests for AnalysisType Literal (Phase A §3.3, §4.9.3)."""

    @pytest.mark.parametrize(
        "value",
        [
            "DE_NOVO_ASSEMBLY",
            "REFERENCE_ALIGNMENT",
            "ABUNDANCE_MEASUREMENT",
            "SEQUENCE_ANNOTATION",
        ],
    )
    def test_valid_values(self, value: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["analysisType"] = value
        sra = SRA(**kwargs)
        assert sra.analysisType == value

    @pytest.mark.parametrize("invalid", ["de_novo_assembly", "VARIANT_CALLING", ""])
    def test_invalid_values_rejected(self, invalid: str) -> None:
        kwargs = _make_minimal_sra_kwargs()
        kwargs["analysisType"] = invalid
        with pytest.raises(ValidationError):
            SRA(**kwargs)


class TestSra:
    """Tests for SRA model (Phase A §3.3 確定形)."""

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
        kwargs["publication"] = [Publication(id="12345", dbType="ePubmed")]
        sra = SRA(**kwargs)
        assert sra.publication[0].id_ == "12345"
        assert sra.publication[0].dbType == "ePubmed"

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
