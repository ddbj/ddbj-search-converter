"""Tests for ddbj_search_converter.schema module."""

import pytest
from pydantic import ValidationError

from ddbj_search_converter.schema import JGA, BioProject, Distribution, Organism, Xref


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
            status="live",
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
            status="live",
            accessibility="controlled-access",
            dateCreated="2024-01-01T00:00:00Z",
            dateModified="2024-01-02T00:00:00Z",
            datePublished="2024-01-03T00:00:00Z",
        )

        assert jga.identifier == "JGAS000001"
        assert jga.type_ == "jga-study"
        assert jga.status == "live"
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
            status="live",
            accessibility="controlled-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        json_str = jga.model_dump_json(by_alias=True)
        assert '"type":"jga-study"' in json_str
        assert '"isPartOf":"jga"' in json_str
