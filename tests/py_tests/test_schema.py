"""Tests for ddbj_search_converter.schema module."""
import pytest

from ddbj_search_converter.schema import JGA, Distribution, Organism, Xref


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
            dbXref=[],
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
            dbXref=[],
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
