"""Tests for ddbj_search_converter.jsonl.bs module."""
from pathlib import Path
from typing import Any, Dict

import pytest

from ddbj_search_converter.jsonl.bs import (normalize_properties,
                                            parse_accessibility,
                                            parse_accession, parse_args,
                                            parse_attributes,
                                            parse_description, parse_model,
                                            parse_name, parse_organism,
                                            parse_package, parse_same_as,
                                            parse_status, parse_title,
                                            xml_entry_to_bs_instance)
from ddbj_search_converter.jsonl.utils import write_jsonl
from ddbj_search_converter.schema import BioSample
from ddbj_search_converter.xml_utils import iterate_xml_element


class TestParseAccession:
    """Tests for parse_accession function."""

    def test_parses_ncbi_accession(self) -> None:
        """NCBI 形式の accession を抽出する。"""
        sample: Dict[str, Any] = {"accession": "SAMN00000001"}
        assert parse_accession(sample, is_ddbj=False) == "SAMN00000001"

    def test_parses_ddbj_accession(self) -> None:
        """DDBJ 形式の accession を抽出する。"""
        sample: Dict[str, Any] = {
            "Ids": {
                "Id": {"namespace": "BioSample", "content": "SAMD00000001"}
            }
        }
        assert parse_accession(sample, is_ddbj=True) == "SAMD00000001"

    def test_parses_ddbj_accession_from_list(self) -> None:
        """DDBJ 形式の accession をリストから抽出する。"""
        sample: Dict[str, Any] = {
            "Ids": {
                "Id": [
                    {"db": "SRA", "content": "DRS000001"},
                    {"namespace": "BioSample", "content": "SAMD00000001"}
                ]
            }
        }
        assert parse_accession(sample, is_ddbj=True) == "SAMD00000001"

    def test_raises_when_missing_ncbi(self) -> None:
        """NCBI で accession がない場合は例外を発生する。"""
        sample: Dict[str, Any] = {}
        with pytest.raises(ValueError, match="No accession found"):
            parse_accession(sample, is_ddbj=False)

    def test_raises_when_missing_ddbj(self) -> None:
        """DDBJ で BioSample namespace ID がない場合は例外を発生する。"""
        sample: Dict[str, Any] = {
            "Ids": {
                "Id": {"db": "SRA", "content": "DRS000001"}
            }
        }
        with pytest.raises(ValueError, match="No BioSample namespace ID found"):
            parse_accession(sample, is_ddbj=True)


class TestParseOrganism:
    """Tests for parse_organism function."""

    def test_parses_ncbi_organism(self) -> None:
        """NCBI 形式の Organism を抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "Organism": {
                    "taxonomy_id": "9606",
                    "taxonomy_name": "Homo sapiens"
                }
            }
        }
        result = parse_organism(sample, is_ddbj=False)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_parses_ddbj_organism(self) -> None:
        """DDBJ 形式の Organism を抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "Organism": {
                    "taxonomy_id": "9606",
                    "OrganismName": "Homo sapiens"
                }
            }
        }
        result = parse_organism(sample, is_ddbj=True)
        assert result is not None
        assert result.identifier == "9606"
        assert result.name == "Homo sapiens"

    def test_returns_none_when_missing(self) -> None:
        """Organism がない場合は None を返す。"""
        sample: Dict[str, Any] = {"Description": {}}
        result = parse_organism(sample, is_ddbj=False)
        assert result is None


class TestParseTitle:
    """Tests for parse_title function."""

    def test_parses_title(self) -> None:
        """タイトルを抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "Title": "Test BioSample Title"
            }
        }
        assert parse_title(sample) == "Test BioSample Title"

    def test_returns_none_when_missing(self) -> None:
        """タイトルがない場合は None を返す。"""
        sample: Dict[str, Any] = {"Description": {}}
        assert parse_title(sample) is None


class TestParseName:
    """Tests for parse_name function."""

    def test_parses_name(self) -> None:
        """SampleName を抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "SampleName": "Sample ABC"
            }
        }
        assert parse_name(sample) == "Sample ABC"

    def test_returns_none_when_missing(self) -> None:
        """SampleName がない場合は None を返す。"""
        sample: Dict[str, Any] = {"Description": {}}
        assert parse_name(sample) is None


class TestParseDescription:
    """Tests for parse_description function."""

    def test_parses_string_comment(self) -> None:
        """文字列形式の Comment を抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "Comment": "Test description"
            }
        }
        assert parse_description(sample) == "Test description"

    def test_parses_paragraph_comment(self) -> None:
        """Paragraph 形式の Comment を抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "Comment": {
                    "Paragraph": "Test paragraph description"
                }
            }
        }
        assert parse_description(sample) == "Test paragraph description"

    def test_parses_paragraph_list_comment(self) -> None:
        """Paragraph リスト形式の Comment を抽出する。"""
        sample: Dict[str, Any] = {
            "Description": {
                "Comment": {
                    "Paragraph": ["First paragraph", "Second paragraph"]
                }
            }
        }
        assert parse_description(sample) == "First paragraph Second paragraph"

    def test_returns_none_when_missing(self) -> None:
        """Comment がない場合は None を返す。"""
        sample: Dict[str, Any] = {"Description": {}}
        assert parse_description(sample) is None


class TestParseAttributes:
    """Tests for parse_attributes function."""

    def test_parses_single_attribute(self) -> None:
        """単一の Attribute を抽出する。"""
        sample: Dict[str, Any] = {
            "Attributes": {
                "Attribute": {
                    "attribute_name": "strain",
                    "display_name": "Strain",
                    "content": "K-12"
                }
            }
        }
        result = parse_attributes(sample)
        assert len(result) == 1
        assert result[0].attribute_name == "strain"
        assert result[0].display_name == "Strain"
        assert result[0].content == "K-12"

    def test_parses_multiple_attributes(self) -> None:
        """複数の Attribute を抽出する。"""
        sample: Dict[str, Any] = {
            "Attributes": {
                "Attribute": [
                    {"attribute_name": "strain", "content": "K-12"},
                    {"attribute_name": "host", "content": "Homo sapiens"},
                ]
            }
        }
        result = parse_attributes(sample)
        assert len(result) == 2

    def test_parses_string_attribute(self) -> None:
        """文字列形式の Attribute を抽出する。"""
        sample: Dict[str, Any] = {
            "Attributes": {
                "Attribute": "simple string"
            }
        }
        result = parse_attributes(sample)
        assert len(result) == 1
        assert result[0].content == "simple string"

    def test_returns_empty_for_missing(self) -> None:
        """Attributes がない場合は空リストを返す。"""
        sample: Dict[str, Any] = {}
        result = parse_attributes(sample)
        assert result == []


class TestParseModel:
    """Tests for parse_model function."""

    def test_parses_string_model(self) -> None:
        """文字列形式の Model を抽出する。"""
        sample: Dict[str, Any] = {
            "Models": {
                "Model": "Generic"
            }
        }
        result = parse_model(sample)
        assert len(result) == 1
        assert result[0].name == "Generic"

    def test_parses_dict_model(self) -> None:
        """辞書形式の Model を抽出する。"""
        sample: Dict[str, Any] = {
            "Models": {
                "Model": {"content": "Generic"}
            }
        }
        result = parse_model(sample)
        assert len(result) == 1
        assert result[0].name == "Generic"

    def test_parses_multiple_models(self) -> None:
        """複数の Model を抽出する。"""
        sample: Dict[str, Any] = {
            "Models": {
                "Model": ["Generic", "MIMS"]
            }
        }
        result = parse_model(sample)
        assert len(result) == 2

    def test_returns_empty_for_missing(self) -> None:
        """Models がない場合は空リストを返す。"""
        sample: Dict[str, Any] = {}
        result = parse_model(sample)
        assert result == []


class TestParsePackage:
    """Tests for parse_package function."""

    def test_parses_ncbi_package(self) -> None:
        """NCBI 形式の Package を抽出する。"""
        sample: Dict[str, Any] = {
            "Package": {
                "content": "Generic.1.0",
                "display_name": "Generic"
            }
        }
        from ddbj_search_converter.schema import Model
        result = parse_package(sample, [], is_ddbj=False)
        assert result is not None
        assert result.name == "Generic.1.0"
        assert result.display_name == "Generic"

    def test_parses_string_package(self) -> None:
        """文字列形式の Package を抽出する。"""
        sample: Dict[str, Any] = {
            "Package": "Generic.1.0"
        }
        from ddbj_search_converter.schema import Model
        result = parse_package(sample, [], is_ddbj=False)
        assert result is not None
        assert result.name == "Generic.1.0"

    def test_uses_model_for_ddbj(self) -> None:
        """DDBJ の場合は Model から Package を生成する。"""
        sample: Dict[str, Any] = {}
        from ddbj_search_converter.schema import Model
        model = [Model(name="Generic")]
        result = parse_package(sample, model, is_ddbj=True)
        assert result is not None
        assert result.name == "Generic"
        assert result.display_name == "Generic"

    def test_returns_none_for_ddbj_without_model(self) -> None:
        """DDBJ で Model がない場合は None を返す。"""
        sample: Dict[str, Any] = {}
        result = parse_package(sample, [], is_ddbj=True)
        assert result is None


class TestParseSameAs:
    """Tests for parse_same_as function."""

    def test_parses_sra_id(self) -> None:
        """SRA ID を sameAs として抽出する。"""
        sample: Dict[str, Any] = {
            "Ids": {
                "Id": {"db": "SRA", "content": "SRS000001"}
            }
        }
        result = parse_same_as(sample)
        assert len(result) == 1
        assert result[0].identifier == "SRS000001"
        assert result[0].type_ == "sra-sample"

    def test_parses_multiple_ids(self) -> None:
        """複数の ID から SRA ID のみを抽出する。"""
        sample: Dict[str, Any] = {
            "Ids": {
                "Id": [
                    {"db": "SRA", "content": "SRS000001"},
                    {"namespace": "BioSample", "content": "SAMN00000001"},
                    {"db": "SRA", "content": "SRS000002"},
                ]
            }
        }
        result = parse_same_as(sample)
        assert len(result) == 2
        identifiers = [x.identifier for x in result]
        assert "SRS000001" in identifiers
        assert "SRS000002" in identifiers

    def test_returns_empty_for_missing(self) -> None:
        """Ids がない場合は空リストを返す。"""
        sample: Dict[str, Any] = {}
        result = parse_same_as(sample)
        assert result == []


class TestNormalizeProperties:
    """Tests for normalize_properties function."""

    def test_normalizes_owner_name_string(self) -> None:
        """Owner.Name の文字列を正規化する。"""
        sample: Dict[str, Any] = {
            "Owner": {
                "Name": "Test Organization"
            }
        }
        normalize_properties(sample)
        assert sample["Owner"]["Name"] == {"content": "Test Organization"}

    def test_normalizes_owner_name_list(self) -> None:
        """Owner.Name のリストを正規化する。"""
        sample: Dict[str, Any] = {
            "Owner": {
                "Name": ["Org1", {"content": "Org2"}]
            }
        }
        normalize_properties(sample)
        assert sample["Owner"]["Name"][0] == {"content": "Org1"}
        assert sample["Owner"]["Name"][1] == {"content": "Org2"}

    def test_normalizes_model_string(self) -> None:
        """Models.Model の文字列を正規化する。"""
        sample: Dict[str, Any] = {
            "Models": {
                "Model": "Generic"
            }
        }
        normalize_properties(sample)
        assert sample["Models"]["Model"] == {"content": "Generic"}

    def test_normalizes_model_list(self) -> None:
        """Models.Model のリストを正規化する。"""
        sample: Dict[str, Any] = {
            "Models": {
                "Model": ["Generic", {"content": "MIMS"}]
            }
        }
        normalize_properties(sample)
        assert sample["Models"]["Model"][0] == {"content": "Generic"}
        assert sample["Models"]["Model"][1] == {"content": "MIMS"}


class TestIterateXmlBiosamples:
    """Tests for iterate_xml_element function with BioSample tag."""

    def test_extracts_biosamples(self, tmp_path: Path) -> None:
        """<BioSample> 要素を抽出する。"""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample accession="SAMN00000001">
  <Description>
    <Title>Sample 1</Title>
  </Description>
</BioSample>
<BioSample accession="SAMN00000002">
  <Description>
    <Title>Sample 2</Title>
  </Description>
</BioSample>
</BioSampleSet>
"""
        xml_path = tmp_path / "test.xml"
        xml_path.write_bytes(xml_content.encode("utf-8"))

        biosamples = list(iterate_xml_element(xml_path, "BioSample"))
        assert len(biosamples) == 2
        assert b"SAMN00000001" in biosamples[0]
        assert b"SAMN00000002" in biosamples[1]


class TestXmlEntryToBsInstance:
    """Tests for xml_entry_to_bs_instance function."""

    def test_converts_ncbi_entry(self) -> None:
        """NCBI XML エントリを BioSample インスタンスに変換する。"""
        entry: Dict[str, Any] = {
            "BioSample": {
                "accession": "SAMN00000001",
                "Description": {
                    "Title": "Test Sample",
                    "Organism": {
                        "taxonomy_id": "9606",
                        "taxonomy_name": "Homo sapiens"
                    }
                },
                "Attributes": {
                    "Attribute": {"attribute_name": "strain", "content": "K-12"}
                },
                "Models": {
                    "Model": "Generic"
                },
                "Package": {
                    "content": "Generic.1.0",
                    "display_name": "Generic"
                }
            }
        }

        result = xml_entry_to_bs_instance(entry, is_ddbj=False)

        assert result.identifier == "SAMN00000001"
        assert result.title == "Test Sample"
        assert result.organism is not None
        assert result.organism.name == "Homo sapiens"
        assert len(result.attributes) == 1
        assert result.status == "live"
        assert result.accessibility == "public-access"

    def test_converts_ddbj_entry(self) -> None:
        """DDBJ XML エントリを BioSample インスタンスに変換する。"""
        entry: Dict[str, Any] = {
            "BioSample": {
                "Ids": {
                    "Id": {"namespace": "BioSample", "content": "SAMD00000001"}
                },
                "Description": {
                    "Title": "DDBJ Sample",
                    "Organism": {
                        "taxonomy_id": "9606",
                        "OrganismName": "Homo sapiens"
                    }
                },
                "Attributes": {
                    "Attribute": {"attribute_name": "strain", "content": "DOA9"}
                },
                "Models": {
                    "Model": "Generic"
                }
            }
        }

        result = xml_entry_to_bs_instance(entry, is_ddbj=True)

        assert result.identifier == "SAMD00000001"
        assert result.title == "DDBJ Sample"
        assert result.package is not None
        assert result.package.name == "Generic"


class TestWriteJsonl:
    """Tests for write_jsonl function."""

    def test_writes_jsonl(self, tmp_path: Path) -> None:
        """JSONL ファイルを書き込む。"""
        bs = BioSample(
            identifier="SAMN00000001",
            properties={"BioSample": {}},
            distribution=[],
            isPartOf="BioSample",
            type="biosample",
            name=None,
            url="https://example.com",
            organism=None,
            title="Test",
            description=None,
            attributes=[],
            model=[],
            package=None,
            dbXrefs=[],
            sameAs=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [bs])

        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "SAMN00000001" in content
        assert '"type":"biosample"' in content

    def test_writes_multiple_entries(self, tmp_path: Path) -> None:
        """複数エントリを書き込む。"""
        bs1 = BioSample(
            identifier="SAMN00000001",
            properties={},
            distribution=[],
            isPartOf="BioSample",
            type="biosample",
            name=None,
            url="https://example.com/1",
            organism=None,
            title=None,
            description=None,
            attributes=[],
            model=[],
            package=None,
            dbXrefs=[],
            sameAs=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )
        bs2 = BioSample(
            identifier="SAMN00000002",
            properties={},
            distribution=[],
            isPartOf="BioSample",
            type="biosample",
            name=None,
            url="https://example.com/2",
            organism=None,
            title=None,
            description=None,
            attributes=[],
            model=[],
            package=None,
            dbXrefs=[],
            sameAs=[],
            status="live",
            accessibility="public-access",
            dateCreated=None,
            dateModified=None,
            datePublished=None,
        )

        output_path = tmp_path / "test.jsonl"
        write_jsonl(output_path, [bs1, bs2])

        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert "SAMN00000001" in lines[0]
        assert "SAMN00000002" in lines[1]


class TestParseStatus:
    """Tests for parse_status function."""

    def test_parses_ncbi_status_live(self) -> None:
        """NCBI の Status/@status (live) を抽出する。"""
        sample: Dict[str, Any] = {
            "Status": {"status": "live", "when": "2024-01-01"}
        }
        assert parse_status(sample) == "live"

    def test_parses_ncbi_status_suppressed(self) -> None:
        """NCBI の Status/@status (suppressed) を抽出する。"""
        sample: Dict[str, Any] = {
            "Status": {"status": "suppressed", "when": "2024-01-01"}
        }
        assert parse_status(sample) == "suppressed"

    def test_returns_live_for_ddbj(self) -> None:
        """DDBJ の場合 (Status 要素なし) は "live" を返す。"""
        sample: Dict[str, Any] = {}
        assert parse_status(sample) == "live"

    def test_returns_live_when_status_missing_in_status_obj(self) -> None:
        """Status オブジェクトに status 属性がない場合は "live" を返す。"""
        sample: Dict[str, Any] = {
            "Status": {"when": "2024-01-01"}
        }
        assert parse_status(sample) == "live"


class TestParseAccessibility:
    """Tests for parse_accessibility function."""

    def test_parses_access_public(self) -> None:
        """@access が public の場合は public-access を返す。"""
        sample: Dict[str, Any] = {"access": "public"}
        assert parse_accessibility(sample) == "public-access"

    def test_parses_access_controlled(self) -> None:
        """@access が controlled の場合は controlled-access を返す。"""
        sample: Dict[str, Any] = {"access": "controlled"}
        assert parse_accessibility(sample) == "controlled-access"

    def test_returns_public_access_when_missing(self) -> None:
        """@access がない場合は "public-access" を返す。"""
        sample: Dict[str, Any] = {}
        assert parse_accessibility(sample) == "public-access"


class TestParseArgsIncremental:
    """Tests for parse_args function incremental update options."""

    def test_parse_args_full_flag(self) -> None:
        """--full フラグを指定した場合、full=True になる。"""
        _, _, _, _, full, _ = parse_args(["--full"])
        assert full is True

    def test_parse_args_default_incremental(self) -> None:
        """デフォルトでは full=False (差分更新モード)。"""
        _, _, _, _, full, _ = parse_args([])
        assert full is False

    def test_parse_args_with_parallel_and_full(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """parallel-num と full オプションを指定した場合のテスト。"""
        monkeypatch.setenv("DDBJ_SEARCH_CONVERTER_RESULT_DIR", str(tmp_path))
        config, tmp_xml_dir, output_dir, parallel_num, full, resume = parse_args([
            "--parallel-num", "8",
            "--full",
        ])
        assert parallel_num == 8
        assert full is True
        assert resume is False


class TestXmlEntryStatusAccessibility:
    """Tests for status/accessibility in xml_entry_to_bs_instance."""

    def test_ncbi_entry_parses_status_and_accessibility(self) -> None:
        """NCBI XML エントリから status と accessibility を正しく抽出する。"""
        entry: Dict[str, Any] = {
            "BioSample": {
                "accession": "SAMN00000001",
                "access": "public",
                "Status": {"status": "live"},
                "Description": {
                    "Title": "Test Sample"
                },
                "Attributes": {},
                "Models": {}
            }
        }
        result = xml_entry_to_bs_instance(entry, is_ddbj=False)
        assert result.status == "live"
        assert result.accessibility == "public-access"

    def test_ncbi_entry_controlled_suppressed(self) -> None:
        """NCBI XML エントリで controlled/suppressed の場合を確認する。"""
        entry: Dict[str, Any] = {
            "BioSample": {
                "accession": "SAMN00000002",
                "access": "controlled",
                "Status": {"status": "suppressed"},
                "Description": {},
                "Attributes": {},
                "Models": {}
            }
        }
        result = xml_entry_to_bs_instance(entry, is_ddbj=False)
        assert result.status == "suppressed"
        assert result.accessibility == "controlled-access"

    def test_ddbj_entry_defaults(self) -> None:
        """DDBJ XML エントリではデフォルト値が設定される。"""
        entry: Dict[str, Any] = {
            "BioSample": {
                "Ids": {
                    "Id": {"namespace": "BioSample", "content": "SAMD00000001"}
                },
                "Description": {},
                "Attributes": {},
                "Models": {}
            }
        }
        result = xml_entry_to_bs_instance(entry, is_ddbj=True)
        assert result.status == "live"
        assert result.accessibility == "public-access"
