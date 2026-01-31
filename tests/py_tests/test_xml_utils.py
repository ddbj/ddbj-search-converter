"""Tests for ddbj_search_converter.xml_utils module."""
import gzip
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.xml_utils import (extract_gzip, get_tmp_xml_dir,
                                             iterate_xml_element, parse_xml,
                                             split_xml)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestParseXml:
    """Tests for parse_xml function."""

    def test_parses_simple_xml(self) -> None:
        """シンプルな XML をパースできる。"""
        xml_bytes = b'<?xml version="1.0"?><Root><Child>value</Child></Root>'
        result = parse_xml(xml_bytes)
        assert result == {"Root": {"Child": "value"}}

    def test_attributes_no_prefix(self) -> None:
        """属性はプレフィックスなしでパースされる。"""
        xml_bytes = b'<Root attr="test"><Child id="1">value</Child></Root>'
        result = parse_xml(xml_bytes)
        assert result["Root"]["attr"] == "test"
        assert result["Root"]["Child"]["id"] == "1"
        assert result["Root"]["Child"]["content"] == "value"

    def test_cdata_as_content(self) -> None:
        """CDATA は content キーでパースされる。"""
        xml_bytes = b"<Root><Child><![CDATA[some cdata content]]></Child></Root>"
        result = parse_xml(xml_bytes)
        assert result["Root"]["Child"] == "some cdata content"

    def test_nested_elements(self) -> None:
        """ネストされた要素をパースできる。"""
        xml_bytes = b"""
        <BioSample accession="SAMN00000001">
            <Description>
                <Title>Test Sample</Title>
                <Organism taxonomy_id="9606">
                    <OrganismName>Homo sapiens</OrganismName>
                </Organism>
            </Description>
        </BioSample>
        """
        result = parse_xml(xml_bytes)
        assert result["BioSample"]["accession"] == "SAMN00000001"
        assert result["BioSample"]["Description"]["Title"] == "Test Sample"
        assert result["BioSample"]["Description"]["Organism"]["taxonomy_id"] == "9606"
        assert result["BioSample"]["Description"]["Organism"]["OrganismName"] == "Homo sapiens"

    def test_list_of_elements(self) -> None:
        """同名の要素が複数ある場合はリストになる。"""
        xml_bytes = b"""
        <Root>
            <Item>first</Item>
            <Item>second</Item>
            <Item>third</Item>
        </Root>
        """
        result = parse_xml(xml_bytes)
        assert result["Root"]["Item"] == ["first", "second", "third"]

    def test_mixed_content_with_attributes(self) -> None:
        """属性とテキストが混在する要素をパースできる。"""
        xml_bytes = b'<Name abbr="DDBJ">DNA Data Bank of Japan</Name>'
        result = parse_xml(xml_bytes)
        assert result["Name"]["abbr"] == "DDBJ"
        assert result["Name"]["content"] == "DNA Data Bank of Japan"

    def test_empty_element(self) -> None:
        """空要素をパースできる。"""
        xml_bytes = b"<Root><Empty></Empty><SelfClose/></Root>"
        result = parse_xml(xml_bytes)
        assert result["Root"]["Empty"] is None
        assert result["Root"]["SelfClose"] is None

    def test_invalid_xml_raises_exception(self) -> None:
        """不正な XML は例外を発生させる。"""
        xml_bytes = b"<Root><Unclosed>"
        with pytest.raises(Exception):
            parse_xml(xml_bytes)


class TestGetTmpXmlDir:
    """Tests for get_tmp_xml_dir function."""

    def test_creates_bioproject_dir(self, test_config: Config) -> None:
        """bioproject 用一時ディレクトリが作成される。"""
        tmp_dir = get_tmp_xml_dir(test_config, "bioproject")
        assert tmp_dir.exists()
        assert tmp_dir.is_dir()
        assert "bioproject" in str(tmp_dir)
        assert "tmp_xml" in str(tmp_dir)

    def test_creates_biosample_dir(self, test_config: Config) -> None:
        """biosample 用一時ディレクトリが作成される。"""
        tmp_dir = get_tmp_xml_dir(test_config, "biosample")
        assert tmp_dir.exists()
        assert tmp_dir.is_dir()
        assert "biosample" in str(tmp_dir)
        assert "tmp_xml" in str(tmp_dir)


class TestIterateXmlElement:
    """Tests for iterate_xml_element function."""

    def test_iterates_biosample_elements(self, tmp_path: Path) -> None:
        """BioSample 要素を正しくイテレートする。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample accession="SAMN00000001">
  <Title>Sample 1</Title>
</BioSample>
<BioSample accession="SAMN00000002">
  <Title>Sample 2</Title>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(elements) == 2
        assert b"SAMN00000001" in elements[0]
        assert b"SAMN00000002" in elements[1]

    def test_iterates_package_elements(self, tmp_path: Path) -> None:
        """Package (BioProject) 要素を正しくイテレートする。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
<Package>
  <Project accession="PRJNA1">
    <Title>Project 1</Title>
  </Project>
</Package>
<Package>
  <Project accession="PRJNA2">
    <Title>Project 2</Title>
  </Project>
</Package>
</PackageSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "Package"))
        assert len(elements) == 2
        assert b"PRJNA1" in elements[0]
        assert b"PRJNA2" in elements[1]

    def test_handles_attributes_in_tag(self, tmp_path: Path) -> None:
        """属性付きタグを正しく検出する。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample submission_date="2024-01-01" accession="SAMN00000001">
  <Title>Sample with attributes</Title>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(elements) == 1
        assert b"submission_date" in elements[0]

    def test_empty_file(self, tmp_path: Path) -> None:
        """空ファイルの場合は要素がない。"""
        xml_file = tmp_path / "empty.xml"
        xml_file.write_bytes(b"")

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert elements == []

    def test_no_matching_elements(self, tmp_path: Path) -> None:
        """マッチする要素がない場合は空リスト。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<OtherRoot>
<OtherElement>content</OtherElement>
</OtherRoot>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert elements == []

    def test_nested_elements(self, tmp_path: Path) -> None:
        """ネストされた要素を正しく処理する。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample accession="SAMN00000001">
  <Description>
    <Title>Test</Title>
    <Paragraph>Content</Paragraph>
  </Description>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(elements) == 1
        assert b"<Description>" in elements[0]
        assert b"<Paragraph>" in elements[0]


class TestSplitXml:
    """Tests for split_xml function."""

    def test_splits_into_batches(self, tmp_path: Path) -> None:
        """バッチサイズに従って分割される。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample accession="SAMN00000001">
  <Title>Sample 1</Title>
</BioSample>
<BioSample accession="SAMN00000002">
  <Title>Sample 2</Title>
</BioSample>
<BioSample accession="SAMN00000003">
  <Title>Sample 3</Title>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "input.xml"
        xml_file.write_bytes(xml_content)

        output_dir = tmp_path / "output"
        wrapper_start = b'<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n'
        wrapper_end = b'</BioSampleSet>'

        output_files = split_xml(
            xml_file,
            output_dir,
            batch_size=2,
            tag="BioSample",
            prefix="biosample",
            wrapper_start=wrapper_start,
            wrapper_end=wrapper_end,
        )

        assert len(output_files) == 2
        assert output_files[0].name == "biosample_1.xml"
        assert output_files[1].name == "biosample_2.xml"

        # 最初のファイルは2要素
        content1 = output_files[0].read_bytes()
        assert content1.count(b"</BioSample>") == 2
        assert content1.startswith(wrapper_start)
        assert content1.endswith(wrapper_end)

        # 2番目のファイルは1要素
        content2 = output_files[1].read_bytes()
        assert content2.count(b"</BioSample>") == 1

    def test_single_batch(self, tmp_path: Path) -> None:
        """要素数がバッチサイズ以下の場合は1ファイル。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample accession="SAMN00000001">
  <Title>Sample 1</Title>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "input.xml"
        xml_file.write_bytes(xml_content)

        output_dir = tmp_path / "output"
        wrapper_start = b'<Root>\n'
        wrapper_end = b'</Root>'

        output_files = split_xml(
            xml_file,
            output_dir,
            batch_size=10,
            tag="BioSample",
            prefix="test",
            wrapper_start=wrapper_start,
            wrapper_end=wrapper_end,
        )

        assert len(output_files) == 1
        assert output_files[0].name == "test_1.xml"

    def test_empty_input(self, tmp_path: Path) -> None:
        """空の入力ファイルの場合は空リスト。"""
        xml_file = tmp_path / "empty.xml"
        xml_file.write_bytes(b"<?xml version='1.0'?><Root></Root>")

        output_dir = tmp_path / "output"

        output_files = split_xml(
            xml_file,
            output_dir,
            batch_size=10,
            tag="BioSample",
            prefix="test",
            wrapper_start=b'<Root>',
            wrapper_end=b'</Root>',
        )

        assert output_files == []


class TestExtractGzip:
    """Tests for extract_gzip function."""

    def test_extracts_gzip_file(self, tmp_path: Path) -> None:
        """gzip ファイルを展開する。"""
        # Create a test gzip file
        content = b"<?xml version='1.0'?><Test>Content</Test>"
        gz_file = tmp_path / "test.xml.gz"
        with gzip.open(gz_file, "wb") as f:
            f.write(content)

        output_dir = tmp_path / "output"
        result = extract_gzip(gz_file, output_dir)

        assert result.exists()
        assert result.name == "test.xml"
        assert result.read_bytes() == content

    def test_creates_output_dir(self, tmp_path: Path) -> None:
        """出力ディレクトリが存在しない場合は作成する。"""
        content = b"test content"
        gz_file = tmp_path / "test.txt.gz"
        with gzip.open(gz_file, "wb") as f:
            f.write(content)

        output_dir = tmp_path / "nested" / "output" / "dir"
        result = extract_gzip(gz_file, output_dir)

        assert output_dir.exists()
        assert result.exists()

    def test_with_fixture_file(self) -> None:
        """実際の fixture ファイルで展開をテストする。"""
        fixture_gz = FIXTURES_DIR / "usr/local/resources/biosample/biosample_set.xml.gz"
        if not fixture_gz.exists():
            pytest.skip("Fixture file not found")

        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "output"
            result = extract_gzip(fixture_gz, output_dir)

            assert result.exists()
            assert result.name == "biosample_set.xml"
            # Check that it starts with XML declaration
            content = result.read_bytes()[:100]
            assert b"<?xml" in content


class TestIntegration:
    """Integration tests using iterate_xml_element with real-ish data."""

    def test_iterate_then_split(self, tmp_path: Path) -> None:
        """iterate と split の連携テスト。"""
        # Create a larger test file
        elements = []
        for i in range(5):
            elements.append(f"""<BioSample accession="SAMN{i:08d}">
  <Title>Sample {i}</Title>
</BioSample>
""".encode())

        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
""" + b"".join(elements) + b"""</BioSampleSet>
"""

        xml_file = tmp_path / "input.xml"
        xml_file.write_bytes(xml_content)

        # First, verify iterate works
        iterated = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(iterated) == 5

        # Then verify split works
        output_dir = tmp_path / "split"
        wrapper_start = b'<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n'
        wrapper_end = b'</BioSampleSet>'

        output_files = split_xml(
            xml_file,
            output_dir,
            batch_size=2,
            tag="BioSample",
            prefix="batch",
            wrapper_start=wrapper_start,
            wrapper_end=wrapper_end,
        )

        assert len(output_files) == 3  # 2 + 2 + 1

        # Verify each split file can be iterated
        total_elements = 0
        for split_file in output_files:
            elements_in_file = list(iterate_xml_element(split_file, "BioSample"))
            total_elements += len(elements_in_file)

        assert total_elements == 5
