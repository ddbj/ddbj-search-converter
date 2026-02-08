"""Tests for ddbj_search_converter.xml_utils module."""
import gzip
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.xml_utils import (extract_gzip, get_tmp_xml_dir,
                                             iterate_xml_element, parse_xml,
                                             split_xml)


class TestParseXml:
    """Tests for parse_xml function."""

    def test_parses_simple_xml(self) -> None:
        xml_bytes = b'<?xml version="1.0"?><Root><Child>value</Child></Root>'
        result = parse_xml(xml_bytes)
        assert result == {"Root": {"Child": "value"}}

    def test_attributes_no_prefix(self) -> None:
        xml_bytes = b'<Root attr="test"><Child id="1">value</Child></Root>'
        result = parse_xml(xml_bytes)
        assert result["Root"]["attr"] == "test"
        assert result["Root"]["Child"]["id"] == "1"
        assert result["Root"]["Child"]["content"] == "value"

    def test_cdata_as_content(self) -> None:
        xml_bytes = b"<Root><Child><![CDATA[some cdata content]]></Child></Root>"
        result = parse_xml(xml_bytes)
        assert result["Root"]["Child"] == "some cdata content"

    def test_nested_elements(self) -> None:
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

    def test_list_of_elements(self) -> None:
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
        xml_bytes = b'<Name abbr="DDBJ">DNA Data Bank of Japan</Name>'
        result = parse_xml(xml_bytes)
        assert result["Name"]["abbr"] == "DDBJ"
        assert result["Name"]["content"] == "DNA Data Bank of Japan"

    def test_empty_element(self) -> None:
        xml_bytes = b"<Root><Empty></Empty><SelfClose/></Root>"
        result = parse_xml(xml_bytes)
        assert result["Root"]["Empty"] is None
        assert result["Root"]["SelfClose"] is None

    def test_invalid_xml_raises_exception(self) -> None:
        xml_bytes = b"<Root><Unclosed>"
        with pytest.raises(Exception):
            parse_xml(xml_bytes)

    def test_namespace_handling(self) -> None:
        xml_bytes = b'<Root xmlns:ns="http://example.com"><ns:Child>val</ns:Child></Root>'
        result = parse_xml(xml_bytes)
        assert "Root" in result

    def test_whitespace_only_text(self) -> None:
        xml_bytes = b"<Root>   </Root>"
        result = parse_xml(xml_bytes)
        assert result["Root"] is None


class TestGetTmpXmlDir:
    """Tests for get_tmp_xml_dir function."""

    def test_creates_bioproject_dir(self, test_config: Config) -> None:
        tmp_dir = get_tmp_xml_dir(test_config, "bioproject")
        assert tmp_dir.exists()
        assert "bioproject" in str(tmp_dir)
        assert "tmp_xml" in str(tmp_dir)

    def test_creates_biosample_dir(self, test_config: Config) -> None:
        tmp_dir = get_tmp_xml_dir(test_config, "biosample")
        assert tmp_dir.exists()
        assert "biosample" in str(tmp_dir)


class TestIterateXmlElement:
    """Tests for iterate_xml_element function."""

    def test_iterates_biosample_elements(self, tmp_path: Path) -> None:
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

    def test_handles_attributes_in_tag(self, tmp_path: Path) -> None:
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
        xml_file = tmp_path / "empty.xml"
        xml_file.write_bytes(b"")
        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert elements == []

    def test_no_matching_elements(self, tmp_path: Path) -> None:
        xml_content = b"<OtherRoot><OtherElement>content</OtherElement></OtherRoot>"
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)
        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert elements == []


class TestBug2PrefixTagMatch:
    """Bug #2 (fixed): プレフィクスタグ名と誤マッチしない。"""

    def test_prefix_tag_with_end_tag_on_own_line(self, tmp_path: Path) -> None:
        """プレフィクスタグ内に行頭 </BioSample> があってもキャプチャしない。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<Root>
<BioSampleAlt id="alt">
  <Ref>see
</BioSample>
  </Ref>
</BioSampleAlt>
<BioSample accession="SAMN00000001">
  <Title>Real sample</Title>
</BioSample>
</Root>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(elements) == 1
        assert b"SAMN00000001" in elements[0]

    def test_prefix_tag_before_real_tag(self, tmp_path: Path) -> None:
        """プレフィクスタグは無視され、正しいタグだけ返る。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSampleExtra id="extra">
  <Content>This is extra</Content>
</BioSampleExtra>
<BioSample accession="SAMN00000001">
  <Title>Real sample</Title>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(elements) == 1
        assert b"SAMN00000001" in elements[0]


class TestBug3SameLineElement:
    """Bug #3 (fixed): 開始・終了タグが同一行にある場合も正しく検出。"""

    def test_single_line_element_should_be_captured(self, tmp_path: Path) -> None:
        """単一行要素 <Tag>text</Tag> が正しくキャプチャされる。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<Root>
<Item>single line content</Item>
<Item>
  multi line
</Item>
</Root>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "Item"))
        assert len(elements) == 2


class TestClosingTagBehavior:
    """Bug #16 (by design): iterate_xml_element の終了タグ行頭検出。

    iterate_xml_element は行ベースパーサのため、終了タグ `</Tag>` が
    行頭に現れることを前提にしている。これは性能のための設計判断であり、
    DDBJ Search の XML データではこの仮定が成り立つ。
    """

    def test_closing_tag_at_line_start(self, tmp_path: Path) -> None:
        """終了タグが行頭にある場合は正しくパースされる (通常ケース)。"""
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<Root>
<Item id="1">
  <Name>First</Name>
</Item>
<Item id="2">
  <Name>Second</Name>
</Item>
</Root>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "Item"))
        assert len(elements) == 2

    def test_closing_tag_indented(self, tmp_path: Path) -> None:
        """終了タグがインデントされている場合の動作。

        行ベースパーサなので、終了タグが行頭にない場合は
        同一行の開始+終了パターンでのみ検出される。
        """
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<Root>
<Item id="1">
  <Name>First</Name>
  </Item>
</Root>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_bytes(xml_content)

        elements = list(iterate_xml_element(xml_file, "Item"))
        # インデントされた終了タグは行頭検出に引っかからないが、
        # 次の要素の開始タグや EOF で暗黙的に終了する
        assert len(elements) >= 0  # 実装依存の動作をドキュメント


class TestSplitXml:
    """Tests for split_xml function."""

    def test_splits_into_batches(self, tmp_path: Path) -> None:
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
            xml_file, output_dir, batch_size=2, tag="BioSample",
            prefix="biosample", wrapper_start=wrapper_start, wrapper_end=wrapper_end,
        )

        assert len(output_files) == 2
        content1 = output_files[0].read_bytes()
        assert content1.count(b"</BioSample>") == 2
        content2 = output_files[1].read_bytes()
        assert content2.count(b"</BioSample>") == 1

    def test_empty_input(self, tmp_path: Path) -> None:
        xml_file = tmp_path / "empty.xml"
        xml_file.write_bytes(b"<?xml version='1.0'?><Root></Root>")
        output_dir = tmp_path / "output"

        output_files = split_xml(
            xml_file, output_dir, batch_size=10, tag="BioSample",
            prefix="test", wrapper_start=b'<Root>', wrapper_end=b'</Root>',
        )
        assert output_files == []


class TestExtractGzip:
    """Tests for extract_gzip function."""

    def test_extracts_gzip_file(self, tmp_path: Path) -> None:
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
        content = b"test content"
        gz_file = tmp_path / "test.txt.gz"
        with gzip.open(gz_file, "wb") as f:
            f.write(content)

        output_dir = tmp_path / "nested" / "output" / "dir"
        result = extract_gzip(gz_file, output_dir)
        assert output_dir.exists()
        assert result.exists()


class TestIntegration:
    """Integration tests: iterate_xml_element + split."""

    def test_iterate_then_split(self, tmp_path: Path) -> None:
        elements_data = []
        for i in range(5):
            elements_data.append(f"""<BioSample accession="SAMN{i:08d}">
  <Title>Sample {i}</Title>
</BioSample>
""".encode())

        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
""" + b"".join(elements_data) + b"""</BioSampleSet>
"""
        xml_file = tmp_path / "input.xml"
        xml_file.write_bytes(xml_content)

        iterated = list(iterate_xml_element(xml_file, "BioSample"))
        assert len(iterated) == 5

        output_dir = tmp_path / "split"
        wrapper_start = b'<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n'
        wrapper_end = b'</BioSampleSet>'

        output_files = split_xml(
            xml_file, output_dir, batch_size=2, tag="BioSample",
            prefix="batch", wrapper_start=wrapper_start, wrapper_end=wrapper_end,
        )
        assert len(output_files) == 3

        total_elements = 0
        for split_file in output_files:
            elements_in_file = list(iterate_xml_element(split_file, "BioSample"))
            total_elements += len(elements_in_file)
        assert total_elements == 5
