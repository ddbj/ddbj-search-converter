"""Tests for bp_bs module."""
import tempfile
from pathlib import Path

from ddbj_search_converter.dblink.bp_bs import (process_ddbj_xml_file,
                                                process_ncbi_xml_file)


class TestProcessNcbiXmlFile:
    """Tests for process_ncbi_xml_file function."""

    def test_extract_biosample_bioproject_from_link(self, tmp_path: Path) -> None:
        """Test extracting BioSample -> BioProject relation from Link element."""
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample access="public" id="12345" accession="SAMN00000001">
  <Ids>
    <Id db="BioSample" is_primary="1">SAMN00000001</Id>
  </Ids>
  <Links>
    <Link target="bioproject" label="PRJNA12345"/>
  </Links>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ncbi_xml_file(xml_file)

        assert len(results) == 1
        assert results[0] == ("SAMN00000001", "PRJNA12345")
        assert len(skipped) == 0
        assert id_mappings == {"12345": "SAMN00000001"}

    def test_extract_biosample_bioproject_from_attribute(self, tmp_path: Path) -> None:
        """Test extracting BioSample -> BioProject relation from Attribute element."""
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample access="public" id="12345" accession="SAMN00000002">
  <Ids>
    <Id db="BioSample" is_primary="1">SAMN00000002</Id>
  </Ids>
  <Attributes>
    <Attribute attribute_name="bioproject_accession">PRJNA67890</Attribute>
  </Attributes>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ncbi_xml_file(xml_file)

        assert len(results) == 1
        assert results[0] == ("SAMN00000002", "PRJNA67890")
        assert len(skipped) == 0
        assert id_mappings == {"12345": "SAMN00000002"}

    def test_numeric_accession_is_skipped(self, tmp_path: Path) -> None:
        """Test that numeric accession (internal ID) is skipped."""
        # This is the problematic case where accession contains numeric ID
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample access="public" id="31458136" accession="31458136">
  <Ids>
    <Id db="BioSample" is_primary="1">SAMEA11488912</Id>
  </Ids>
  <Attributes>
    <Attribute attribute_name="bioproject_accession">PRJEB85464</Attribute>
  </Attributes>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ncbi_xml_file(xml_file)

        # Should be empty because accession="31458136" doesn't start with "SAM"
        assert len(results) == 0
        assert len(skipped) == 1
        assert skipped[0] == "31458136"
        # No id mapping because accession is invalid
        assert id_mappings == {}

    def test_real_entry_samea11488912(self, tmp_path: Path) -> None:
        """Test with the actual problematic entry from biosample_set.xml.gz."""
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample access="public" publication_date="2024-06-01T00:00:00.000" last_update="2025-11-26T09:34:30.000" submission_date="2022-10-26T10:09:43.363" id="31458136" accession="SAMEA11488912">
  <Ids>
    <Id db="BioSample" is_primary="1">SAMEA11488912</Id>
    <Id db="SRA">ERS13597352</Id>
  </Ids>
  <Description>
    <Title>Sample from Hordeum vulgare</Title>
    <Organism taxonomy_id="4513" taxonomy_name="Hordeum vulgare">
      <OrganismName>Hordeum vulgare</OrganismName>
    </Organism>
  </Description>
  <Owner>
    <Name>Centro Nacional de Recursos Fitogenéticos</Name>
  </Owner>
  <Models>
    <Model>Generic</Model>
  </Models>
  <Package display_name="Generic">Generic.1.0</Package>
  <Attributes>
    <Attribute attribute_name="biological material ID">ESP004:SBCC025</Attribute>
  </Attributes>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ncbi_xml_file(xml_file)

        # This entry has no bioproject_accession attribute, so should return empty
        assert len(results) == 0
        assert len(skipped) == 0
        # But id mapping should be present
        assert id_mappings == {"31458136": "SAMEA11488912"}

    def test_link_with_numeric_text_converts_to_prjna(self, tmp_path: Path) -> None:
        """Test that Link with numeric text is converted to PRJNA prefix."""
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample access="public" id="12345" accession="SAMN00000003">
  <Ids>
    <Id db="BioSample" is_primary="1">SAMN00000003</Id>
  </Ids>
  <Links>
    <Link target="bioproject">12345</Link>
  </Links>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ncbi_xml_file(xml_file)

        assert len(results) == 1
        assert results[0] == ("SAMN00000003", "PRJNA12345")
        assert len(skipped) == 0
        assert id_mappings == {"12345": "SAMN00000003"}


class TestProcessDdbjXmlFile:
    """Tests for process_ddbj_xml_file function."""

    def test_extract_biosample_bioproject(self, tmp_path: Path) -> None:
        """Test extracting BioSample -> BioProject relation from DDBJ XML."""
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample>
  <Ids>
    <Id namespace="BioSample">SAMD00000001</Id>
  </Ids>
  <Attributes>
    <Attribute attribute_name="bioproject_accession">PRJDB12345</Attribute>
  </Attributes>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ddbj_xml_file(xml_file)

        assert len(results) == 1
        assert results[0] == ("SAMD00000001", "PRJDB12345")
        assert len(skipped) == 0
        # DDBJ XML does not have id attribute, so mapping is empty
        assert id_mappings == {}

    def test_numeric_biosample_id_is_skipped(self, tmp_path: Path) -> None:
        """Test that numeric BioSample ID is skipped."""
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample>
  <Ids>
    <Id namespace="BioSample">12345678</Id>
  </Ids>
  <Attributes>
    <Attribute attribute_name="bioproject_accession">PRJDB12345</Attribute>
  </Attributes>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ddbj_xml_file(xml_file)

        # Should be empty because BioSample ID doesn't start with "SAM"
        assert len(results) == 0
        assert len(skipped) == 1
        assert skipped[0] == "12345678"
        assert id_mappings == {}

    def test_extract_biosample_bioproject_from_bioproject_id(self, tmp_path: Path) -> None:
        """Test extracting from bioproject_id attribute (actual DDBJ XML format)."""
        # This is the actual format used in DDBJ BioSample XML
        xml_content = """\
<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample last_update="2022-04-05T17:24:38.000+09:00" publication_date="2014-04-07T00:00:00.000+09:00" access="public">
    <Ids>
        <Id namespace="BioSample" is_primary="1">SAMD00000001</Id>
    </Ids>
    <Attributes>
        <Attribute attribute_name="sample_name">Bradyrhizobium sp. DOA9</Attribute>
        <Attribute attribute_name="strain">DOA9</Attribute>
        <Attribute attribute_name="bioproject_id">PRJDB1640</Attribute>
        <Attribute attribute_name="locus_tag_prefix">BDOA9</Attribute>
    </Attributes>
</BioSample>
</BioSampleSet>
"""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        results, skipped, id_mappings = process_ddbj_xml_file(xml_file)

        assert len(results) == 1
        assert results[0] == ("SAMD00000001", "PRJDB1640")
        assert len(skipped) == 0
        assert id_mappings == {}
