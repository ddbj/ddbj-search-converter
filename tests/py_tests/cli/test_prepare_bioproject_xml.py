"""Tests for ddbj_search_converter.cli.prepare_bioproject_xml module."""
import os
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pytest

from ddbj_search_converter.logging.logger import _ctx


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestProcessBioprojectXml:
    """Tests for process_bioproject_xml function."""

    def test_splits_xml(self, tmp_path: Path, clean_ctx: None) -> None:
        """BioProject XML を分割する。"""
        from ddbj_search_converter.cli.prepare_bioproject_xml import \
            process_bioproject_xml
        from ddbj_search_converter.config import Config
        from ddbj_search_converter.logging.logger import init_logger

        config = Config(result_dir=tmp_path / "result", const_dir=tmp_path / "const")
        init_logger(run_name="test", config=config)

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
<Package>
  <Project accession="PRJNA3">
    <Title>Project 3</Title>
  </Project>
</Package>
</PackageSet>
"""
        xml_file = tmp_path / "bioproject.xml"
        xml_file.write_bytes(xml_content)

        output_dir = tmp_path / "output"

        output_files = process_bioproject_xml(
            xml_path=xml_file,
            output_dir=output_dir,
            prefix="test",
            batch_size=2,
        )

        assert len(output_files) == 2
        for f in output_files:
            assert f.exists()


class TestPrepareBioprojectXmlMain:
    """Tests for prepare_bioproject_xml main function."""

    def test_main_processes_xml_files(
        self, tmp_path: Path, clean_ctx: None
    ) -> None:
        """main() が BioProject XML を処理する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"

        ncbi_xml = tmp_path / "ncbi_bioproject.xml"
        ddbj_xml = tmp_path / "ddbj_bioproject.xml"

        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<PackageSet>
<Package>
  <Project accession="PRJNA1">
    <Title>Project 1</Title>
  </Project>
</Package>
</PackageSet>
"""
        ncbi_xml.write_bytes(xml_content)
        ddbj_xml.write_bytes(xml_content)

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            with patch(
                "ddbj_search_converter.cli.prepare_bioproject_xml.NCBI_BIOPROJECT_XML",
                ncbi_xml,
            ), patch(
                "ddbj_search_converter.cli.prepare_bioproject_xml.DDBJ_BIOPROJECT_XML",
                ddbj_xml,
            ):
                from ddbj_search_converter.cli.prepare_bioproject_xml import \
                    main
                main()

            bp_dir = result_dir / "bioproject"
            assert bp_dir.exists()
            xml_files = list(bp_dir.rglob("*.xml"))
            assert len(xml_files) >= 2

        finally:
            os.environ.clear()
            os.environ.update(original_env)
