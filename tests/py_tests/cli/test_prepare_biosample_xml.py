"""Tests for ddbj_search_converter.cli.prepare_biosample_xml module."""
import gzip
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


class TestProcessBiosampleXml:
    """Tests for process_biosample_xml function."""

    def test_extracts_and_splits_gzip(self, tmp_path: Path, clean_ctx: None) -> None:
        """gzip BioSample XML を展開して分割する。"""
        from ddbj_search_converter.cli.prepare_biosample_xml import \
            process_biosample_xml
        from ddbj_search_converter.config import Config
        from ddbj_search_converter.logging.logger import init_logger

        config = Config(result_dir=tmp_path / "result", const_dir=tmp_path / "const")
        init_logger(run_name="test", config=config)

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
        gz_file = tmp_path / "biosample.xml.gz"
        with gzip.open(gz_file, "wb") as f:
            f.write(xml_content)

        output_files = process_biosample_xml(
            config=config,
            gz_path=gz_file,
            prefix="test",
            batch_size=2,
        )

        assert len(output_files) == 2
        for f in output_files:
            assert f.exists()


class TestPrepareBiosampleXmlMain:
    """Tests for prepare_biosample_xml main function."""

    def test_main_processes_xml_files(
        self, tmp_path: Path, clean_ctx: None
    ) -> None:
        """main() が BioSample XML を処理する。"""
        result_dir = tmp_path / "result"
        const_dir = tmp_path / "const"

        ncbi_xml_gz = tmp_path / "ncbi_biosample.xml.gz"
        ddbj_xml_gz = tmp_path / "ddbj_biosample.xml.gz"

        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<BioSampleSet>
<BioSample accession="SAMN00000001">
  <Title>Sample 1</Title>
</BioSample>
</BioSampleSet>
"""
        with gzip.open(ncbi_xml_gz, "wb") as f:
            f.write(xml_content)
        with gzip.open(ddbj_xml_gz, "wb") as f:
            f.write(xml_content)

        env = {
            "DDBJ_SEARCH_CONVERTER_RESULT_DIR": str(result_dir),
            "DDBJ_SEARCH_CONVERTER_CONST_DIR": str(const_dir),
        }

        original_env = os.environ.copy()
        try:
            os.environ.update(env)

            with patch(
                "ddbj_search_converter.cli.prepare_biosample_xml.NCBI_BIOSAMPLE_XML",
                ncbi_xml_gz,
            ), patch(
                "ddbj_search_converter.cli.prepare_biosample_xml.DDBJ_BIOSAMPLE_XML",
                ddbj_xml_gz,
            ):
                from ddbj_search_converter.cli.prepare_biosample_xml import \
                    main
                main()

            bs_dir = result_dir / "biosample"
            assert bs_dir.exists()
            xml_files = list(bs_dir.rglob("*.xml"))
            assert len(xml_files) >= 2

        finally:
            os.environ.clear()
            os.environ.update(original_env)
