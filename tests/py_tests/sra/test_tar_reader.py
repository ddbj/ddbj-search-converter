"""Tests for TarXMLReader class."""

import io
import tarfile
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import run_logger
from ddbj_search_converter.sra.tar_reader import TarXMLReader


@pytest.fixture
def sample_tar(tmp_path: Path) -> Path:
    """Create a sample tar file with XML files."""
    tar_path = tmp_path / "sample.tar"

    with tarfile.open(tar_path, "w") as tar:
        # Add a submission XML
        submission_content = b"<SUBMISSION>test</SUBMISSION>"
        info = tarfile.TarInfo(name="DRA000001/DRA000001.submission.xml")
        info.size = len(submission_content)
        tar.addfile(info, fileobj=io.BytesIO(submission_content))

        # Add a run XML
        run_content = b"<RUN_SET><RUN accession='DRR000001'/></RUN_SET>"
        info = tarfile.TarInfo(name="DRA000001/DRA000001.run.xml")
        info.size = len(run_content)
        tar.addfile(info, fileobj=io.BytesIO(run_content))

        # Add another submission
        submission_content2 = b"<SUBMISSION>test2</SUBMISSION>"
        info = tarfile.TarInfo(name="ERA123456/ERA123456.submission.xml")
        info.size = len(submission_content2)
        tar.addfile(info, fileobj=io.BytesIO(submission_content2))

    return tar_path


class TestTarXMLReader:
    """Tests for TarXMLReader class."""

    def test_exists_returns_true_for_existing_file(self, sample_tar: Path, test_config: Config) -> None:
        """Test exists() returns True for existing XML file."""
        with run_logger(config=test_config), TarXMLReader(sample_tar) as reader:
            assert reader.exists("DRA000001", "submission") is True
            assert reader.exists("DRA000001", "run") is True
            assert reader.exists("ERA123456", "submission") is True

    def test_exists_returns_false_for_missing_file(self, sample_tar: Path, test_config: Config) -> None:
        """Test exists() returns False for non-existing XML file."""
        with run_logger(config=test_config), TarXMLReader(sample_tar) as reader:
            assert reader.exists("DRA000001", "experiment") is False
            assert reader.exists("DRA000002", "submission") is False

    def test_read_xml_returns_content(self, sample_tar: Path, test_config: Config) -> None:
        """Test read_xml() returns XML content."""
        with run_logger(config=test_config), TarXMLReader(sample_tar) as reader:
            content = reader.read_xml("DRA000001", "submission")
            assert content == b"<SUBMISSION>test</SUBMISSION>"

            content = reader.read_xml("DRA000001", "run")
            assert content == b"<RUN_SET><RUN accession='DRR000001'/></RUN_SET>"

    def test_read_xml_returns_none_for_missing_file(self, sample_tar: Path, test_config: Config) -> None:
        """Test read_xml() returns None for non-existing file."""
        with run_logger(config=test_config), TarXMLReader(sample_tar) as reader:
            content = reader.read_xml("DRA000001", "experiment")
            assert content is None

    def test_get_member_count(self, sample_tar: Path, test_config: Config) -> None:
        """Test get_member_count() returns correct count."""
        with run_logger(config=test_config), TarXMLReader(sample_tar) as reader:
            assert reader.get_member_count() == 3


class TestTarXMLReaderWithDuplicates:
    """Tests for TarXMLReader with duplicate entries (append scenario)."""

    @pytest.fixture
    def tar_with_duplicates(self, tmp_path: Path) -> Path:
        """Create a tar with duplicate entries (simulating append)."""
        tar_path = tmp_path / "duplicates.tar"

        with tarfile.open(tar_path, "w") as tar:
            # First version
            content1 = b"<SUBMISSION>version1</SUBMISSION>"
            info = tarfile.TarInfo(name="DRA000001/DRA000001.submission.xml")
            info.size = len(content1)
            tar.addfile(info, fileobj=io.BytesIO(content1))

        # Append second version
        with tarfile.open(tar_path, "a") as tar:
            content2 = b"<SUBMISSION>version2</SUBMISSION>"
            info = tarfile.TarInfo(name="DRA000001/DRA000001.submission.xml")
            info.size = len(content2)
            tar.addfile(info, fileobj=io.BytesIO(content2))

        return tar_path

    def test_later_entry_wins(self, tar_with_duplicates: Path, test_config: Config) -> None:
        """Test that later entry overwrites earlier one in index."""
        with run_logger(config=test_config), TarXMLReader(tar_with_duplicates) as reader:
            content = reader.read_xml("DRA000001", "submission")
            # Should get the second (later) version
            assert content == b"<SUBMISSION>version2</SUBMISSION>"
