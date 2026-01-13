"""\
TarXMLReader: Read XML files from tar archives with in-memory index.

The index is built by iterating through all tar members on first access.
For duplicate entries (same filename), the last occurrence wins.
This supports append-based daily updates where newer entries are appended.
"""
import tarfile
from pathlib import Path
from typing import Dict, Literal, Optional

from ddbj_search_converter.config import (DRA_TAR_FILE_NAME,
                                          NCBI_SRA_TAR_FILE_NAME,
                                          SRA_TAR_DIR_NAME, Config)
from ddbj_search_converter.logging.logger import log_info

# SRA XML types
SraXmlType = Literal["submission", "study", "experiment", "run", "sample", "analysis"]


class TarXMLReader:
    """Read XML files from a tar archive using an in-memory index."""

    def __init__(self, tar_path: Path):
        self.tar_path = tar_path
        self._tar: Optional[tarfile.TarFile] = None
        self._members: Optional[Dict[str, tarfile.TarInfo]] = None

    def _ensure_open(self) -> None:
        if self._tar is None:
            log_info(f"Opening tar: {self.tar_path}")
            # pylint: disable=consider-using-with
            self._tar = tarfile.open(self.tar_path, "r")

    def _build_index(self) -> None:
        """Build the in-memory index. Later entries overwrite earlier ones."""
        if self._members is not None:
            return

        self._ensure_open()
        assert self._tar is not None

        log_info("Building tar index (this may take a few minutes)...")
        # Later entries overwrite earlier ones (append-based update support)
        self._members = {}
        for member in self._tar.getmembers():
            self._members[member.name] = member

        log_info(f"Tar index built: {len(self._members)} entries")

    @property
    def members(self) -> Dict[str, tarfile.TarInfo]:
        """Get the member index, building it if necessary."""
        self._build_index()
        assert self._members is not None
        return self._members

    def exists(self, submission: str, xml_type: SraXmlType) -> bool:
        """Check if an XML file exists in the tar."""
        key = f"{submission}/{submission}.{xml_type}.xml"
        return key in self.members

    def read_xml(self, submission: str, xml_type: SraXmlType) -> Optional[bytes]:
        """Read an XML file from the tar archive."""
        key = f"{submission}/{submission}.{xml_type}.xml"
        member = self.members.get(key)
        if member is None:
            return None

        self._ensure_open()
        assert self._tar is not None

        extracted = self._tar.extractfile(member)
        if extracted is None:
            return None
        return extracted.read()

    def get_member_count(self) -> int:
        """Get the number of entries in the tar."""
        return len(self.members)

    def close(self) -> None:
        """Close the tar file."""
        if self._tar is not None:
            self._tar.close()
            self._tar = None

    def __enter__(self) -> "TarXMLReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        self.close()


def get_ncbi_tar_reader(config: Config) -> TarXMLReader:
    """Get a TarXMLReader for the NCBI SRA Metadata tar."""
    tar_path = config.const_dir.joinpath(SRA_TAR_DIR_NAME, NCBI_SRA_TAR_FILE_NAME)
    return TarXMLReader(tar_path)


def get_dra_tar_reader(config: Config) -> TarXMLReader:
    """Get a TarXMLReader for the DRA Metadata tar."""
    tar_path = config.const_dir.joinpath(SRA_TAR_DIR_NAME, DRA_TAR_FILE_NAME)
    return TarXMLReader(tar_path)
