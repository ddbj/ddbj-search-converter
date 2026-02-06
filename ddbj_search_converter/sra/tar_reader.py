"""\
TarXMLReader: Read XML files from tar archives with in-memory index.

The index is built by iterating through all tar members on first access.
For duplicate entries (same filename), the last occurrence wins.
This supports append-based daily updates where newer entries are appended.
"""
import tarfile
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from ddbj_search_converter.config import (DRA_TAR_FILE_NAME,
                                          NCBI_SRA_TAR_FILE_NAME, Config)
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.sra.paths import get_sra_tar_dir

# SRA XML types
SraXmlType = Literal["submission", "study", "experiment", "run", "sample", "analysis"]
SRA_XML_TYPES: List[SraXmlType] = ["submission", "study", "experiment", "run", "sample", "analysis"]


class TarXMLReader:
    """Read XML files from a tar archive using an in-memory index."""

    def __init__(self, tar_path: Path, index_data: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Args:
            tar_path: tar ファイルのパス
            index_data: 事前にロードしたインデックスデータ（キャッシュから）
        """
        self.tar_path = tar_path
        self._tar: Optional[tarfile.TarFile] = None
        self._members: Optional[Dict[str, tarfile.TarInfo]] = None
        self._index_data = index_data

    def _ensure_open(self) -> None:
        if self._tar is None:
            log_info(f"opening tar: {self.tar_path}")
            # pylint: disable=consider-using-with
            self._tar = tarfile.open(self.tar_path, "r")

    def _build_index(self) -> None:
        """Build the in-memory index. Later entries overwrite earlier ones."""
        if self._members is not None:
            return

        # キャッシュデータがあればそれを使用
        if self._index_data is not None:
            log_info("using cached tar index")
            self._members = {}
            for name, data in self._index_data.items():
                info = tarfile.TarInfo()
                info.name = data["name"]
                info.offset = data["offset"]
                info.offset_data = data["offset_data"]
                info.size = data["size"]
                self._members[name] = info
            log_info(f"tar index restored from cache: {len(self._members)} entries")
            return

        # キャッシュがなければ tar をスキャン
        self._ensure_open()
        assert self._tar is not None

        log_info("building tar index (this may take a few minutes)...")
        # Later entries overwrite earlier ones (append-based update support)
        self._members = {}
        for member in self._tar.getmembers():
            self._members[member.name] = member

        log_info(f"tar index built: {len(self._members)} entries")

    def get_index_for_cache(self) -> Dict[str, tarfile.TarInfo]:
        """キャッシュ保存用のインデックスを取得する。"""
        self._build_index()
        assert self._members is not None
        return self._members

    def get_submission_offsets(self, submissions: List[str]) -> Dict[str, int]:
        """指定された submission の最小 offset を取得する。

        各 submission の XML ファイル群のうち最小の offset を返す。
        tar 内の物理的な位置順でソートするために使用。
        """
        self._build_index()
        assert self._members is not None

        result: Dict[str, int] = {}
        for sub in submissions:
            min_offset = float("inf")
            for xml_type in SRA_XML_TYPES:
                key = f"{sub}/{sub}.{xml_type}.xml"
                member = self._members.get(key)
                if member is not None and member.offset < min_offset:
                    min_offset = member.offset
            if min_offset < float("inf"):
                result[sub] = int(min_offset)
        return result

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


def get_ncbi_tar_path(config: Config) -> Path:
    """Get the path to the NCBI SRA Metadata tar."""
    return get_sra_tar_dir(config).joinpath(NCBI_SRA_TAR_FILE_NAME)


def get_dra_tar_path(config: Config) -> Path:
    """Get the path to the DRA Metadata tar."""
    return get_sra_tar_dir(config).joinpath(DRA_TAR_FILE_NAME)


def get_ncbi_tar_reader(config: Config) -> TarXMLReader:
    """Get a TarXMLReader for the NCBI SRA Metadata tar."""
    return TarXMLReader(get_ncbi_tar_path(config))


def get_dra_tar_reader(config: Config) -> TarXMLReader:
    """Get a TarXMLReader for the DRA Metadata tar."""
    return TarXMLReader(get_dra_tar_path(config))
