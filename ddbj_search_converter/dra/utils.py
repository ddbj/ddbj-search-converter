import re
import tarfile
from pathlib import Path
from typing import Any, Dict, Generator, List, Literal, Optional, Set, Tuple

import xmltodict
from pydantic import BaseModel

from ddbj_search_converter.config import Config

AccessionType = Literal["SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"]
ACCESSION_TYPE: List[AccessionType] = ["SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"]


class SraMetadata(BaseModel):
    accession: str
    submission: str
    status: Literal["live", "suppressed", "unpublished", "withdrawn"]
    updated: str  # YYYY-MM-DDTHH:MM:SSZ
    published: Optional[str]  # YYYY-MM-DDTHH:MM:SSZ
    received: str  # YYYY-MM-DDTHH:MM:SSZ
    type: AccessionType
    center: Optional[str]
    visibility: Literal["public", "controlled_access"]
    alias: Optional[str]
    experiment: Optional[str]
    sample: Optional[str]
    study: Optional[str]
    loaded: Optional[str]
    spots: Optional[str]
    bases: Optional[str]
    md5sum: Optional[str]
    biosample: Optional[str]
    bioproject: Optional[str]
    replaced_by: Optional[str]


def line_to_sra_metadata(line: str) -> SraMetadata:
    # https://github.com/linsalrob/SRA_Metadata/blob/master/README.md
    fields = line.strip().split("\t")
    return SraMetadata(
        accession=fields[0],
        submission=fields[1],
        status=fields[2] if fields[2] != "-" else None,  # type: ignore
        updated=fields[3],
        published=fields[4] if fields[4] != "-" else None,
        received=fields[5],
        type=fields[6] if fields[6] != "-" else None,  # type: ignore
        center=fields[7] if fields[7] != "-" else None,
        visibility=fields[8] if fields[8] != "-" else None,  # type: ignore
        alias=fields[9] if fields[9] != "-" else None,
        experiment=fields[10] if fields[10] != "-" else None,
        sample=fields[11] if fields[11] != "-" else None,
        study=fields[12] if fields[12] != "-" else None,
        loaded=fields[13] if fields[13] != "-" else None,
        spots=fields[14] if fields[14] != "-" else None,
        bases=fields[15] if fields[15] != "-" else None,
        md5sum=fields[16] if fields[16] != "-" else None,
        biosample=fields[17] if fields[17] != "-" else None,
        bioproject=fields[18] if fields[18] != "-" else None,
        replaced_by=fields[19] if fields[19] != "-" else None,
    )


def generate_xml_file_path(sra_metadata: SraMetadata) -> str:
    return f"fastq/{sra_metadata.submission[:6]}/{sra_metadata.submission}/{sra_metadata.submission}.{sra_metadata.type.lower()}.xml"


def generate_experiment_dir_path(sra_metadata: SraMetadata, experiment: str) -> str:
    return f"fastq/{sra_metadata.submission[:6]}/{sra_metadata.submission}/{experiment}"


def generate_sra_file_path(sra_metadata: SraMetadata, experiment: str) -> str:
    return f"sra/ByExp/sra/DRX/{experiment[:6]}/{experiment}/{sra_metadata.accession}/{sra_metadata.accession}.sra"


class TarXmlStore:
    """
    A utility class to read XML files from a tar.gz archive.
    Dir structure inside the tar.gz: <SUBMISSION>/<SUBMISSION>.<kind>.xml
    e.g., DRA000001/DRA000001.experiment.xml
    """
    _name_re = re.compile(r"^(?P<submission>[^/]+)/(?P=submission)\.(?P<kind>experiment|sample|run|study|submission)\.xml$",
                          re.IGNORECASE)

    def __init__(self, tar_path: Path) -> None:
        self._tar = tarfile.open(tar_path, "r:*")
        self._index: Dict[Tuple[str, str], tarfile.TarInfo] = {}  # (submission, kind) -> TarInfo
        self._build_index()

    def _build_index(self) -> None:
        for member in self._tar.getmembers():
            if not member.isfile():
                continue
            match = self._name_re.match(member.name)
            if match:
                submission = match.group("submission")
                kind = match.group("kind").lower()
                self._index[(submission, kind)] = member

    def has(self, submission: str, kind: AccessionType) -> bool:
        return (submission, kind.lower()) in self._index

    def read_xml_bytes(self, submission: str, kind: AccessionType) -> bytes:
        ti = self._index.get((submission, kind.lower()))
        if ti is None:
            raise FileNotFoundError(f"XML not found: {submission}.{kind}.xml")
        f = self._tar.extractfile(ti)
        assert f is not None
        return f.read()

    def close(self) -> None:
        self._tar.close()


def iterate_sra_metadata(
    config: Config,
    accession_type: Optional[AccessionType] = None,
    exist_keys: Optional[Set[Tuple[str, AccessionType]]] = None,
    not_exist_keys: Optional[Set[Tuple[str, AccessionType]]] = None,
    tar_store: Optional[TarXmlStore] = None,
) -> Generator[SraMetadata, None, None]:
    if tar_store is None:
        tar_store = TarXmlStore(config.dra_xml_tar_file_path)
    if exist_keys is None:
        exist_keys = set()
    if not_exist_keys is None:
        not_exist_keys = set()

    with config.sra_accessions_tab_file_path.open("r", encoding="utf-8") as f:
        next(f, None)  # skip header
        for line in f:
            sra_metadata = line_to_sra_metadata(line)

            if accession_type is not None and sra_metadata.type != accession_type:
                continue

            # Skip if the ReplacedBy field is not None
            if sra_metadata.replaced_by is not None:
                continue

            # Skip if the status field is "unpublished"
            if sra_metadata.status == "unpublished":
                continue

            key = (sra_metadata.submission, sra_metadata.type)
            if key in exist_keys:
                yield sra_metadata
                continue
            if key in not_exist_keys:
                continue

            if tar_store.has(sra_metadata.submission, sra_metadata.type):
                exist_keys.add(key)
                yield sra_metadata
            else:
                not_exist_keys.add(key)
                continue


def load_xml_metadata_from_tar(
    tar_store: TarXmlStore,
    sra_metadata: SraMetadata,
) -> Dict[str, Any]:
    xml_bytes = tar_store.read_xml_bytes(sra_metadata.submission, sra_metadata.type)
    xml_metadata = xmltodict.parse(xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False)

    return xml_metadata
