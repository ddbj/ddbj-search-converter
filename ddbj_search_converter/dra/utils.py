from pathlib import Path
from typing import Generator, List, Literal, Optional, Set

from pydantic import BaseModel

from ddbj_search_converter.config import Config

AccessionType = Literal["SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"]
ACCESSION_TYPE: List[AccessionType] = ["SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"]


class SraMetadata(BaseModel):
    accession: str
    submission: str
    status: Literal["live", "suppressed", "unpublished", "withdrawn"]  # no withdrawn status in DRA
    updated: str  # YYYY-MM-DDTHH:MM:SSZ
    published: Optional[str]  # YYYY-MM-DDTHH:MM:SSZ
    received: str  # YYYY-MM-DDTHH:MM:SSZ
    type: AccessionType
    center: Optional[str]
    visibility: Literal["public"]
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


def iterate_sra_metadata(
    config: Config,
    accession_type: Optional[AccessionType] = None,
    exist_xml_files: Optional[Set[Path]] = None,
    not_exist_xml_files: Optional[Set[Path]] = None
) -> Generator[SraMetadata, None, None]:
    if exist_xml_files is None:
        exist_xml_files = set()
    if not_exist_xml_files is None:
        not_exist_xml_files = set()
    with config.sra_accessions_tab_file_path.open("r", encoding="utf-8") as f:
        next(f, None)  # skip header
        for line in f:
            if not line.startswith("D"):
                continue

            sra_metadata = line_to_sra_metadata(line)

            if sra_metadata.type != accession_type:
                continue

            # Skip if the ReplacedBy field is not None
            if sra_metadata.replaced_by is not None:
                continue

            # Skip if the status field is "unpublished"
            if sra_metadata.status == "unpublished":
                continue

            xml_file_path = config.dra_base_path.joinpath(generate_xml_file_path(sra_metadata))
            if xml_file_path in exist_xml_files:
                yield sra_metadata
            else:
                if xml_file_path in not_exist_xml_files:
                    # Skip if the XML file does not exist
                    continue

                if xml_file_path.exists():
                    exist_xml_files.add(xml_file_path)
                    yield sra_metadata
                else:
                    not_exist_xml_files.add(xml_file_path)
                    # Skip if the XML file does not exist
                    continue
