"""
Inputs:
- assembly_summary_genbank.txt
    - Assembly summary file from GenBank.
    - Used to derive:
        - assembly ↔ BioProject
        - assembly ↔ BioSample
        - assembly ↔ INSDC master
        - INSDC master ↔ BioProject(assembly-derived)
        - INSDC master ↔ BioSample(assembly-derived)
- /lustre9/open/shared_data/trad/wgs/WGS_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tls/TLS_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tsa/TSA_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tpa/tls/TPA_TLS_ORGANISM_LIST.txt
    - Bulk sequence organism list files.
    - Used to derive:
        - INSDC master ↔ BioProject
        - INSDC master ↔ BioSample
"""
from typing import Iterable, Set, Tuple

import httpx

from ddbj_search_converter.config import TRAD_BASE_PATH, Config, get_config
from ddbj_search_converter.dblink.db import (AccessionType, Relation,
                                             bulk_insert_relations,
                                             create_connection)
from ddbj_search_converter.logging.logger import init_logger, log

ASSEMBLY_SUMMARY_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt"

TRAD_FILES = [
    TRAD_BASE_PATH.joinpath("wgs/WGS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tls/TLS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tsa/TSA_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/wgs/TPA_WGS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/tsa/TPA_TSA_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/tls/TPA_TLS_ORGANISM_LIST.txt"),
]

IdPairs = Set[Tuple[str, str]]


def normalize_insdc_master_id(raw_master_id: str) -> str:
    """
    Normalize INSDC master ID.

    Perl equivalent:
      split('-', $set[3])[0]
      s/[1-9]/0/g
    """
    base_id = raw_master_id.split("-", 1)[0]

    return "".join("0" if char.isdigit() else char for char in base_id)


def process_assembly_summary_file(
    assembly_to_bp: IdPairs,
    assembly_to_bs: IdPairs,
    assembly_to_insdc: IdPairs,
    master_to_bp: IdPairs,
    master_to_bs: IdPairs,
) -> None:
    log(event="progress", message="streaming assembly_summary_genbank.txt", extra={"url": ASSEMBLY_SUMMARY_URL})

    relations = [
        ("asm", "bp", assembly_to_bp),
        ("asm", "bs", assembly_to_bs),
        ("asm", "master", assembly_to_insdc),
        ("master", "bp", master_to_bp),
        ("master", "bs", master_to_bs),
    ]

    with httpx.Client(follow_redirects=True, timeout=60.0) as client:
        with client.stream("GET", ASSEMBLY_SUMMARY_URL) as response:
            response.raise_for_status()

            for line in response.iter_lines():
                if not line or line.startswith("#"):
                    continue

                cols = line.rstrip("\r\n").split("\t")
                if len(cols) < 4:
                    continue

                values = {
                    "asm": cols[0].split(".", 1)[0],
                    "bp": cols[1],
                    "bs": cols[2],
                    "master": cols[3].split(".", 1)[0],
                }

                for left, right, target_set in relations:
                    l = values[left]
                    r = values[right]
                    if l != "na" and r != "na":
                        target_set.add((l, r))


def process_trad_files(
    master_to_bp: IdPairs,
    master_to_bs: IdPairs,
) -> None:
    log(event="progress", message="processing trad organism list files")

    for path in TRAD_FILES:
        log(event="progress", message=f"processing file: {path}", extra={"file_path": str(path)})

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.startswith((" ", "\t", "-")):
                    continue

                cols = line.rstrip("\r\n").split("\t")
                if len(cols) < 11:
                    continue

                master = normalize_insdc_master_id(cols[3])
                bp = cols[9]
                bs = cols[10]

                if bp:
                    master_to_bp.add((master, bp))
                if bs:
                    master_to_bs.add((master, bs))


def load_to_db(
    config: Config,
    lines: IdPairs,
    type_src: AccessionType,
    type_dst: AccessionType,
) -> None:
    def line_generator() -> Iterable[Relation]:
        for src_id, dst_id in lines:
            yield (type_src, src_id, type_dst, dst_id)

    with create_connection(config) as conn:
        bulk_insert_relations(conn, line_generator())


def main() -> None:
    config = get_config()
    init_logger(
        run_name="create_dblink_assembly_and_master_relations",
        config=config,
    )
    log(
        event="start",
        message="starting relation generation between assembly/genome and INSDC master, BioProject, BioSample",
        extra={"config": config.model_dump()}
    )

    try:
        assembly_to_bp: IdPairs = set()
        assembly_to_bs: IdPairs = set()
        assembly_to_insdc: IdPairs = set()
        master_to_bp: IdPairs = set()
        master_to_bs: IdPairs = set()

        process_assembly_summary_file(assembly_to_bp, assembly_to_bs, assembly_to_insdc, master_to_bp, master_to_bs,)
        process_trad_files(master_to_bp, master_to_bs)

        log(event="progress", message="loading relations into dblink database")

        load_to_db(config, assembly_to_bp, "insdc-assembly", "bioproject")
        load_to_db(config, assembly_to_bs, "insdc-assembly", "biosample")
        load_to_db(config, assembly_to_insdc, "insdc-assembly", "insdc-master")
        load_to_db(config, master_to_bp, "insdc-master", "bioproject")
        load_to_db(config, master_to_bs, "insdc-master", "biosample")

        log(event="end", message="finished relation generation successfully")
    except Exception as e:
        log(event="failed", error=e)
        raise e


if __name__ == "__main__":
    main()
