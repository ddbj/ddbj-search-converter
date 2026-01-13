"""
Inputs:
- assembly_summary_genbank.txt
    - Assembly summary file from GenBank.
    - Used to derive:
        - assembly <-> BioProject
        - assembly <-> BioSample
        - assembly <-> INSDC master
        - INSDC master <-> BioProject(assembly-derived)
        - INSDC master <-> BioSample(assembly-derived)
- /lustre9/open/shared_data/trad/wgs/WGS_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tls/TLS_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tsa/TSA_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt
- /lustre9/open/shared_data/trad/tpa/tls/TPA_TLS_ORGANISM_LIST.txt
    - Bulk sequence organism list files.
    - Used to derive:
        - INSDC master <-> BioProject
        - INSDC master <-> BioSample
Outputs:
- /lustre9/open/shared_data/dblink/assembly_genome-insdc/assembly_genome2insdc.tsv
    - Mapping: Assembly genome ID -> INSDC master ID
- /lustre9/open/shared_data/dblink/assembly_genome-bp/assembly_genome2bp.tsv
    - Mapping: Assembly genome ID -> BioProject ID
- /lustre9/open/shared_data/dblink/assembly_genome-bs/assembly_genome2bs.tsv
    - Mapping: Assembly genome ID -> BioSample ID
- /lustre9/open/shared_data/dblink/insdc_master-bioproject/insdc_master2bioproject.tsv
    - Mapping: INSDC master ID -> BioProject ID
- /lustre9/open/shared_data/dblink/insdc_master-biosample/insdc_master2biosample.tsv
    - Mapping: INSDC master ID -> BioSample ID
"""
from typing import Iterable, Set, Tuple

import httpx

from ddbj_search_converter.config import TRAD_BASE_PATH, Config, get_config
from ddbj_search_converter.dblink.db import (AccessionType, Relation,
                                             bulk_insert_relations)
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                   run_logger)

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
    log_info("streaming assembly_summary_genbank.txt", url=ASSEMBLY_SUMMARY_URL)

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
    log_info("processing trad organism list files")

    for path in TRAD_FILES:
        log_info(f"processing file: {path}", file=path)

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

    bulk_insert_relations(config, line_generator())


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        assembly_to_bp: IdPairs = set()
        assembly_to_bs: IdPairs = set()
        assembly_to_insdc: IdPairs = set()
        master_to_bp: IdPairs = set()
        master_to_bs: IdPairs = set()

        process_assembly_summary_file(
            assembly_to_bp,
            assembly_to_bs,
            assembly_to_insdc,
            master_to_bp,
            master_to_bs,
        )
        process_trad_files(master_to_bp, master_to_bs)

        log_info("loading relations into dblink database")

        load_to_db(config, assembly_to_bp, "insdc-assembly", "bioproject")
        load_to_db(config, assembly_to_bs, "insdc-assembly", "biosample")
        load_to_db(config, assembly_to_insdc, "insdc-assembly", "insdc-master")
        load_to_db(config, master_to_bp, "insdc-master", "bioproject")
        load_to_db(config, master_to_bs, "insdc-master", "biosample")


if __name__ == "__main__":
    main()
