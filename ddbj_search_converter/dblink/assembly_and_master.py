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

Outputs:
- /lustre9/open/shared_data/dblink/assembly_genome-insdc/assembly_genome2insdc.tsv
    - Mapping: Assembly genome ID → INSDC master ID
- /lustre9/open/shared_data/dblink/assembly_genome-bp/assembly_genome2bp.tsv
    - Mapping: Assembly genome ID → BioProject ID
- /lustre9/open/shared_data/dblink/assembly_genome-bs/assembly_genome2bs.tsv
    - Mapping: Assembly genome ID → BioSample ID
- /lustre9/open/shared_data/dblink/insdc_master-bioproject/insdc_master2bioproject.tsv
    - Mapping: INSDC master ID → BioProject ID
- /lustre9/open/shared_data/dblink/insdc_master-biosample/insdc_master2biosample.tsv
    - Mapping: INSDC master ID → BioSample ID
"""
from pathlib import Path
from typing import Iterable, Set

import httpx

from ddbj_search_converter.config import (DBLINK_BASE_PATH, TRAD_BASE_PATH,
                                          Config, get_config)
from ddbj_search_converter.logging.logger import init_logger, log

ASSEMBLY_SUMMARY_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt"
ASSEMBLY_SUMMARY_FILE_NAME = "assembly_summary_genbank.txt"

TRAD_FILES = [
    TRAD_BASE_PATH.joinpath("wgs/WGS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tls/TLS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tsa/TSA_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/wgs/TPA_WGS_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/tsa/TPA_TSA_ORGANISM_LIST.txt"),
    TRAD_BASE_PATH.joinpath("tpa/tls/TPA_TLS_ORGANISM_LIST.txt"),
]

ASSEMBLY_TO_INSDC_OUTPUT_FILE = DBLINK_BASE_PATH.joinpath("assembly_genome-insdc/assembly_genome2insdc.tsv")
ASSEMBLY_TO_BP_OUTPUT_FILE = DBLINK_BASE_PATH.joinpath("assembly_genome-bp/assembly_genome2bp.tsv")
ASSEMBLY_TO_BS_OUTPUT_FILE = DBLINK_BASE_PATH.joinpath("assembly_genome-bs/assembly_genome2bs.tsv")
INSDC_MASTER_TO_BP_OUTPUT_FILE = DBLINK_BASE_PATH.joinpath("insdc_master-bioproject/insdc_master2bioproject.tsv")
INSDC_MASTER_TO_BS_OUTPUT_FILE = DBLINK_BASE_PATH.joinpath("insdc_master-biosample/insdc_master2biosample.tsv")


def normalize_insdc_master_id(raw_master_id: str) -> str:
    """
    Normalize INSDC master ID.

    Perl equivalent:
      split('-', $set[3])[0]
      s/[1-9]/0/g
    """
    base_id = raw_master_id.split("-", 1)[0]

    return "".join("0" if char.isdigit() else char for char in base_id)


def write_sorted_lines(output_file: Path, lines: Iterable[str]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as file:
        for line in sorted(lines):
            file.write(line + "\n")

    log(event="progress", message=f"written {output_file}", target={"file": str(output_file)})


def download_assembly_summary_file(config: Config) -> Path:
    assembly_summary_file = config.work_dir.joinpath(ASSEMBLY_SUMMARY_FILE_NAME)

    log(
        event="progress",
        message="downloading assembly_summary_genbank.txt",
        target={"file": str(assembly_summary_file)},
        extra={"url": ASSEMBLY_SUMMARY_URL}
    )

    assembly_summary_file.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(follow_redirects=True, timeout=60.0) as client:
        with client.stream("GET", ASSEMBLY_SUMMARY_URL) as response:
            response.raise_for_status()

            with assembly_summary_file.open("wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

    log(
        event="progress",
        message="assembly summary file downloaded",
        target={"file": str(assembly_summary_file)},
        extra={"url": ASSEMBLY_SUMMARY_URL}
    )

    return assembly_summary_file


def process_assembly_summary_file(
    config: Config,
    assembly_to_bp: Set[str],
    assembly_to_bs: Set[str],
    assembly_to_insdc: Set[str],
    master_to_bp: Set[str],
    master_to_bs: Set[str],
) -> None:
    assembly_summary_file = download_assembly_summary_file(config)

    log(event="progress", message="processing assembly_summary_genbank.txt", target={"file": str(assembly_summary_file)})

    relations = [
        ("asm", "bp", assembly_to_bp),
        ("asm", "bs", assembly_to_bs),
        ("asm", "master", assembly_to_insdc),
        ("master", "bp", master_to_bp),
        ("master", "bs", master_to_bs),
    ]

    with assembly_summary_file.open("r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
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
                    target_set.add(f"{l}\t{r}")


def process_trad_files(
    master_to_bp: Set[str],
    master_to_bs: Set[str],
) -> None:
    log(event="progress", message="processing trad organism list files")

    for path in TRAD_FILES:
        log(event="progress", message=f"reading {path}", target={"file": str(path)})

        with path.open("w", encoding="utf-8") as f:
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
                    master_to_bp.add(f"{master}\t{bp}")
                if bs:
                    master_to_bs.add(f"{master}\t{bs}")


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
        assembly_to_bp: Set[str] = set()
        assembly_to_bs: Set[str] = set()
        assembly_to_insdc: Set[str] = set()
        master_to_bp: Set[str] = set()
        master_to_bs: Set[str] = set()

        process_assembly_summary_file(config, assembly_to_bp, assembly_to_bs, assembly_to_insdc, master_to_bp, master_to_bs,)
        process_trad_files(master_to_bp, master_to_bs)

        log(event="progress", message="writing output files")

        write_sorted_lines(ASSEMBLY_TO_BP_OUTPUT_FILE, assembly_to_bp)
        write_sorted_lines(ASSEMBLY_TO_BS_OUTPUT_FILE, assembly_to_bs)
        write_sorted_lines(ASSEMBLY_TO_INSDC_OUTPUT_FILE, assembly_to_insdc)
        write_sorted_lines(INSDC_MASTER_TO_BP_OUTPUT_FILE, master_to_bp)
        write_sorted_lines(INSDC_MASTER_TO_BS_OUTPUT_FILE, master_to_bs)

        log(event="end", message="relation generation completed")

    except Exception as e:
        log(event="failed", error=e)
        raise e


if __name__ == "__main__":
    main()
