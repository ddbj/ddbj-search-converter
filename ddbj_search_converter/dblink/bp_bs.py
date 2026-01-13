"""
Inputs:
- biosample_set.xml.gz
    - BioSample master XML
- SRA.accession.tab
    - SRA accession table.
    - Visibility とかは参照するか？
- 自極分 SRA accession.tab
    - accession tab の
    - Visibility
- bpbs_asm
    - Assembly-derived biosample <-> bioproject relations.
- bpbs_preserved.tsv
    - ~/const/dblink/bpbs_preserved.tsv
    - Preserved biosample <-> bioproject relations.
- blacklist
    - relation に乗っけないという判断をするか否か？

Outputs:
- /lustre9/open/shared_data/dblink/biosample-bioproject/biosample2bioproject.tsv
    - Mapping: BioSample ID -> BioProject ID
- /lustre9/open/shared_data/dblink/bioproject-biosample/bioproject2biosample.tsv
    - Mapping: BioProject ID -> BioSample ID
"""

import gzip
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable, Set

from ddbj_search_converter.config import DBLINK_BASE_PATH, get_config
from ddbj_search_converter.logging.logger import log_info, run_logger

BIOSAMPLE_XML = Path("/lustre9/open/shared_data/biosample/biosample_set.xml.gz")
SRA_TAB = Path("SRA.accession.tab")
BSBP_ASM = Path("bsbp_asm")
BSBP_PRESERVED = Path("bsbp_preserved.tsv")

OUT_BS_TO_BP = DBLINK_BASE_PATH.joinpath(
    "biosample-bioproject/biosample2bioproject.tsv"
)
OUT_BP_TO_BS = DBLINK_BASE_PATH.joinpath(
    "bioproject-biosample/bioproject2biosample.tsv"
)


def write_sorted_lines(output_file: Path, lines: Iterable[str]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        for line in sorted(lines):
            f.write(line + "\n")

    log_info(f"written {output_file}", file=output_file)


def process_biosample_xml(bs_to_bp: Set[str]) -> None:
    log_info("processing biosample_set.xml.gz")

    current_bs: str | None = None

    with gzip.open(BIOSAMPLE_XML, "rt", encoding="utf-8") as f:
        for event, elem in ET.iterparse(f, events=("start", "end")):
            tag = elem.tag.split("}")[-1]

            if event == "start" and tag == "BioSample":
                current_bs = elem.attrib.get("accession")

            elif event == "end" and tag == "BioSample":
                current_bs = None
                elem.clear()

            elif current_bs and event == "end":
                if tag == "Link" and elem.attrib.get("target") == "bioproject":
                    bp = elem.attrib.get("label") or (elem.text or "").strip()
                    if bp.startswith("PRJ"):
                        bs_to_bp.add(f"{current_bs}\t{bp}")

                elif tag == "Attribute" and elem.attrib.get("attribute_name") == "bioproject_accession":
                    bp = (elem.text or "").strip()
                    if bp.startswith("PRJ"):
                        bs_to_bp.add(f"{current_bs}\t{bp}")

                elem.clear()


def process_sra_tab(bs_to_bp: Set[str]) -> None:
    log_info("processing SRA.accession.tab")

    with SRA_TAB.open("r", encoding="utf-8") as f:
        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 19:
                continue

            bs = cols[17]
            bp = cols[18]

            if bs.startswith("SAM") and bp.startswith("PRJ"):
                bs_to_bp.add(f"{bs}\t{bp}")


def process_simple_bsbp_file(path: Path, bs_to_bp: Set[str]) -> None:
    log_info(f"processing {path}", file=path)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            bs_to_bp.add(line.rstrip("\n"))


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bs_to_bp: Set[str] = set()

        process_biosample_xml(bs_to_bp)
        process_sra_tab(bs_to_bp)
        process_simple_bsbp_file(BSBP_PRESERVED, bs_to_bp)
        process_simple_bsbp_file(BSBP_ASM, bs_to_bp)

        log_info("writing output files")

        write_sorted_lines(OUT_BS_TO_BP, bs_to_bp)
        write_sorted_lines(
            OUT_BP_TO_BS,
            {f"{bp}\t{bs}" for bs, bp in (line.split("\t") for line in bs_to_bp)},
        )


if __name__ == "__main__":
    main()
