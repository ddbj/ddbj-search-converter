"""
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
from typing import List, Tuple

from ddbj_search_converter.config import DBLINK_BASE_PATH, get_config
from ddbj_search_converter.dblink.db import AccessionType, export_relations
from ddbj_search_converter.logging.logger import log_info, run_logger

# Export target relations
EXPORT_RELATIONS: List[Tuple[AccessionType, AccessionType, str]] = [
    ("insdc-assembly", "bioproject", "assembly_genome-bp/assembly_genome2bp.tsv"),
    ("insdc-assembly", "biosample", "assembly_genome-bs/assembly_genome2bs.tsv"),
    ("insdc-assembly", "insdc-master", "assembly_genome-insdc/assembly_genome2insdc.tsv"),
    ("insdc-master", "bioproject", "insdc_master-bioproject/insdc_master2bioproject.tsv"),
    ("insdc-master", "biosample", "insdc_master-biosample/insdc_master2biosample.tsv"),
]


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        for type_a, type_b, rel_path in EXPORT_RELATIONS:
            output_path = DBLINK_BASE_PATH.joinpath(rel_path)
            log_info(f"exporting {type_a} <-> {type_b}", file=str(output_path))
            export_relations(config, output_path, type_a=type_a, type_b=type_b)


if __name__ == "__main__":
    main()
