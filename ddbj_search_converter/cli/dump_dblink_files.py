"""
DBLink DB から TSV ファイルを出力する CLI。

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
- /lustre9/open/shared_data/dblink/biosample-bioproject/biosample2bioproject.tsv
    - Mapping: BioSample ID -> BioProject ID
- /lustre9/open/shared_data/dblink/bioproject-biosample/bioproject2biosample.tsv
    - Mapping: BioProject ID -> BioSample ID
- /lustre9/open/shared_data/dblink/bioproject-umbrella/bioproject2umbrella.tsv
    - Mapping: BioProject ID (primary) -> BioProject ID (umbrella)
- /lustre9/open/shared_data/dblink/gea-bioproject/gea2bioproject.tsv
    - Mapping: GEA ID -> BioProject ID
- /lustre9/open/shared_data/dblink/gea-biosample/gea2biosample.tsv
    - Mapping: GEA ID -> BioSample ID
- /lustre9/open/shared_data/dblink/metabobank-bioproject/metabobank2bioproject.tsv
    - Mapping: MetaboBank ID -> BioProject ID
- /lustre9/open/shared_data/dblink/metabobank-biosample/metabobank2biosample.tsv
    - Mapping: MetaboBank ID -> BioSample ID
"""
from typing import List, Tuple

from ddbj_search_converter.config import DBLINK_OUTPUT_PATH, get_config
from ddbj_search_converter.dblink.db import AccessionType, export_relations
from ddbj_search_converter.logging.logger import log_info, run_logger

# Export target relations
EXPORT_RELATIONS: List[Tuple[AccessionType, AccessionType, str]] = [
    ("insdc-assembly", "bioproject", "assembly_genome-bp/assembly_genome2bp.tsv"),
    ("insdc-assembly", "biosample", "assembly_genome-bs/assembly_genome2bs.tsv"),
    ("insdc-assembly", "insdc-master", "assembly_genome-insdc/assembly_genome2insdc.tsv"),
    ("insdc-master", "bioproject", "insdc_master-bioproject/insdc_master2bioproject.tsv"),
    ("insdc-master", "biosample", "insdc_master-biosample/insdc_master2biosample.tsv"),
    ("biosample", "bioproject", "biosample-bioproject/biosample2bioproject.tsv"),
    ("bioproject", "biosample", "bioproject-biosample/bioproject2biosample.tsv"),
    ("bioproject", "umbrella-bioproject", "bioproject-umbrella/bioproject2umbrella.tsv"),
    ("gea", "bioproject", "gea-bioproject/gea2bioproject.tsv"),
    ("gea", "biosample", "gea-biosample/gea2biosample.tsv"),
    ("metabobank", "bioproject", "metabobank-bioproject/metabobank2bioproject.tsv"),
    ("metabobank", "biosample", "metabobank-biosample/metabobank2biosample.tsv"),
]


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        for type_a, type_b, rel_path in EXPORT_RELATIONS:
            output_path = DBLINK_OUTPUT_PATH.joinpath(rel_path)
            log_info(f"exporting {type_a} <-> {type_b}", file=str(output_path))
            export_relations(config, output_path, type_a=type_a, type_b=type_b)


if __name__ == "__main__":
    main()
