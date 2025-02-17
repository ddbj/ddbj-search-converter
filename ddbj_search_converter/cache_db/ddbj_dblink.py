"""\
- dblink 以下の id relation 情報 から、BioProject/BioSample ID と SRA Accession ID の関連を取得するための module
    - /lustre9/open/shared_data/dblink 以下のファイルのこと
- bp_relation_ids や bs_relation_ids などの db を作成するために用いられる
"""
from typing import Dict, Literal, Optional, Set, Tuple

from ddbj_search_converter.config import Config
from ddbj_search_converter.schema import XrefType

AccessionType = Literal["bioproject", "biosample"]


SOURCE_FILE_TO_ID_RELATION: Dict[str, Tuple[XrefType, XrefType]] = {
    "assembly_genome-bp/assembly_genome2bp.tsv": ("insdc-assembly", "bioproject"),
    "assembly_genome-bs/assembly_genome2bs.tsv": ("insdc-assembly", "biosample"),
    "bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv": ("bioproject", "bioproject"),
    "bioproject-biosample/bioproject2biosample.tsv": ("bioproject", "biosample"),
    "biosample-bioproject/biosample2bioproject.tsv": ("biosample", "bioproject"),
    "gea-bioproject/gea2bioproject.tsv": ("gea", "bioproject"),
    "gea-biosample/gea2biosample.tsv": ("gea", "biosample"),
    "insdc_master-bioproject/insdc_master2bioproject.tsv": ("insdc-master", "bioproject"),
    "insdc_master-biosample/insdc_master2biosample.tsv": ("insdc-master", "biosample"),
    "insdc-bioproject/insdc2bioproject.tsv": ("insdc", "bioproject"),
    "insdc-biosample/insdc2biosample.tsv": ("insdc", "biosample"),
    "mtb2bp/mtb_id_bioproject.tsv": ("metabobank", "bioproject"),
    "mtb2bs/mtb_id_biosample.tsv": ("metabobank", "biosample"),
    "ncbi_biosample_bioproject/ncbi_biosample_bioproject.tsv": ("biosample", "bioproject"),
    "taxonomy_biosample/trace_biosample_taxon2bs.tsv": ("taxonomy", "biosample"),
}


# Key: BioProject/BioSample ID, Value: Set of DBLink IDs
DBLINK_FILES_CACHE: Optional[Dict[str, Set[str]]] = None


def load_dblink_files(config: Config, accession_type: AccessionType) -> None:
    global DBLINK_FILES_CACHE  # pylint: disable=global-statement
    if DBLINK_FILES_CACHE is None:
        DBLINK_FILES_CACHE = {}

    for filename, relation in SOURCE_FILE_TO_ID_RELATION.items():
        if accession_type not in relation:
            continue
        source_file = config.dblink_base_path.joinpath(filename)
        with source_file.open("r", encoding="utf-8") as f:
            for line in f:
                id0, id1 = line.strip().split("\t")
                if relation.index(accession_type) == 0:
                    bp_bs_id, relation_id = id0, id1
                else:
                    bp_bs_id, relation_id = id1, id0
                if bp_bs_id not in DBLINK_FILES_CACHE:
                    DBLINK_FILES_CACHE[bp_bs_id] = set()
                DBLINK_FILES_CACHE[bp_bs_id].add(relation_id)


def get_relation_ids(bp_bs_id: str) -> Set[str]:
    if DBLINK_FILES_CACHE is None:
        raise Exception("DBLink Files Cache is not loaded.")

    return DBLINK_FILES_CACHE.get(bp_bs_id, set())


def get_cache() -> Dict[str, Set[str]]:
    if DBLINK_FILES_CACHE is None:
        raise Exception("DBLink Files Cache is not loaded.")

    return DBLINK_FILES_CACHE
