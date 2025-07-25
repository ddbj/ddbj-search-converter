"""\
- dblink 以下の id relation 情報 から、BioProject/BioSample ID と SRA Accession ID の関連を取得するための module
    - /lustre9/open/shared_data/dblink 以下のファイルのこと
- relation ids (dbXrefs) の bulk insert のために使われる
"""
from typing import Dict, Set, Tuple

from ddbj_search_converter.config import AccessionType, Config
from ddbj_search_converter.schema import XrefType

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


def load_dblink_files(config: Config, accession_type: AccessionType) -> Dict[str, Set[str]]:
    """\
    - Return:
        - Key: BioProject/BioSample ID
        - Value: Set of DBLink IDs
    """

    id_to_relation_ids: Dict[str, Set[str]] = {}

    for filename, relation in SOURCE_FILE_TO_ID_RELATION.items():
        if accession_type not in relation:
            continue
        source_file = config.dblink_base_path.joinpath(filename)
        try:
            with source_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line == "":
                        continue
                    id0, id1 = line.split("\t")
                    if relation.index(accession_type) == 0:
                        bp_bs_id, relation_id = id0, id1
                    else:
                        bp_bs_id, relation_id = id1, id0
                    if bp_bs_id not in id_to_relation_ids:
                        id_to_relation_ids[bp_bs_id] = set()
                    id_to_relation_ids[bp_bs_id].add(relation_id)
        except Exception as e:
            raise RuntimeError(f"Failed to read {source_file}: {e}") from e

    return id_to_relation_ids
