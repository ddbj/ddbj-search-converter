"""\
- 複数の DB から横断して data を取得するような getter をまとめる
"""
import re
from typing import Dict, List, Literal, Pattern

from sqlalchemy import select

from ddbj_search_converter.cache_db.ddbj_dblink import Base as DblinkBase
from ddbj_search_converter.cache_db.ddbj_dblink import \
    TableNames as DblinkTableNames
from ddbj_search_converter.cache_db.ddbj_dblink import \
    get_session as get_dblink_session
from ddbj_search_converter.cache_db.sra_accessions import \
    Base as SraAccessionsBase
from ddbj_search_converter.cache_db.sra_accessions import \
    TableNames as SraAccessionsTableNames
from ddbj_search_converter.cache_db.sra_accessions import \
    get_session as get_sra_accessions_session
from ddbj_search_converter.config import Config
from ddbj_search_converter.schema import Xref, XrefType

XREF_LIMIT = 10000  # xref の取得条件数 (for es limit)


AccessionType = Literal["bioproject", "biosample"]

DBLINK_TABLES: Dict[AccessionType, List[DblinkTableNames]] = {
    "bioproject": [
        "assembly_genome2bp",
        "bioproject2biosample",
        "bioproject_umbrella2bioproject",
        "biosample2bioproject",
        "gea2bioproject",
        "mtb_id_bioproject",
        "ncbi_biosample_bioproject",
    ],
    "biosample": [
        "assembly_genome2bs",
        "bioproject2biosample",
        "biosample2bioproject",
        "gea2biosample",
        "mtb_id_biosample",
        "ncbi_biosample_bioproject",
        "trace_biosample_taxon2bs"
    ],
}

SRA_ACCESSIONS_TABLES: Dict[AccessionType, List[SraAccessionsTableNames]] = {
    "bioproject": [
        "experiment_bioproject",
        "study_bioproject",
    ],
    "biosample": [
        "experiment_biosample",
        "experiment_sample",
        "run_biosample",
        "sample_biosample",
    ],
}

ID_PATTERN_MAP: Dict[XrefType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM"),
    "bioproject": re.compile(r"^PRJ"),
    "sra-experiment": re.compile(r"(S|D|E)RX"),
    "sra-run": re.compile(r"(S|D|E)RR"),
    "sra-sample": re.compile(r"(S|D|E)RS"),
    "sra-study": re.compile(r"(S|D|E)RP"),
    "gea": re.compile(r"^E-GEA"),
    "assemblies": re.compile(r"^GCA"),
    "metabobank": re.compile(r"^MTB"),
    "taxonomy": re.compile(r"^\d+"),
}

URL_TEMPLATE: Dict[XrefType, str] = {
    "biosample": "https://ddbj.nig.ac.jp/resource/biosample/{id}",
    "bioproject": "https://ddbj.nig.ac.jp/resource/bioproject/{id}",
    "sra-experiment": "https://ddbj.nig.ac.jp/resource/sra-experiment/{id}",
    "sra-run": "https://ddbj.nig.ac.jp/resource/sra-run/{id}",
    "sra-sample": "https://ddbj.nig.ac.jp/resource/sra-sample/{id}",
    "sra-study": "https://ddbj.nig.ac.jp/resource/sra-study/{id}",
    "gea": "https://ddbj.nig.ac.jp/public/ddbj_database/gea/experiment/E-GEAD-000/{id}/",
    "assemblies": "https://www.ncbi.nlm.nih.gov/datasets/genome/{id}/",
    "metabobank": "https://mb2.ddbj.nig.ac.jp/study/{id}.html",
    "taxonomy": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={id}",
}


def _to_xref(id_: str) -> Xref:
    for db_type, pattern in ID_PATTERN_MAP.items():
        if pattern.match(id_):
            return Xref(identifier=id_, type=db_type, url=URL_TEMPLATE[db_type].format(id=id_))

    # default は taxonomy を返す
    return Xref(identifier=id_, type="taxonomy", url=URL_TEMPLATE["taxonomy"].format(id=id_))


def get_xrefs(config: Config, id_: str, type_: AccessionType) -> List[Xref]:
    related_ids = set()

    with get_dblink_session(config) as dblink_session:
        for dblink_table_name in DBLINK_TABLES[type_]:
            table = DblinkBase.metadata.tables[dblink_table_name]
            query = select(table).where((table.c.id0 == id_) | (table.c.id1 == id_))
            result = dblink_session.execute(query).fetchall()
            for row in result:
                related_ids.add(row.id0)
                related_ids.add(row.id1)

    with get_sra_accessions_session(config) as sra_accessions_session:
        for sra_accessions_table_name in SRA_ACCESSIONS_TABLES[type_]:
            table = SraAccessionsBase.metadata.tables[sra_accessions_table_name]
            query = select(table).where((table.c.id0 == id_) | (table.c.id1 == id_))
            result = sra_accessions_session.execute(query).fetchall()
            for row in result:
                related_ids.add(row.id0)
                related_ids.add(row.id1)

    # remove self id
    related_ids.discard(id_)

    return [_to_xref(related_id) for related_id in sorted(list(related_ids))[:XREF_LIMIT]]
