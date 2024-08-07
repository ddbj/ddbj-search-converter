import re
from typing import Dict, List, Literal

from pydantic import BaseModel
from sqlalchemy import Table
from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData

from ddbj_search_converter.dblink.create_dblink_db import \
    get_session as get_dblink_session
from ddbj_search_converter.dblink.id_relation_db import \
    get_session as get_accessions_session

# dbXref が 1M 要素あるようなドキュメントがあり、Elasticsearch のリクエストが 413 となるのを回避するため設定
SELECT_LIMIT = 10000


class DBXRef(BaseModel):
    identifier: str
    type: str


ProjectOrSample = Literal["bioproject", "biosample"]


ACCESSIONS_TABLES: Dict[ProjectOrSample, List[str]] = {
    "bioproject": [
        "experiment_bioproject",
        "study_bioproject",
    ],
    "biosample": [
        "experiment_biosample",
        "experiment_sample",
        "run_biosample",
        "sample_biosample",
    ]
}


DBLINK_TABLES: Dict[ProjectOrSample, List[str]] = {
    "bioproject": [
        "assembly_genome2bp",
        "bioproject2biosample",
        "bioproject_umbrella2bioproject",
        "biosample2bioproject",
        "gea2bioproject",
        "insdc2bioproject",
        "insdc_master2bioproject",
        "mtb_id_bioproject",
        "ncbi_biosample_bioproject",
    ],
    "biosample": [
        "assembly_genome2bs",
        "bioproject2biosample",
        "biosample2bioproject",
        "gea2biosample",
        "insdc2biosample",
        "insdc_master2biosample",
        "mtb_id_biosample",
        "ncbi_biosample_bioproject",
        "trace_biosample_taxon2bs"
    ]
}

ID_PATTERN_MAP = {
    "biosample": re.compile(r"^SAM"),
    "bioproject": re.compile(r"^PRJ"),
    "sra-experiment": re.compile(r"(S|D|E)RX"),
    "sra-run": re.compile(r"(S|D|E)RR"),
    "sra-sample": re.compile(r"(S|D|E)RS"),
    "sra-study": re.compile(r"(S|D|E)RP"),
    "gea": re.compile(r"^E-GEA"),
    "assemblies": re.compile(r"^GCA"),
    "mtb": re.compile(r"^MTB"),
    "insdc": re.compile(r"([A-Z]{1}[0-9]{5}\.[0-9]+|[A-Z]{2}[0-9]{6}|[A-Z]{2}[0-9]{6}\.[0-9]+|[A-Z]{2}[0-9]{8}|[A-Z]{4}[0-9]{2}S?[0-9]{6,8})|[A-Z]{6}[0-9]{2}S?[0-9]{7,9}"),
}


def id_to_dbxref(id_: str) -> DBXRef:
    for db_type, pattern in ID_PATTERN_MAP.items():
        if pattern.match(id_):
            return DBXRef(identifier=id_, type=db_type)

    # map に含まれず値が数字で始まる ID であれば type: taxonomy を返す
    return DBXRef(identifier=id_, type="taxonomy")


def select_dbxref(
    accessions_engine: Engine,
    dblink_engine: Engine,
    id_: str,
    project_or_sample: ProjectOrSample,
) -> List[DBXRef]:
    """
    全ての関係テーブルから指定した ID に関連する ID を取得する
    関係テーブルの引数の ID をどちらかに含む相補的な ID リストを変換した dbXref オブジェクトのリストを返す
    """
    # sra_accessions と dblink の 2 系統の relation をまとめる
    relation_ids = set()

    # TODO: 要確認
    with get_accessions_session(engine=accessions_engine) as accessions_session:
        for table_name in ACCESSIONS_TABLES[project_or_sample]:
            table = Table(table_name, MetaData(), autoload_with=accessions_engine)
            rows = accessions_session.query(table).filter(
                (table.c.id0 == id_) | (table.c.id1 == id_),
            ).limit(SELECT_LIMIT).all()
            for row in rows:
                relation_ids.add(row.id0)
                relation_ids.add(row.id1)

    # TODO: 要確認
    with get_dblink_session(engine=dblink_engine) as dblink_session:
        for table_name in DBLINK_TABLES[project_or_sample]:
            table = Table(table_name, MetaData(), autoload_with=dblink_engine)
            rows = dblink_session.query(table).filter(
                (table.c.field1 == id_) | (table.c.field2 == id_),
            ).limit(SELECT_LIMIT).all()
            for row in rows:
                relation_ids.add(row.field1)
                relation_ids.add(row.field2)

    return [id_to_dbxref(x) for x in relation_ids if x != id_]


# TODO: add tests
# test_id_list = ["SAMN00000002", "PRJNA3", "PRJNA5"]
