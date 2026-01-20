"""JSONL 出力用 Pydantic モデル定義。"""
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field


class Distribution(BaseModel):
    type_: str = Field(alias="type")
    encodingFormat: str
    contentUrl: str


class Organism(BaseModel):
    identifier: Optional[str]
    name: Optional[str]


XrefType = Literal[
    "biosample",
    "bioproject",
    "gea",
    "hum-id",
    "insdc",
    "insdc-assembly",
    "insdc-master",
    "jga-dac",
    "jga-dataset",
    "jga-policy",
    "jga-study",
    "metabobank",
    "pubmed-id",
    "sra-analysis",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-study",
    "sra-submission",
    "taxonomy",
]


class Xref(BaseModel):
    identifier: str
    type_: XrefType = Field(alias="type")
    url: str


class JGA(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["jga"]
    type_: Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"] = Field(alias="type")
    name: Optional[str]
    url: str
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: Literal["public"]
    visibility: Literal["controlled-access"]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]
