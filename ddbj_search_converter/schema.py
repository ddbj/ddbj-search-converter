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
    "bioproject",
    "biosample",
    "gea",
    "geo",
    "hum-id",
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
    "umbrella-bioproject",
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


class Organization(BaseModel):
    name: Optional[str]
    organizationType: Optional[str]
    role: Optional[str]
    url: Optional[str]
    abbreviation: Optional[str]


class Agency(BaseModel):
    abbreviation: Optional[str]
    name: Optional[str]


class Grant(BaseModel):
    id: Optional[str]
    title: Optional[str]
    agency: List[Agency]


class Publication(BaseModel):
    id: Optional[str]
    title: Optional[str]
    date: Optional[str]
    Reference: Optional[str]
    url: Optional[str]
    DbType: Optional[str]
    status: Optional[str]


class ExternalLink(BaseModel):
    url: str
    label: str


class BioProject(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["BioProject"]
    type_: Literal["bioproject"] = Field(alias="type")
    objectType: Literal["BioProject", "UmbrellaBioProject"]
    name: Optional[str]
    url: str
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    organization: List[Organization]
    publication: List[Publication]
    grant: List[Grant]
    externalLink: List[ExternalLink]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: str
    visibility: Literal["unrestricted-access"]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]
