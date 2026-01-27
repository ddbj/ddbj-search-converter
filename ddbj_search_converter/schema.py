"""JSONL 出力用 Pydantic モデル定義。"""
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field

# Status (INSDC 標準)
Status = Literal["live", "unpublished", "suppressed", "withdrawn"]

# Accessibility
Accessibility = Literal["public-access", "controlled-access"]

# === BioProject ===


class Distribution(BaseModel):
    type_: str = Field(alias="type")
    encodingFormat: str
    contentUrl: str


class Organism(BaseModel):
    identifier: Optional[str]
    name: Optional[str]


class Organization(BaseModel):
    name: Optional[str]
    organizationType: Optional[str]
    role: Optional[str]
    url: Optional[str]
    abbreviation: Optional[str]


class Publication(BaseModel):
    id_: Optional[str] = Field(default=None, alias="id")
    title: Optional[str]
    date: Optional[str]
    Reference: Optional[str]
    url: Optional[str]
    DbType: Optional[str]
    status: Optional[str]


class Agency(BaseModel):
    abbreviation: Optional[str]
    name: Optional[str]


class Grant(BaseModel):
    id_: Optional[str] = Field(default=None, alias="id")
    title: Optional[str]
    agency: List[Agency]


class ExternalLink(BaseModel):
    url: str
    label: str


XrefType = Literal[
    "biosample",
    "bioproject",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-study",
    "sra-submission",
    "sra-analysis",
    "jga-study",
    "jga-dataset",
    "jga-dac",
    "jga-policy",
    "gea",
    "geo",
    "hum-id",
    "insdc-assembly",
    "insdc-master",
    "metabobank",
    "pubmed-id",
    "taxonomy",
    "umbrella-bioproject",
]


class Xref(BaseModel):
    identifier: str
    type_: XrefType = Field(alias="type")
    url: str


class BioProject(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["BioProject"]
    type_: Literal["bioproject"] = Field(alias="type")
    objectType: Literal["UmbrellaBioProject", "BioProject"]
    name: Optional[str]
    url: str
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    organization: List[Organization]
    publication: List[Publication]
    grant: List[Grant]
    externalLink: List[ExternalLink]
    dbXrefs: List[Xref]
    sameAs: List[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


# === BioSample ===


class Attribute(BaseModel):
    attribute_name: Optional[str]
    display_name: Optional[str]
    harmonized_name: Optional[str]
    content: Optional[str]


class Model(BaseModel):
    name: str


class Package(BaseModel):
    name: str
    display_name: str


class BioSample(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["BioSample"]
    type_: Literal["biosample"] = Field(alias="type")
    name: Optional[str]
    url: str
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    attributes: List[Attribute]
    model: List[Model]
    package: Optional[Package]
    dbXrefs: List[Xref]
    sameAs: List[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


# === SRA ===


class DownloadUrl(BaseModel):
    type_: Optional[str] = Field(alias="type")
    url: str


class SRA(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["sra"]
    type_: Literal["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"] = Field(alias="type")
    name: Optional[str]
    url: str
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    dbXrefs: List[Xref]
    sameAs: List[Xref]
    downloadUrl: List[DownloadUrl]
    status: Status
    accessibility: Accessibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


# === JGA ===


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
    dbXrefs: List[Xref]
    sameAs: List[Xref]
    status: Literal["live"]
    accessibility: Literal["controlled-access"]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]
