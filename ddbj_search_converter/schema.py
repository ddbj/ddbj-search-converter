"""JSONL 出力用 Pydantic モデル定義。"""

from typing import Any, Literal

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
    identifier: str | None
    name: str | None


class Organization(BaseModel):
    name: str | None
    organizationType: str | None
    role: str | None
    url: str | None
    abbreviation: str | None


class Publication(BaseModel):
    id_: str | None = Field(default=None, alias="id")
    title: str | None
    date: str | None
    Reference: str | None
    url: str | None
    DbType: str | None
    status: str | None


class Agency(BaseModel):
    abbreviation: str | None
    name: str | None


class Grant(BaseModel):
    id_: str | None = Field(default=None, alias="id")
    title: str | None
    agency: list[Agency]


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
    distribution: list[Distribution]
    isPartOf: Literal["BioProject"]
    type_: Literal["bioproject"] = Field(alias="type")
    objectType: Literal["UmbrellaBioProject", "BioProject"]
    name: str | None
    url: str
    organism: Organism | None
    title: str | None
    description: str | None
    organization: list[Organization]
    publication: list[Publication]
    grant: list[Grant]
    externalLink: list[ExternalLink]
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None


# === BioSample ===


class Attribute(BaseModel):
    attribute_name: str | None
    display_name: str | None
    harmonized_name: str | None
    content: str | None


class Model(BaseModel):
    name: str


class Package(BaseModel):
    name: str
    display_name: str


class BioSample(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["BioSample"]
    type_: Literal["biosample"] = Field(alias="type")
    name: str | None
    url: str
    organism: Organism | None
    title: str | None
    description: str | None
    attributes: list[Attribute]
    model: list[Model]
    package: Package | None
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None


# === SRA ===


class SRA(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["sra"]
    type_: Literal["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"] = Field(
        alias="type"
    )
    name: str | None
    url: str
    organism: Organism | None
    title: str | None
    description: str | None
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None


# === JGA ===


class JGA(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["jga"]
    type_: Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"] = Field(alias="type")
    name: str | None
    url: str
    organism: Organism | None
    title: str | None
    description: str | None
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Literal["live"]
    accessibility: Literal["controlled-access"]
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None
