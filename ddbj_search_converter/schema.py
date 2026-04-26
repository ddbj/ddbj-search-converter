"""JSONL 出力用 Pydantic モデル定義。"""

from typing import Any, Literal

from pydantic import BaseModel, Field

# Status (INSDC 標準)
Status = Literal["public", "private", "suppressed", "withdrawn"]

# Accessibility
Accessibility = Literal["public-access", "controlled-access"]

EncodingFormat = Literal["JSON", "JSON-LD", "XML", "FASTQ", "SRA"]


# === 共通型 ===


class Distribution(BaseModel):
    type_: str = Field(alias="type")
    encodingFormat: EncodingFormat
    contentUrl: str


class Organism(BaseModel):
    identifier: str | None = None
    name: str | None = None


OrganizationType = Literal["institute", "center", "consortium", "lab"]
OrganizationRole = Literal["owner", "participant", "submitter", "broker"]


class Organization(BaseModel):
    name: str | None = None
    abbreviation: str | None = None
    role: OrganizationRole | None = None
    organizationType: OrganizationType | None = None
    department: str | None = None
    url: str | None = None


PublicationDbType = Literal["pubmed", "doi", "pmc", "other"]


class Publication(BaseModel):
    id_: str | None = Field(default=None, alias="id")
    title: str | None = None
    date: str | None = None
    reference: str | None = None
    url: str | None = None
    dbType: PublicationDbType | None = None


class Grant(BaseModel):
    id_: str | None = Field(default=None, alias="id")
    title: str | None = None
    agency: list[Organization]


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
    "humandbs",
    "insdc",
    "insdc-assembly",
    "insdc-master",
    "metabobank",
    "pubmed",
    "taxonomy",
]


class Xref(BaseModel):
    identifier: str
    type_: XrefType = Field(alias="type")
    url: str


class BioProject(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["bioproject"]
    type_: Literal["bioproject"] = Field(alias="type")
    objectType: Literal["UmbrellaBioProject", "BioProject"]
    name: str | None = None
    url: str
    organism: Organism | None = None
    title: str | None = None
    description: str | None = None
    projectType: list[str]
    relevance: list[str]
    organization: list[Organization]
    publication: list[Publication]
    grant: list[Grant]
    externalLink: list[ExternalLink]
    dbXrefs: list[Xref]
    parentBioProjects: list[Xref]
    childBioProjects: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None


# === BioSample ===


class BioSamplePackage(BaseModel):
    name: str
    displayName: str | None = None


class BioSample(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["biosample"]
    type_: Literal["biosample"] = Field(alias="type")
    name: str | None = None
    url: str
    organism: Organism | None = None
    title: str | None = None
    description: str | None = None
    derivedFrom: list[Xref]
    geoLocName: str | None = None
    collectionDate: str | None = None
    host: str | None = None
    strain: str | None = None
    isolate: str | None = None
    organization: list[Organization]
    model: list[str]
    package: BioSamplePackage | None
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None


# === SRA ===


class SRA(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["sra"]
    type_: Literal["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"] = Field(
        alias="type"
    )
    name: str | None = None
    url: str
    organism: Organism | None = None
    title: str | None = None
    description: str | None = None
    organization: list[Organization]
    publication: list[Publication]
    libraryStrategy: list[str]
    librarySource: list[str]
    librarySelection: list[str]
    libraryLayout: str | None
    platform: str | None
    instrumentModel: list[str]
    libraryName: str | None = None
    libraryConstructionProtocol: str | None = None
    analysisType: str | None
    collectionDate: str | None = None
    geoLocName: str | None = None
    derivedFrom: list[Xref]
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None


# === JGA ===


class JGA(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["jga"]
    type_: Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"] = Field(alias="type")
    name: str | None = None
    url: str
    organism: Organism | None = None
    title: str | None = None
    description: str | None = None
    organization: list[Organization]
    publication: list[Publication]
    grant: list[Grant]
    externalLink: list[ExternalLink]
    studyType: list[str]
    datasetType: list[str]
    vendor: list[str]
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Literal["public"]
    accessibility: Literal["controlled-access"]
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None


# === GEA ===


class GEA(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["gea"]
    type_: Literal["gea"] = Field(alias="type")
    name: str | None = None
    url: str
    organism: Organism | None = None
    title: str | None = None
    description: str | None = None
    organization: list[Organization]
    publication: list[Publication]
    experimentType: list[str]
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Literal["public"]
    accessibility: Literal["public-access"]
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None


# === MetaboBank ===


class MetaboBank(BaseModel):
    identifier: str
    properties: Any
    distribution: list[Distribution]
    isPartOf: Literal["metabobank"]
    type_: Literal["metabobank"] = Field(alias="type")
    name: str | None = None
    url: str
    organism: Organism | None = None
    title: str | None = None
    description: str | None = None
    organization: list[Organization]
    publication: list[Publication]
    studyType: list[str]
    experimentType: list[str]
    submissionType: list[str]
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Literal["public"]
    accessibility: Literal["public-access"]
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None
