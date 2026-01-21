"""JSONL 出力用 Pydantic モデル定義。"""
from typing import Any, List, Literal, Optional, Union

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


# BioSample visibility: @access から取得
# NCBI: public, controlled
# DDBJ: public のみ
BioSampleVisibility = Literal["public", "controlled"]

# BioSample status: Status/@status から取得
# NCBI: live, suppressed など
# DDBJ: Status 要素なし → "live" とみなす
BioSampleStatus = Literal["live", "suppressed"]


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
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: BioSampleStatus
    visibility: BioSampleVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


# === SRA Types ===

SraStatus = Literal["public", "suppressed", "replaced", "killed", "unpublished"]
SraVisibility = Literal["public", "controlled-access"]

SraType = Literal[
    "sra-submission",
    "sra-study",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-analysis",
]


class SraOrganism(BaseModel):
    identifier: Optional[str]
    name: Optional[str]


class SraSubmission(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["SRA"]
    type_: Literal["sra-submission"] = Field(alias="type")
    name: Optional[str]
    url: str
    title: Optional[str]
    description: Optional[str]
    centerName: Optional[str]
    labName: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: SraStatus
    visibility: SraVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


class SraStudy(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["SRA"]
    type_: Literal["sra-study"] = Field(alias="type")
    name: Optional[str]
    url: str
    organism: Optional[SraOrganism]
    title: Optional[str]
    description: Optional[str]
    studyType: Optional[str]
    centerName: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: SraStatus
    visibility: SraVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


class SraExperiment(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["SRA"]
    type_: Literal["sra-experiment"] = Field(alias="type")
    name: Optional[str]
    url: str
    organism: Optional[SraOrganism]
    title: Optional[str]
    description: Optional[str]
    instrumentModel: Optional[str]
    libraryStrategy: Optional[str]
    librarySource: Optional[str]
    librarySelection: Optional[str]
    libraryLayout: Optional[str]
    centerName: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: SraStatus
    visibility: SraVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


class SraRun(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["SRA"]
    type_: Literal["sra-run"] = Field(alias="type")
    name: Optional[str]
    url: str
    title: Optional[str]
    description: Optional[str]
    runDate: Optional[str]
    runCenter: Optional[str]
    centerName: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: SraStatus
    visibility: SraVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


class SraSampleAttribute(BaseModel):
    tag: Optional[str]
    value: Optional[str]
    units: Optional[str]


class SraSample(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["SRA"]
    type_: Literal["sra-sample"] = Field(alias="type")
    name: Optional[str]
    url: str
    organism: Optional[SraOrganism]
    title: Optional[str]
    description: Optional[str]
    attributes: List[SraSampleAttribute]
    centerName: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: SraStatus
    visibility: SraVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


class SraAnalysis(BaseModel):
    identifier: str
    properties: Any
    distribution: List[Distribution]
    isPartOf: Literal["SRA"]
    type_: Literal["sra-analysis"] = Field(alias="type")
    name: Optional[str]
    url: str
    title: Optional[str]
    description: Optional[str]
    analysisType: Optional[str]
    centerName: Optional[str]
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: SraStatus
    visibility: SraVisibility
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


# SRA モデルの共用型
SraEntry = Union[
    SraSubmission,
    SraStudy,
    SraExperiment,
    SraRun,
    SraSample,
    SraAnalysis,
]