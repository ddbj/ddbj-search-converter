from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field

# === BioProject ===


class Distribution(BaseModel):
    type_: str = Field(alias="type")
    encodingFormat: str
    contentUrl: str


class Organism(BaseModel):
    identifier: str
    name: Optional[str]


class Organization(BaseModel):
    name: str
    organizationType: Optional[str]
    role: Optional[str]
    url: Optional[str]
    abbreviation: Optional[str]


class Publication(BaseModel):
    id_: Optional[str] = Field(alias="id")
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
    id_: Optional[str] = Field(alias="id")
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
    "gea",
    "assemblies",
    "metabobank",
    "taxonomy",
    "GEO",
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
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: str
    visibility: Literal["unrestricted-access"]
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
    dbXref: List[Xref]
    sameAs: List[Xref]
    status: Literal["public"]
    visibility: Literal["unrestricted-access"]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]
