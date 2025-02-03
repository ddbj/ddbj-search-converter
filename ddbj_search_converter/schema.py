from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

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
    id_: str = Field(alias="id")
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
    dbXref: Optional[List[Xref]]
    sameAs: List[Xref]
    status: str
    visibility: Literal["unrestricted-access"]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]


class BioProjectLD(BioProject):
    context: str = Field(alias="@context")
    id_: str = Field(alias="@id")

    model_config = ConfigDict(
        populate_by_name=True
    )


# === BioSample ===


# class Attribute(BaseModel):
#     attribute_name: str
#     display_name: str
#     harmonized_name: str
#     content: str


# class Model(BaseModel):
#     name: str


# class Package(BaseModel):
#     name: str
#     display_name: str


# class BioSample(BaseModel):
#     type_: Literal["biosample"] = Field(alias="type")
#     identifier: str
#     name: Optional[str]
#     dateCreated: str
#     datePublished: Optional[str]
#     dateModified: str
#     visibility: str
#     status: str
#     isPartOf: str
#     url: str
#     distribution: List[Distribution]
#     properties: Any
#     sameAs: Union[List[Xref], None]
#     description: Optional[str]
#     title: Optional[str]
#     dbXref: Union[List[Xref], None]
#     organism: Union[Organism, None]
#     attributes: List[Attribute]
#     model: List[Model]
#     Package: Package


# class BioSampleLD(BioSample):
#     context: str = Field(alias="@context")
#     id_: str = Field(alias="@id")

#     model_config = ConfigDict(
#         populate_by_name=True
#     )
