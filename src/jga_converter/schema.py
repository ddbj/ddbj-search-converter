from typing import Any, List, Literal, Optional
from pydantic import BaseModel, Field


XrefType = Literal[
    "biosample",
    "bioproject",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-study",
    "gea",
    "insdc-assembly",
    "insdc-master",
    "insdc",
    "jga-dac",
    "jga-dataset",
    "jga-policy",
    "jga-study",
    "metabobank",
    "taxonomy",
    "GEO",
]


class Attribute(BaseModel):
    attribute_name: Optional[str]
    display_name: Optional[str]
    harmonized_name: Optional[str]
    content: Optional[str]


class Model(BaseModel):
    name: str


# === JGA ===
class Organism(BaseModel):
    identifier: Literal["9606"]
    name: Literal["Homo sapiens"]


class Xref(BaseModel):
    identifier: str
    type_: XrefType = Field(alias="type")
    url: str


class JGA(BaseModel):
    identifier: str
    properties: Any
    title: Optional[str]
    description: Optional[str]
    name: Optional[str]
    type: Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"]
    url: str
    sameAs: None
    isPartOf: Literal["jga"]
    organism: Optional[Organism]
    dbXref: List[Xref]
    status: Literal["public"]
    visibility: Literal["controlled-access"]
    dateCreated: Optional[str]
    dateModified: Optional[str]
    datePublished: Optional[str]