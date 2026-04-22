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
    identifier: str | None
    name: str | None


OrganizationType = Literal["institute", "center", "consortium", "lab"]
OrganizationRole = Literal["owner", "participant", "submitter", "broker"]


class Organization(BaseModel):
    name: str | None = None
    abbreviation: str | None = None
    role: OrganizationRole | None = None
    organizationType: OrganizationType | None = None
    department: str | None = None
    url: str | None = None


PublicationDbType = Literal["ePubmed", "eDOI", "ePMC", "eNotAvailable"]
PublicationStatus = Literal["ePublished", "eUnpublished"]


class Publication(BaseModel):
    id_: str | None = Field(default=None, alias="id")
    title: str | None = None
    date: str | None = None
    reference: str | None = None
    url: str | None = None
    dbType: PublicationDbType | None = None
    status: PublicationStatus | None = None


class Grant(BaseModel):
    id_: str | None = Field(default=None, alias="id")
    title: str | None
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
    parentBioProjects: list[Xref]
    childBioProjects: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None


# === BioSample ===


class BioSamplePackage(BaseModel):
    name: str
    displayName: str | None = None


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
    organization: list[Organization]
    model: list[str]
    package: BioSamplePackage | None
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Status
    accessibility: Accessibility
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None


# === SRA ===

LibrarySource = Literal[
    "GENOMIC",
    "METAGENOMIC",
    "TRANSCRIPTOMIC",
    "VIRAL RNA",
    "OTHER",
    "METATRANSCRIPTOMIC",
    "TRANSCRIPTOMIC SINGLE CELL",
    "GENOMIC SINGLE CELL",
    "SYNTHETIC",
]

LibraryLayout = Literal["PAIRED", "SINGLE"]

Platform = Literal[
    "ILLUMINA",
    "OXFORD_NANOPORE",
    "PACBIO_SMRT",
    "ION_TORRENT",
    "LS454",
    "CAPILLARY",
    "DNBSEQ",
    "BGISEQ",
    "ELEMENT",
    "ABI_SOLID",
    "COMPLETE_GENOMICS",
    "HELICOS",
    "ULTIMA",
    "GENEMIND",
    "VELA_DIAGNOSTICS",
    "TAPESTRI",
    "GENAPSYS",
    "SINGULAR_GENOMICS",
    "GENEUS_TECH",
    "SALUS",
]

AnalysisType = Literal[
    "DE_NOVO_ASSEMBLY",
    "REFERENCE_ALIGNMENT",
    "ABUNDANCE_MEASUREMENT",
    "SEQUENCE_ANNOTATION",
]


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
    organization: list[Organization]
    publication: list[Publication]
    libraryStrategy: list[str]
    librarySource: list[LibrarySource]
    librarySelection: list[str]
    libraryLayout: LibraryLayout | None
    platform: Platform | None
    instrumentModel: list[str]
    analysisType: AnalysisType | None
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
    organization: list[Organization] = Field(default_factory=list)
    publication: list[Publication] = Field(default_factory=list)
    grant: list[Grant] = Field(default_factory=list)
    externalLink: list[ExternalLink] = Field(default_factory=list)
    studyType: list[str] = Field(default_factory=list)
    datasetType: list[str] = Field(default_factory=list)
    vendor: list[str] = Field(default_factory=list)
    dbXrefs: list[Xref]
    sameAs: list[Xref]
    status: Literal["public"]
    accessibility: Literal["controlled-access"]
    dateCreated: str | None
    dateModified: str | None
    datePublished: str | None


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
    organization: list[Organization] = Field(default_factory=list)
    publication: list[Publication] = Field(default_factory=list)
    experimentType: list[str] = Field(default_factory=list)
    dbXrefs: list[Xref] = Field(default_factory=list)
    sameAs: list[Xref] = Field(default_factory=list)
    status: Literal["public"]
    accessibility: Literal["public-access"]
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None


# === MetaboBank ===

# §4.6.4 Phase C で全 110 件 controlled vocab 確認済 (unique 値数は値列挙そのまま)
MetabobankStudyType = Literal[
    "untargeted metabolite profiling",
    "targeted metabolite profiling",
    "metabolite target analysis",
    "lipid profiling",
    "metabolite profiling",
    "metabolomics",
    "volatile organic compound",
    "blood metabolite profiling",
]

MetabobankExperimentType = Literal[
    "liquid chromatography-mass spectrometry",
    "fourier transform ion cyclotron resonance mass spectrometry",
    "time-of-flight mass spectrometry",
    "gas chromatography-mass spectrometry",
    "quadrupole mass spectrometer",
    "tandem mass spectrometry",
    "orbitrap",
    "data-dependent acquisition",
    "capillary electrophoresis-mass spectrometry",
    "flow injection analysis-mass spectrometry",
    "ion mobility spectrometry-mass spectrometry",
    "nuclear magnetic resonance spectroscopy",
    "direct infusion-mass spectrometry",
    "mass spectrometry imaging",
    "SWATH MS",
    "selected reaction monitoring",
    "selective ion monitoring",
    "ultra-performance liquid chromatography-mass spectrometry",
]

MetabobankSubmissionType = Literal[
    "LC-DAD-MS",
    "LC-MS",
    "GC-MS",
    "CE-MS",
    "FIA-MS",
    "NMR",
    "DI-MS",
    "MSI",
]


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
    organization: list[Organization] = Field(default_factory=list)
    publication: list[Publication] = Field(default_factory=list)
    studyType: list[MetabobankStudyType] = Field(default_factory=list)
    experimentType: list[MetabobankExperimentType] = Field(default_factory=list)
    submissionType: list[MetabobankSubmissionType] = Field(default_factory=list)
    dbXrefs: list[Xref] = Field(default_factory=list)
    sameAs: list[Xref] = Field(default_factory=list)
    status: Literal["public"]
    accessibility: Literal["public-access"]
    dateCreated: str | None = None
    dateModified: str | None = None
    datePublished: str | None = None
