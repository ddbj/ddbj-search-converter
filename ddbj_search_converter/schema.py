"""Pydantic models for JSONL output (the source of truth for OpenAPI schemas)."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

# Status (INSDC standard)
Status = Annotated[
    Literal["public", "private", "suppressed", "withdrawn"],
    Field(
        description=(
            "Public release state of the INSDC record. "
            '"public" = released; '
            '"private" = not released (hidden by the API as 404); '
            '"suppressed" = retrievable only via exact ID match; '
            '"withdrawn" = retracted (hidden by the API as 404).'
        ),
    ),
]

# Accessibility
Accessibility = Annotated[
    Literal["public-access", "controlled-access"],
    Field(
        description=(
            "Access-control category in DDBJ Search. "
            '"public-access" entries are freely retrievable; '
            '"controlled-access" entries (e.g. JGA) require explicit authorization.'
        ),
    ),
]

EncodingFormat = Annotated[
    Literal["JSON", "JSON-LD", "XML", "FASTQ", "SRA"],
    Field(
        description=(
            "Representation format of a Schema.org Distribution. "
            '"JSON" / "JSON-LD" / "XML" describe textual metadata payloads; '
            '"FASTQ" / "SRA" describe binary sequencing data downloads.'
        ),
    ),
]


# === Shared types ===


class Distribution(BaseModel):
    """Schema.org-compatible DataDownload entry describing a single distribution of a record."""

    type_: str = Field(
        alias="type",
        description='Schema.org "@type" value (effectively the constant "DataDownload").',
    )
    encodingFormat: EncodingFormat
    contentUrl: str = Field(
        description="Download URL where the distribution can be retrieved.",
    )


class Organism(BaseModel):
    """Organism information sourced from INSDC / NCBI Taxonomy."""

    identifier: str | None = Field(
        default=None,
        description='NCBI Taxonomy ID as a numeric string (e.g. "9606"). None when unknown.',
    )
    name: str | None = Field(
        default=None,
        description='Scientific name (e.g. "Homo sapiens"). None when unknown.',
    )


OrganizationType = Annotated[
    Literal["institute", "center", "consortium", "lab"],
    Field(
        description=(
            "Category describing the scale or nature of an organization (INSDC vocabulary)."
        ),
    ),
]
OrganizationRole = Annotated[
    Literal["owner", "participant", "submitter", "broker"],
    Field(
        description=(
            "Role an organization plays in a project. "
            '"owner" = primary owner; '
            '"participant" = participating organization; '
            '"submitter" = direct submitter; '
            '"broker" = submission broker on behalf of others.'
        ),
    ),
]


class Organization(BaseModel):
    """Organization linked to an entry (e.g. submitter, participating institution)."""

    name: str | None = Field(default=None, description="Official organization name.")
    abbreviation: str | None = Field(default=None, description="Abbreviated organization name.")
    role: OrganizationRole | None = Field(default=None, description="Role in the project.")
    organizationType: OrganizationType | None = Field(
        default=None, description="Organization category."
    )
    department: str | None = Field(
        default=None,
        description="Department or laboratory name (e.g. SRA `lab_name`).",
    )
    url: str | None = Field(default=None, description="Organization website URL.")


PublicationDbType = Annotated[
    Literal["pubmed", "doi", "pmc", "other"],
    Field(
        description=(
            "Identifier type of `Publication.id`. "
            '"pubmed" = PubMed ID; '
            '"doi" = DOI string; '
            '"pmc" = PMC ID; '
            '"other" = identifier kept verbatim because it does not match any of the above.'
        ),
    ),
]


class Publication(BaseModel):
    """A single publication or reference associated with an entry."""

    id_: str | None = Field(
        default=None,
        alias="id",
        description="Identifier string of the publication (its kind is given by `dbType`).",
    )
    title: str | None = Field(default=None, description="Publication title.")
    date: str | None = Field(
        default=None,
        description="Publication date as a free-form string (format varies by source).",
    )
    reference: str | None = Field(default=None, description="Full bibliographic reference text.")
    url: str | None = Field(
        default=None,
        description=(
            "URL of the publication. Generated from `id` according to `dbType` "
            "(PubMed / DOI / PMC templates) when possible."
        ),
    )
    dbType: PublicationDbType | None = Field(
        default=None,
        description="Kind of `id`. None when the source value cannot be classified.",
    )


class Grant(BaseModel):
    """A single grant associated with an entry."""

    id_: str | None = Field(default=None, alias="id", description="Grant identifier.")
    title: str | None = Field(default=None, description="Grant title.")
    agency: list[Organization] = Field(
        description="Funding organizations (at least one).",
    )


class ExternalLink(BaseModel):
    """External link attached to a BioProject or JGA entry for UI display."""

    url: str = Field(description="External URL.")
    label: str = Field(description="Human-readable label for UI display.")


XrefType = Annotated[
    Literal[
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
    ],
    Field(
        description=(
            "Accession type of an entry in the dblink graph (21 values). "
            "The DB family of the referenced peer record."
        ),
    ),
]


class Xref(BaseModel):
    """A single reference edge in the dblink graph (pointer to a peer accession)."""

    identifier: str = Field(description="Accession ID of the referenced peer record.")
    type_: XrefType = Field(alias="type")
    url: str = Field(description="Canonical URL of the referenced peer record.")


class BioProject(BaseModel):
    """A BioProject entry — one document of the Elasticsearch `bioproject` index."""

    identifier: str = Field(
        description='Primary accession used as the ES `_id` (e.g. "PRJDB1234", "PRJNA12345").',
    )
    properties: Any = Field(
        description=(
            "Raw element tree carried over from the source XML (converted via xmltodict). "
            "Schema-free nested structure; prefer the typed sibling fields for structured "
            "access. This field exists to round-trip the source representation."
        ),
        json_schema_extra={"additionalProperties": True},
    )
    distribution: list[Distribution] = Field(
        description="Distributions (Schema.org DataDownload). Key is always emitted, even when empty.",
    )
    isPartOf: Annotated[
        Literal["bioproject"],
        Field(
            description=(
                'Coarse-grained index category (used by the front-end DB facet). '
                'Always "bioproject" for BioProject entries.'
            ),
        ),
    ]
    type_: Annotated[
        Literal["bioproject"],
        Field(
            alias="type",
            description=(
                'Fine-grained entry type. Always "bioproject" for this index '
                "(use `objectType` to distinguish umbrella entries)."
            ),
        ),
    ]
    objectType: Annotated[
        Literal["UmbrellaBioProject", "BioProject"],
        Field(
            description=(
                "BioProject object kind. "
                '"UmbrellaBioProject" = upper-level entry that has child BioProjects; '
                '"BioProject" = regular leaf entry.'
            ),
        ),
    ]
    name: str | None = Field(
        default=None, description="Short name (sourced from the `Name` element of the source XML)."
    )
    url: str = Field(description="Canonical DDBJ Search entry URL.")
    organism: Organism | None = Field(default=None, description="Associated organism.")
    title: str | None = Field(
        default=None,
        description="Long human-readable title (sourced from the `Title` element of the source XML).",
    )
    description: str | None = Field(default=None, description="Project description text.")
    projectType: list[str] = Field(
        description="BioProject project-type values (vocabulary is source-dependent).",
    )
    relevance: list[str] = Field(
        description=(
            'Names of relevance tags whose value was "yes" in the source XML. '
            "Possible names: Agricultural / Medical / Industrial / Environmental / "
            "Evolution / ModelOrganism / Other."
        ),
    )
    organization: list[Organization] = Field(
        description="Participating organizations. Key is always emitted, even when empty.",
    )
    publication: list[Publication] = Field(
        description="Associated publications. Key is always emitted, even when empty.",
    )
    grant: list[Grant] = Field(
        description="Associated grants. Key is always emitted, even when empty.",
    )
    externalLink: list[ExternalLink] = Field(
        description="External UI links. Key is always emitted, even when empty.",
    )
    dbXrefs: list[Xref] = Field(
        description=(
            "Edges in the dblink graph. Key is always emitted, even when empty. "
            "Populated only when the JSONL was generated with `--include-dbxrefs`."
        ),
    )
    parentBioProjects: list[Xref] = Field(
        description=(
            "Parent BioProjects in the umbrella DAG. Key is always emitted, even when empty."
        ),
    )
    childBioProjects: list[Xref] = Field(
        description=(
            "Child BioProjects in the umbrella DAG. Key is always emitted, even when empty."
        ),
    )
    sameAs: list[Xref] = Field(
        description=(
            "Alias accessions of this entry (e.g. GEO cross-references). "
            "Key is always emitted, even when empty."
        ),
    )
    status: Status
    accessibility: Accessibility
    dateCreated: str | None = Field(
        default=None,
        description="ISO 8601 (YYYY-MM-DD) creation date. None if absent in the source.",
    )
    dateModified: str | None = Field(
        default=None,
        description="ISO 8601 (YYYY-MM-DD) last-modified date. None if absent in the source.",
    )
    datePublished: str | None = Field(
        default=None,
        description="ISO 8601 (YYYY-MM-DD) publication date. None if absent in the source.",
    )


# === BioSample ===


class BioSamplePackage(BaseModel):
    """BioSample package metadata (INSDC controlled-vocabulary package name)."""

    name: str = Field(
        description='INSDC controlled-vocabulary package name (e.g. "MIGS.ba.soil").',
    )
    displayName: str | None = Field(default=None, description="Human-readable package name for UI display.")


class BioSample(BaseModel):
    """A BioSample entry — one document of the Elasticsearch `biosample` index."""

    identifier: str = Field(
        description='Primary accession used as the ES `_id` (e.g. "SAMD00000001", "SAMN12345678").',
    )
    properties: Any = Field(
        description=(
            "Raw element tree carried over from the source XML (converted via xmltodict). "
            "Schema-free nested structure; prefer the typed sibling fields for structured access."
        ),
        json_schema_extra={"additionalProperties": True},
    )
    distribution: list[Distribution] = Field(
        description="Distributions (Schema.org DataDownload). Key is always emitted, even when empty.",
    )
    isPartOf: Annotated[
        Literal["biosample"],
        Field(
            description=(
                'Coarse-grained index category (used by the front-end DB facet). '
                'Always "biosample" for BioSample entries.'
            ),
        ),
    ]
    type_: Annotated[
        Literal["biosample"],
        Field(
            alias="type",
            description='Fine-grained entry type. Always "biosample" for this index.',
        ),
    ]
    name: str | None = Field(
        default=None, description="Short name (sourced from the `Name` element of the source XML)."
    )
    url: str = Field(description="Canonical DDBJ Search entry URL.")
    organism: Organism | None = Field(default=None, description="Associated organism.")
    title: str | None = Field(default=None, description="Long human-readable title.")
    description: str | None = Field(default=None, description="Sample description text.")
    derivedFrom: list[Xref] = Field(
        description=(
            "Parent entities (e.g. parent BioSample) as Xref edges. "
            "Key is always emitted, even when empty."
        ),
    )
    geoLocName: str | None = Field(
        default=None, description="Geographic collection location (from sample attributes)."
    )
    collectionDate: str | None = Field(
        default=None,
        description="Sample collection date as a free-form string (format varies by source).",
    )
    host: str | None = Field(default=None, description="Host organism (from sample attributes).")
    strain: str | None = Field(
        default=None,
        description='Strain identifier (from sample attributes). Distinct from "isolate".',
    )
    isolate: str | None = Field(
        default=None,
        description='Isolate identifier (from sample attributes). Distinct from "strain".',
    )
    organization: list[Organization] = Field(
        description="Participating organizations. Key is always emitted, even when empty.",
    )
    model: list[str] = Field(
        description="BioSample model values. Key is always emitted, even when empty.",
    )
    package: BioSamplePackage | None = Field(
        description=(
            "INSDC package metadata. Required field — None is allowed, but the builder "
            "must always set this key explicitly."
        ),
    )
    dbXrefs: list[Xref] = Field(
        description=(
            "Edges in the dblink graph. Key is always emitted, even when empty. "
            "Populated only when the JSONL was generated with `--include-dbxrefs`."
        ),
    )
    sameAs: list[Xref] = Field(
        description="Alias accessions of this entry. Key is always emitted, even when empty.",
    )
    status: Status
    accessibility: Accessibility
    dateCreated: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) creation date."
    )
    dateModified: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) last-modified date."
    )
    datePublished: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) publication date."
    )


# === SRA ===


class SRA(BaseModel):
    """An SRA / DRA entry — one document of any of the six Elasticsearch `sra-*` indexes."""

    identifier: str = Field(
        description=(
            'Primary accession (e.g. "DRR000001", "SRP000001"). '
            "The specific subtype is encoded in `type`."
        ),
    )
    properties: Any = Field(
        description=(
            "Raw element tree carried over from the source XML (converted via xmltodict). "
            "Schema-free nested structure; prefer the typed sibling fields for structured access."
        ),
        json_schema_extra={"additionalProperties": True},
    )
    distribution: list[Distribution] = Field(
        description=(
            "Distributions (XML / FASTQ / SRA). Key is always emitted, even when empty. "
            "DRA-origin runs may include FASTQ / SRA; for NCBI/EBI-origin runs the SRA mirror URL is generated mechanically."
        ),
    )
    isPartOf: Annotated[
        Literal["sra"],
        Field(
            description=(
                'Coarse-grained index category (used by the front-end DB facet). '
                'Always "sra" for SRA entries — the fine subtype is in `type`.'
            ),
        ),
    ]
    type_: Annotated[
        Literal["sra-submission", "sra-study", "sra-experiment", "sra-run", "sra-sample", "sra-analysis"],
        Field(
            alias="type",
            description=(
                "Fine-grained SRA entry type "
                "(submission / study / experiment / run / sample / analysis)."
            ),
        ),
    ]
    name: str | None = Field(default=None, description="Short name.")
    url: str = Field(description="Canonical DDBJ Search entry URL.")
    organism: Organism | None = Field(default=None, description="Associated organism.")
    title: str | None = Field(default=None, description="Long human-readable title.")
    description: str | None = Field(default=None, description="Entry description text.")
    organization: list[Organization] = Field(
        description="Participating organizations. Key is always emitted, even when empty.",
    )
    publication: list[Publication] = Field(
        description="Associated publications. Key is always emitted, even when empty.",
    )
    libraryStrategy: list[str] = Field(
        description=(
            "INSDC SRA library-strategy values (controlled vocabulary). "
            "Key is always emitted, even when empty."
        ),
    )
    librarySource: list[str] = Field(
        description=(
            "INSDC SRA library-source values (controlled vocabulary). "
            "Key is always emitted, even when empty."
        ),
    )
    librarySelection: list[str] = Field(
        description=(
            "INSDC SRA library-selection values (controlled vocabulary). "
            "Key is always emitted, even when empty."
        ),
    )
    libraryLayout: str | None = Field(
        description=(
            "INSDC SRA library layout (e.g. PAIRED / SINGLE). "
            "Required field — None is allowed, but the builder must always set this key explicitly."
        ),
    )
    platform: str | None = Field(
        description=(
            "INSDC SRA sequencing platform (e.g. ILLUMINA). "
            "Required field — None is allowed, but the builder must always set this key explicitly."
        ),
    )
    instrumentModel: list[str] = Field(
        description=(
            "INSDC SRA instrument-model values (controlled vocabulary). "
            "Key is always emitted, even when empty."
        ),
    )
    libraryName: str | None = Field(default=None, description="Library name (free-form string).")
    libraryConstructionProtocol: str | None = Field(
        default=None, description="Library construction protocol (free-form text)."
    )
    analysisType: str | None = Field(
        description=(
            "Analysis type for `sra-analysis` entries. "
            "Required field — None is allowed, but the builder must always set this key explicitly."
        ),
    )
    collectionDate: str | None = Field(
        default=None,
        description="Sample collection date as a free-form string (format varies by source).",
    )
    geoLocName: str | None = Field(
        default=None, description="Geographic collection location."
    )
    derivedFrom: list[Xref] = Field(
        description=(
            "Parent entities (e.g. an sra-sample referencing its BioSample) as Xref edges. "
            "Key is always emitted, even when empty."
        ),
    )
    dbXrefs: list[Xref] = Field(
        description=(
            "Edges in the dblink graph. Key is always emitted, even when empty. "
            "Populated only when the JSONL was generated with `--include-dbxrefs`."
        ),
    )
    sameAs: list[Xref] = Field(
        description="Alias accessions of this entry. Key is always emitted, even when empty.",
    )
    status: Status
    accessibility: Accessibility
    dateCreated: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) creation date."
    )
    dateModified: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) last-modified date."
    )
    datePublished: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) publication date."
    )


# === JGA ===


class JGA(BaseModel):
    """A JGA entry — one document of any of the four `jga-*` indexes (controlled-access)."""

    identifier: str = Field(
        description='Primary accession (e.g. "JGAS000001"). The specific subtype is in `type`.',
    )
    properties: Any = Field(
        description=(
            "Raw element tree carried over from the source XML / CSV (dict-converted). "
            "Schema-free nested structure; prefer the typed sibling fields for structured access."
        ),
        json_schema_extra={"additionalProperties": True},
    )
    distribution: list[Distribution] = Field(
        description=(
            "Distributions (JSON / JSON-LD only for JGA). "
            "Key is always emitted, even when empty."
        ),
    )
    isPartOf: Annotated[
        Literal["jga"],
        Field(
            description=(
                'Coarse-grained index category (used by the front-end DB facet). '
                'Always "jga" for JGA entries.'
            ),
        ),
    ]
    type_: Annotated[
        Literal["jga-study", "jga-dataset", "jga-dac", "jga-policy"],
        Field(
            alias="type",
            description="Fine-grained JGA entry type (study / dataset / dac / policy).",
        ),
    ]
    name: str | None = Field(default=None, description="Short name.")
    url: str = Field(description="Canonical DDBJ Search entry URL.")
    organism: Organism | None = Field(default=None, description="Associated organism.")
    title: str | None = Field(default=None, description="Long human-readable title.")
    description: str | None = Field(default=None, description="Entry description text.")
    organization: list[Organization] = Field(
        description="Participating organizations. Key is always emitted, even when empty.",
    )
    publication: list[Publication] = Field(
        description="Associated publications. Key is always emitted, even when empty.",
    )
    grant: list[Grant] = Field(
        description="Associated grants. Key is always emitted, even when empty.",
    )
    externalLink: list[ExternalLink] = Field(
        description="External UI links. Key is always emitted, even when empty.",
    )
    studyType: list[str] = Field(
        description=(
            "JGA study-type values (vocabulary is source-dependent). "
            "Key is always emitted, even when empty."
        ),
    )
    datasetType: list[str] = Field(
        description=(
            "JGA dataset-type values (vocabulary is source-dependent). "
            "Key is always emitted, even when empty."
        ),
    )
    vendor: list[str] = Field(
        description="Vendor values. Key is always emitted, even when empty.",
    )
    dbXrefs: list[Xref] = Field(
        description=(
            "Edges in the dblink graph. Key is always emitted, even when empty. "
            "Populated only when the JSONL was generated with `--include-dbxrefs`."
        ),
    )
    sameAs: list[Xref] = Field(
        description=(
            "Alias accessions (e.g. JGA Secondary IDs). Key is always emitted, even when empty."
        ),
    )
    status: Annotated[
        Literal["public"],
        Field(
            description=(
                'JGA is always "public" — meaningful access control is handled by '
                "`accessibility` (controlled-access)."
            ),
        ),
    ]
    accessibility: Annotated[
        Literal["controlled-access"],
        Field(description='JGA is always "controlled-access".'),
    ]
    dateCreated: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) creation date."
    )
    dateModified: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) last-modified date."
    )
    datePublished: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) publication date."
    )


# === GEA ===


class GEA(BaseModel):
    """A GEA entry — one document of the Elasticsearch `gea` index (IDF/SDRF-derived)."""

    identifier: str = Field(description='Primary accession (e.g. "E-GEAD-1").')
    properties: Any = Field(
        description=(
            "Raw element tree carried over from the source IDF. "
            "Schema-free; top-level values are list[str]."
        ),
        json_schema_extra={"additionalProperties": True},
    )
    distribution: list[Distribution] = Field(
        description=(
            "Distributions (JSON / JSON-LD only for GEA). "
            "Key is always emitted, even when empty."
        ),
    )
    isPartOf: Annotated[
        Literal["gea"],
        Field(
            description=(
                'Coarse-grained index category (used by the front-end DB facet). '
                'Always "gea" for GEA entries.'
            ),
        ),
    ]
    type_: Annotated[
        Literal["gea"],
        Field(
            alias="type",
            description='Fine-grained entry type. Always "gea" for this index.',
        ),
    ]
    name: str | None = Field(default=None, description="Short name.")
    url: str = Field(description="Canonical DDBJ Search entry URL.")
    organism: Organism | None = Field(default=None, description="Associated organism.")
    title: str | None = Field(default=None, description="Long human-readable title.")
    description: str | None = Field(default=None, description="Entry description text.")
    organization: list[Organization] = Field(
        description="Participating organizations. Key is always emitted, even when empty.",
    )
    publication: list[Publication] = Field(
        description="Associated publications. Key is always emitted, even when empty.",
    )
    experimentType: list[str] = Field(
        description=(
            "Experiment-type values (free-form text per source; e.g. Microarray vs. Sequencing). "
            "Key is always emitted, even when empty."
        ),
    )
    dbXrefs: list[Xref] = Field(
        description=(
            "Edges in the dblink graph. Key is always emitted, even when empty. "
            "Populated only when the JSONL was generated with `--include-dbxrefs`."
        ),
    )
    sameAs: list[Xref] = Field(
        description="Alias accessions of this entry. Key is always emitted, even when empty.",
    )
    status: Annotated[
        Literal["public"],
        Field(description='GEA is always "public".'),
    ]
    accessibility: Annotated[
        Literal["public-access"],
        Field(description='GEA is always "public-access".'),
    ]
    dateCreated: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) creation date."
    )
    dateModified: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) last-modified date."
    )
    datePublished: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) publication date."
    )


# === MetaboBank ===


class MetaboBank(BaseModel):
    """A MetaboBank entry — one document of the Elasticsearch `metabobank` index (IDF/SDRF-derived)."""

    identifier: str = Field(description='Primary accession (e.g. "MTBKS1").')
    properties: Any = Field(
        description=(
            "Raw element tree carried over from the source IDF. "
            "Schema-free; top-level values are list[str]."
        ),
        json_schema_extra={"additionalProperties": True},
    )
    distribution: list[Distribution] = Field(
        description=(
            "Distributions (JSON / JSON-LD only for MetaboBank). "
            "Key is always emitted, even when empty."
        ),
    )
    isPartOf: Annotated[
        Literal["metabobank"],
        Field(
            description=(
                'Coarse-grained index category (used by the front-end DB facet). '
                'Always "metabobank" for MetaboBank entries.'
            ),
        ),
    ]
    type_: Annotated[
        Literal["metabobank"],
        Field(
            alias="type",
            description='Fine-grained entry type. Always "metabobank" for this index.',
        ),
    ]
    name: str | None = Field(default=None, description="Short name.")
    url: str = Field(description="Canonical DDBJ Search entry URL.")
    organism: Organism | None = Field(default=None, description="Associated organism.")
    title: str | None = Field(default=None, description="Long human-readable title.")
    description: str | None = Field(default=None, description="Entry description text.")
    organization: list[Organization] = Field(
        description="Participating organizations. Key is always emitted, even when empty.",
    )
    publication: list[Publication] = Field(
        description="Associated publications. Key is always emitted, even when empty.",
    )
    studyType: list[str] = Field(
        description=(
            "Study-type values (free-form text per source). "
            "Key is always emitted, even when empty."
        ),
    )
    experimentType: list[str] = Field(
        description=(
            "Experiment-type values (free-form text per source). "
            "Key is always emitted, even when empty."
        ),
    )
    submissionType: list[str] = Field(
        description=(
            "Submission-type values (free-form text per source). "
            "Key is always emitted, even when empty."
        ),
    )
    dbXrefs: list[Xref] = Field(
        description=(
            "Edges in the dblink graph. Key is always emitted, even when empty. "
            "Populated only when the JSONL was generated with `--include-dbxrefs`."
        ),
    )
    sameAs: list[Xref] = Field(
        description="Alias accessions of this entry. Key is always emitted, even when empty.",
    )
    status: Annotated[
        Literal["public"],
        Field(description='MetaboBank is always "public".'),
    ]
    accessibility: Annotated[
        Literal["public-access"],
        Field(description='MetaboBank is always "public-access".'),
    ]
    dateCreated: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) creation date."
    )
    dateModified: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) last-modified date."
    )
    datePublished: str | None = Field(
        default=None, description="ISO 8601 (YYYY-MM-DD) publication date."
    )
