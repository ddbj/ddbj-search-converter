"""Pydantic schema factories for integration tests.

Each ``make_*_doc`` builds a minimum Pydantic instance with all required
fields populated. ``sameAs`` and ``organism`` are intentionally empty/None
so the bulk insert produces exactly ``len(docs)`` documents (no alias docs).
"""

from ddbj_search_converter.es.mappings.jga import JgaIndexType
from ddbj_search_converter.es.mappings.sra import SraIndexType
from ddbj_search_converter.schema import GEA, JGA, SRA, BioProject, BioSample, MetaboBank


def make_bp_doc(identifier: str) -> BioProject:
    return BioProject(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="bioproject",
        type="bioproject",
        objectType="BioProject",
        name=None,
        url=f"https://example.com/{identifier}",
        organism=None,
        title=f"Test {identifier}",
        description=None,
        projectType=[],
        relevance=[],
        organization=[],
        publication=[],
        grant=[],
        externalLink=[],
        dbXrefs=[],
        parentBioProjects=[],
        childBioProjects=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


def make_bs_doc(identifier: str) -> BioSample:
    return BioSample(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="biosample",
        type="biosample",
        name=None,
        url=f"https://example.com/{identifier}",
        organism=None,
        title=f"Test {identifier}",
        description=None,
        derivedFrom=[],
        organization=[],
        model=[],
        package=None,
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


def make_sra_doc(identifier: str, type_: SraIndexType) -> SRA:
    return SRA(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="sra",
        type=type_,
        name=None,
        url=f"https://example.com/{identifier}",
        organism=None,
        title=f"Test {identifier}",
        description=None,
        organization=[],
        publication=[],
        libraryStrategy=[],
        librarySource=[],
        librarySelection=[],
        libraryLayout=None,
        platform=None,
        instrumentModel=[],
        analysisType=None,
        derivedFrom=[],
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


def make_jga_doc(identifier: str, type_: JgaIndexType) -> JGA:
    return JGA(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="jga",
        type=type_,
        name=None,
        url=f"https://example.com/{identifier}",
        organism=None,
        title=f"Test {identifier}",
        description=None,
        organization=[],
        publication=[],
        grant=[],
        externalLink=[],
        studyType=[],
        datasetType=[],
        vendor=[],
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="controlled-access",
        dateCreated=None,
        dateModified=None,
        datePublished=None,
    )


def make_gea_doc(identifier: str) -> GEA:
    return GEA(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="gea",
        type="gea",
        url=f"https://example.com/{identifier}",
        organization=[],
        publication=[],
        experimentType=[],
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
    )


def make_mtb_doc(identifier: str) -> MetaboBank:
    return MetaboBank(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="metabobank",
        type="metabobank",
        url=f"https://example.com/{identifier}",
        organization=[],
        publication=[],
        studyType=[],
        experimentType=[],
        submissionType=[],
        dbXrefs=[],
        sameAs=[],
        status="public",
        accessibility="public-access",
    )
