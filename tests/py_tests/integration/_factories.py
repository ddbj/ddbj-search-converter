"""Pydantic schema factories for integration tests.

Each ``make_*_doc`` builds a minimum Pydantic instance with all required
fields populated. ``organism`` is None, and ``sameAs`` is empty unless the
caller passes Secondary IDs to ``make_jga_doc``.
"""

from ddbj_search_converter.es.mappings.jga import JgaIndexType
from ddbj_search_converter.es.mappings.sra import SraIndexType
from ddbj_search_converter.schema import GEA, JGA, SRA, BioProject, BioSample, MetaboBank, Xref


def make_bp_doc(identifier: str) -> BioProject:
    return BioProject(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="bioproject",
        type_="bioproject",
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
        type_="biosample",
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
        type_=type_,
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


def make_jga_doc(identifier: str, type_: JgaIndexType, same_as: list[Xref] | None = None) -> JGA:
    return JGA(
        identifier=identifier,
        properties={},
        distribution=[],
        isPartOf="jga",
        type_=type_,
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
        sameAs=same_as or [],
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
        type_="gea",
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
        type_="metabobank",
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
