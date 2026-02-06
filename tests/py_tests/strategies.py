"""Hypothesis custom strategies for PBT."""
import re
import string
from datetime import datetime

from hypothesis import strategies as st

from ddbj_search_converter.dblink.db import AccessionType

ALL_ACCESSION_TYPES: list[AccessionType] = [
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


def st_accession_type() -> st.SearchStrategy[AccessionType]:
    """AccessionType の Literal 値から sampled_from。"""
    return st.sampled_from(ALL_ACCESSION_TYPES)


def st_bioproject_id() -> st.SearchStrategy[str]:
    """Valid BioProject accession IDs."""
    prefix = st.sampled_from(["PRJD", "PRJN", "PRJE"])
    letter = st.sampled_from(list(string.ascii_uppercase))
    digits = st.integers(min_value=1, max_value=999999).map(str)
    return st.tuples(prefix, letter, digits).map(lambda t: t[0] + t[1] + t[2])


def st_biosample_id() -> st.SearchStrategy[str]:
    """Valid BioSample accession IDs."""
    prefix = st.sampled_from(["SAMN", "SAMD", "SAME"])
    digits = st.integers(min_value=1, max_value=99999999).map(str)
    return st.tuples(prefix, digits).map(lambda t: t[0] + t[1])


def st_sra_accession(letter: str = "R") -> st.SearchStrategy[str]:
    """Valid SRA accession IDs (e.g., SRR, DRR, ERR)."""
    prefix = st.sampled_from(["S", "D", "E"])
    digits = st.integers(min_value=1, max_value=999999).map(str)
    return st.tuples(prefix, digits).map(lambda t: t[0] + letter + t[1])


def st_sra_type() -> st.SearchStrategy[str]:
    """SRA accession Type (SUBMISSION, STUDY, EXPERIMENT, RUN, SAMPLE, ANALYSIS)."""
    return st.sampled_from(["SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"])


def st_timestamp_str() -> st.SearchStrategy[str]:
    """ISO 8601 timestamp string suitable for SRA Accessions TSV."""
    return st.datetimes(
        min_value=datetime(2000, 1, 1),
        max_value=datetime(2030, 12, 31),
    ).map(lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%SZ"))


def st_xml_dict() -> st.SearchStrategy[dict]:
    """XML parse result-like nested dict generation."""
    leaf = st.one_of(
        st.text(min_size=0, max_size=50),
        st.none(),
        st.integers(min_value=0, max_value=99999).map(str),
    )
    return st.fixed_dictionaries({
        "content": leaf,
        "attr": st.text(min_size=0, max_size=20),
    })
