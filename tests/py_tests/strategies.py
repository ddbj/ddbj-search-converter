"""Hypothesis custom strategies for PBT.

各 strategy は ``ddbj_search_converter/id_patterns.py`` の正規表現と
**整合する文字列のみ** を生成する。invalid 例の生成には
``st_invalid_accession_text(acc_type)`` を使う。
"""

import string
from datetime import datetime

from hypothesis import strategies as st

from ddbj_search_converter.dblink.db import AccessionType

ALL_ACCESSION_TYPES: list[AccessionType] = [
    "bioproject",
    "biosample",
    "gea",
    "geo",
    "humandbs",
    "insdc",
    "insdc-assembly",
    "insdc-master",
    "jga-dac",
    "jga-dataset",
    "jga-policy",
    "jga-study",
    "metabobank",
    "pubmed",
    "sra-analysis",
    "sra-experiment",
    "sra-run",
    "sra-sample",
    "sra-study",
    "sra-submission",
    "taxonomy",
]


def st_accession_type() -> st.SearchStrategy[AccessionType]:
    """AccessionType の Literal 値から sampled_from。"""
    return st.sampled_from(ALL_ACCESSION_TYPES)


# === positive ID strategies (match id_patterns.py の regex) ===


def st_bioproject_id() -> st.SearchStrategy[str]:
    """``^PRJ[DEN][A-Z]\\d+\\Z`` に整合する BioProject ID。"""
    prefix = st.sampled_from(["PRJD", "PRJE", "PRJN"])
    letter = st.sampled_from(list(string.ascii_uppercase))
    digits = st.integers(min_value=1, max_value=999999).map(str)
    return st.tuples(prefix, letter, digits).map(lambda t: t[0] + t[1] + t[2])


def st_biosample_id() -> st.SearchStrategy[str]:
    """``^SAM[NED](\\w)?\\d+\\Z`` に整合する BioSample ID。
    optional の `(\\w)?` を再現する (空 / 単語文字 1 文字)。
    """
    prefix = st.sampled_from(["SAMN", "SAMD", "SAME"])
    word_char = st.text(
        alphabet=string.ascii_letters + string.digits + "_",
        min_size=0,
        max_size=1,
    )
    digits = st.integers(min_value=1, max_value=99999999).map(str)
    return st.tuples(prefix, word_char, digits).map(lambda t: t[0] + t[1] + t[2])


def _st_sra_with_letter(third: str) -> st.SearchStrategy[str]:
    """``^[SDE]R{third}\\d+\\Z`` 形式の SRA accession を生成する。

    SRA accession の 1 文字目は origin (`S`/`D`/`E`)、2 文字目は固定 `R`、
    3 文字目が entity 種別 (`A`/`P`/`X`/`R`/`S`/`Z`)。
    """
    prefix = st.sampled_from(["S", "D", "E"])
    digits = st.integers(min_value=1, max_value=999999).map(str)
    return st.tuples(prefix, digits).map(lambda t: f"{t[0]}R{third}{t[1]}")


def st_sra_accession(third: str = "R") -> st.SearchStrategy[str]:
    """SRA accession (e.g. SRR, DRR, ERR)。``third`` は `A/P/X/R/S/Z` から。"""
    return _st_sra_with_letter(third)


def st_sra_submission() -> st.SearchStrategy[str]:
    """``^[SDE]RA\\d+\\Z``"""
    return _st_sra_with_letter("A")


def st_sra_study() -> st.SearchStrategy[str]:
    """``^[SDE]RP\\d+\\Z``"""
    return _st_sra_with_letter("P")


def st_sra_experiment() -> st.SearchStrategy[str]:
    """``^[SDE]RX\\d+\\Z``"""
    return _st_sra_with_letter("X")


def st_sra_run() -> st.SearchStrategy[str]:
    """``^[SDE]RR\\d+\\Z``"""
    return _st_sra_with_letter("R")


def st_sra_sample() -> st.SearchStrategy[str]:
    """``^[SDE]RS\\d+\\Z``"""
    return _st_sra_with_letter("S")


def st_sra_analysis() -> st.SearchStrategy[str]:
    """``^[SDE]RZ\\d+\\Z``"""
    return _st_sra_with_letter("Z")


def _st_jga_with_letter(letter: str) -> st.SearchStrategy[str]:
    digits = st.integers(min_value=1, max_value=999999).map(lambda n: f"{n:06d}")
    return digits.map(lambda d: f"JGA{letter}{d}")


def st_jga_study() -> st.SearchStrategy[str]:
    return _st_jga_with_letter("S")


def st_jga_dataset() -> st.SearchStrategy[str]:
    return _st_jga_with_letter("D")


def st_jga_dac() -> st.SearchStrategy[str]:
    return _st_jga_with_letter("C")


def st_jga_policy() -> st.SearchStrategy[str]:
    return _st_jga_with_letter("P")


def st_gea_id() -> st.SearchStrategy[str]:
    """``^E-GEAD-\\d+\\Z``"""
    return st.integers(min_value=1, max_value=99999).map(lambda n: f"E-GEAD-{n}")


def st_geo_id() -> st.SearchStrategy[str]:
    """``^GSE\\d+\\Z``"""
    return st.integers(min_value=1, max_value=999999).map(lambda n: f"GSE{n}")


def st_metabobank_id() -> st.SearchStrategy[str]:
    """``^MTBKS\\d+\\Z``"""
    return st.integers(min_value=1, max_value=9999).map(lambda n: f"MTBKS{n}")


def st_humandbs_id() -> st.SearchStrategy[str]:
    """``^hum\\d+\\Z``"""
    return st.integers(min_value=1, max_value=9999).map(lambda n: f"hum{n:04d}")


def st_pubmed_id() -> st.SearchStrategy[str]:
    """``^\\d+\\Z`` (pubmed と taxonomy 共有)。"""
    return st.integers(min_value=1, max_value=99999999).map(str)


def st_taxonomy_id() -> st.SearchStrategy[str]:
    return st_pubmed_id()


def st_insdc_assembly_id() -> st.SearchStrategy[str]:
    """``^GCA_[0-9]{9}(\\.[0-9]+)?\\Z``"""
    digits = st.integers(min_value=0, max_value=999_999_999).map(lambda n: f"{n:09d}")
    version = st.one_of(st.just(""), st.integers(min_value=1, max_value=99).map(lambda v: f".{v}"))
    return st.tuples(digits, version).map(lambda t: f"GCA_{t[0]}{t[1]}")


def st_insdc_master_id() -> st.SearchStrategy[str]:
    """``^([A-Z]0{5}|[A-Z]{2}0{6}|[A-Z]{4,6}0{8,10}|[A-J][A-Z]{2}0{5})\\Z``

    全 4 ブランチをカバーする。
    """
    upper = string.ascii_uppercase
    aj = "ABCDEFGHIJ"

    def _to_letters(n: int, length: int, alphabet: str = upper) -> str:
        result = ""
        for _ in range(length):
            n, r = divmod(n, len(alphabet))
            result = alphabet[r] + result
        return result

    branch_1 = st.integers(0, 25).map(lambda n: f"{upper[n]}{'0' * 5}")
    branch_2 = st.integers(0, 25 * 26).map(lambda n: f"{_to_letters(n, 2)}{'0' * 6}")
    branch_3 = st.tuples(
        st.integers(min_value=4, max_value=6),
        st.integers(min_value=8, max_value=10),
        st.integers(min_value=0),
    ).map(lambda t: f"{_to_letters(t[2], t[0])}{'0' * t[1]}")
    branch_4 = st.tuples(
        st.integers(0, 9),
        st.integers(0, 25 * 26 - 1),
    ).map(lambda t: f"{aj[t[0]]}{_to_letters(t[1], 2)}{'0' * 5}")
    return st.one_of(branch_1, branch_2, branch_3, branch_4)


# === negative strategies ===


def _all_id_patterns() -> dict[AccessionType, "object"]:
    # Imported lazily to avoid circular import at module load.
    from ddbj_search_converter.id_patterns import ID_PATTERN_MAP

    return ID_PATTERN_MAP  # type: ignore[return-value]


def st_invalid_accession_text(acc_type: AccessionType) -> st.SearchStrategy[str]:
    """指定 ``acc_type`` の regex に **一致しない** 文字列を生成する negative strategy。

    任意 text を生成し、id_patterns.py の regex に match しないものだけ通す。
    Hypothesis の filter で生成効率を保つため、印字可能文字に絞る。
    """
    patterns = _all_id_patterns()
    pattern = patterns.get(acc_type)
    if pattern is None:
        return st.text(min_size=0, max_size=20)

    base = st.text(
        alphabet=string.ascii_letters + string.digits + "_-.",
        min_size=0,
        max_size=20,
    )
    return base.filter(lambda s: not pattern.match(s))  # type: ignore[union-attr]


# === SRA accession type strategy ===


def st_sra_type() -> st.SearchStrategy[str]:
    """SRA accession Type (SUBMISSION, STUDY, EXPERIMENT, RUN, SAMPLE, ANALYSIS)."""
    return st.sampled_from(["SUBMISSION", "STUDY", "EXPERIMENT", "RUN", "SAMPLE", "ANALYSIS"])


def st_accession_like_text(*, min_size: int = 1, max_size: int = 20) -> st.SearchStrategy[str]:
    """Accession ID として通り得る文字 (Unicode Letter + Number カテゴリ) の文字列。

    `id_patterns.py` の regex に必ずしも match しない、より広めの空間。``\\W``
    で TSV を割らないだけの最低限の保証を持ち、TSV → DuckDB の roundtrip 系テスト
    に向く。pattern.match 検証用には別途 ``st_<type>_id()`` を使う。
    """
    return st.text(
        alphabet=st.characters(categories=["L", "N"]),
        min_size=min_size,
        max_size=max_size,
    )


def st_timestamp_str() -> st.SearchStrategy[str]:
    """ISO 8601 timestamp string suitable for SRA Accessions TSV."""
    return st.datetimes(
        min_value=datetime(2000, 1, 1),
        max_value=datetime(2030, 12, 31),
    ).map(lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%SZ"))


def st_xml_dict() -> st.SearchStrategy[dict]:  # type: ignore[type-arg]
    """XML parse result-like nested dict generation."""
    leaf = st.one_of(
        st.text(min_size=0, max_size=50),
        st.none(),
        st.integers(min_value=0, max_value=99999).map(str),
    )
    return st.fixed_dictionaries(
        {
            "content": leaf,
            "attr": st.text(min_size=0, max_size=20),
        }
    )
