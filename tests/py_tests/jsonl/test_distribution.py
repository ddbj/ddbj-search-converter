"""Tests for ddbj_search_converter.jsonl.distribution module."""

from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import DRA_PUBLIC_BASE_URL, SEARCH_BASE_URL
from ddbj_search_converter.jsonl.distribution import (
    make_bp_distribution,
    make_bs_distribution,
    make_jga_distribution,
    make_sra_distribution,
)


class TestMakeBpDistribution:
    """Tests for make_bp_distribution."""

    def test_returns_json_and_jsonld(self) -> None:
        dists = make_bp_distribution("PRJDB12345")

        assert len(dists) == 2
        assert dists[0].encodingFormat == "JSON"
        assert dists[1].encodingFormat == "JSON-LD"

    def test_json_url_pattern(self) -> None:
        dists = make_bp_distribution("PRJDB12345")

        assert dists[0].contentUrl == f"{SEARCH_BASE_URL}/search/entry/bioproject/PRJDB12345.json"

    def test_jsonld_url_pattern(self) -> None:
        dists = make_bp_distribution("PRJDB12345")

        assert dists[1].contentUrl == f"{SEARCH_BASE_URL}/search/entry/bioproject/PRJDB12345.jsonld"

    def test_type_is_data_download(self) -> None:
        dists = make_bp_distribution("PRJDB12345")

        for dist in dists:
            assert dist.type_ == "DataDownload"


class TestMakeBsDistribution:
    """Tests for make_bs_distribution."""

    def test_returns_json_and_jsonld(self) -> None:
        dists = make_bs_distribution("SAMD00000001")

        assert len(dists) == 2
        assert dists[0].encodingFormat == "JSON"
        assert dists[1].encodingFormat == "JSON-LD"

    def test_json_url_pattern(self) -> None:
        dists = make_bs_distribution("SAMD00000001")

        assert dists[0].contentUrl == f"{SEARCH_BASE_URL}/search/entry/biosample/SAMD00000001.json"

    def test_jsonld_url_pattern(self) -> None:
        dists = make_bs_distribution("SAMD00000001")

        assert dists[1].contentUrl == f"{SEARCH_BASE_URL}/search/entry/biosample/SAMD00000001.jsonld"


class TestMakeJgaDistribution:
    """Tests for make_jga_distribution."""

    def test_returns_json_and_jsonld(self) -> None:
        dists = make_jga_distribution("jga-study", "JGAS000001")

        assert len(dists) == 2
        assert dists[0].encodingFormat == "JSON"
        assert dists[1].encodingFormat == "JSON-LD"

    def test_url_uses_index_name(self) -> None:
        dists = make_jga_distribution("jga-dataset", "JGAD000001")

        assert dists[0].contentUrl == f"{SEARCH_BASE_URL}/search/entry/jga-dataset/JGAD000001.json"
        assert dists[1].contentUrl == f"{SEARCH_BASE_URL}/search/entry/jga-dataset/JGAD000001.jsonld"


class TestMakeSraDistribution:
    """Tests for make_sra_distribution."""

    def test_ncbi_sra_returns_json_and_jsonld_only(self) -> None:
        """NCBI SRA (is_dra=False) -> [JSON, JSON-LD] のみ。"""
        dists = make_sra_distribution(
            "sra-study",
            "SRP000001",
            is_dra=False,
            sra_type="study",
            submission="SRA000001",
        )

        assert len(dists) == 2
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD"]

    def test_dra_study_returns_json_jsonld_xml(self) -> None:
        """DRA study -> [JSON, JSON-LD, XML]。"""
        dists = make_sra_distribution(
            "sra-study",
            "DRP000001",
            is_dra=True,
            sra_type="study",
            submission="DRA000001",
        )

        assert len(dists) == 3
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML"]

    def test_dra_xml_url_pattern(self) -> None:
        """DRA XML の URL パターンが正しい。"""
        dists = make_sra_distribution(
            "sra-study",
            "DRP000001",
            is_dra=True,
            sra_type="study",
            submission="DRA000001",
        )
        xml_dist = dists[2]

        assert xml_dist.contentUrl == f"{DRA_PUBLIC_BASE_URL}/fastq/DRA000/DRA000001/DRA000001.study.xml"

    def test_dra_run_with_fastq_and_sra(self) -> None:
        """DRA run + FASTQ + SRA -> [JSON, JSON-LD, XML, FASTQ, SRA]。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_dra=True,
            sra_type="run",
            submission="DRA000001",
            experiment="DRX000001",
            fastq_dirs={"DRX000001"},
            sra_file_runs={"DRR000001"},
        )

        assert len(dists) == 5
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML", "FASTQ", "SRA"]

    def test_dra_run_fastq_url_pattern(self) -> None:
        """FASTQ URL パターンが正しい。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_dra=True,
            sra_type="run",
            submission="DRA000001",
            experiment="DRX000001",
            fastq_dirs={"DRX000001"},
        )
        fastq_dist = [d for d in dists if d.encodingFormat == "FASTQ"]

        assert len(fastq_dist) == 1
        assert fastq_dist[0].contentUrl == f"{DRA_PUBLIC_BASE_URL}/fastq/DRA000/DRA000001/DRX000001/"

    def test_dra_run_sra_url_pattern(self) -> None:
        """SRA URL パターンが正しい。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_dra=True,
            sra_type="run",
            submission="DRA000001",
            experiment="DRX000001",
            sra_file_runs={"DRR000001"},
        )
        sra_dist = [d for d in dists if d.encodingFormat == "SRA"]

        assert len(sra_dist) == 1
        expected = f"{DRA_PUBLIC_BASE_URL}/sra/ByExp/sra/DRX/DRX000/DRX000001/DRR000001/DRR000001.sra"
        assert sra_dist[0].contentUrl == expected

    def test_dra_run_no_fastq(self) -> None:
        """FASTQ ディレクトリなし -> FASTQ distribution なし。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_dra=True,
            sra_type="run",
            submission="DRA000001",
            experiment="DRX000001",
            fastq_dirs=set(),
            sra_file_runs={"DRR000001"},
        )

        formats = [d.encodingFormat for d in dists]
        assert "FASTQ" not in formats
        assert "SRA" in formats

    def test_dra_run_experiment_none(self) -> None:
        """experiment が None -> FASTQ/SRA distribution なし。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_dra=True,
            sra_type="run",
            submission="DRA000001",
            experiment=None,
            fastq_dirs={"DRX000001"},
            sra_file_runs={"DRR000001"},
        )

        formats = [d.encodingFormat for d in dists]
        assert "FASTQ" not in formats
        assert "SRA" not in formats
        assert len(dists) == 3  # JSON, JSON-LD, XML

    def test_dra_non_run_type_no_fastq_sra(self) -> None:
        """DRA non-run タイプ -> FASTQ/SRA distribution なし。"""
        for sra_type in ["submission", "study", "experiment", "sample", "analysis"]:
            entry_type = f"sra-{sra_type}"
            dists = make_sra_distribution(
                entry_type,
                f"DR{sra_type[0].upper()}000001",
                is_dra=True,
                sra_type=sra_type,
                submission="DRA000001",
            )

            formats = [d.encodingFormat for d in dists]
            assert "FASTQ" not in formats, f"FASTQ should not be in {sra_type}"
            assert "SRA" not in formats, f"SRA should not be in {sra_type}"


# accession 文字列戦略
_accession_st = st.text(
    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
    min_size=6,
    max_size=12,
)


class TestDistributionPBT:
    """Property-based tests for distribution helpers."""

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_bp_distribution_always_has_two_items(self, accession) -> None:
        dists = make_bp_distribution(accession)

        assert len(dists) == 2

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_bs_distribution_always_has_two_items(self, accession) -> None:
        dists = make_bs_distribution(accession)

        assert len(dists) == 2

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_ncbi_sra_distribution_always_has_two_items(self, accession) -> None:
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_dra=False,
            sra_type="run",
            submission="SRA000001",
        )

        assert len(dists) == 2

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_dra_sra_distribution_has_at_least_three_items(self, accession) -> None:
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_dra=True,
            sra_type="run",
            submission="DRA000001",
        )

        assert len(dists) >= 3

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_all_urls_are_https(self, accession) -> None:
        dists = make_bp_distribution(accession)

        for dist in dists:
            assert dist.contentUrl.startswith("https://")

    @given(
        accession=st.from_regex(r"DRR[0-9]{6}", fullmatch=True),
        experiment=st.from_regex(r"DRX[0-9]{6}", fullmatch=True),
        submission=st.from_regex(r"DRA[0-9]{6}", fullmatch=True),
    )
    @settings(max_examples=50)
    def test_dra_run_with_all_files_has_five_items(self, accession, experiment, submission) -> None:
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_dra=True,
            sra_type="run",
            submission=submission,
            experiment=experiment,
            fastq_dirs={experiment},
            sra_file_runs={accession},
        )

        assert len(dists) == 5
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML", "FASTQ", "SRA"]
