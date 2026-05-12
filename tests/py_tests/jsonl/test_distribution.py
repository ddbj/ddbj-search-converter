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

    def test_dra_study_returns_json_jsonld_xml(self) -> None:
        """DRA study -> [JSON, JSON-LD, XML]。"""
        dists = make_sra_distribution(
            "sra-study",
            "DRP000001",
            is_ddbj_origin=True,
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
            is_ddbj_origin=True,
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
            is_ddbj_origin=True,
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
            is_ddbj_origin=True,
            sra_type="run",
            submission="DRA000001",
            experiment="DRX000001",
            fastq_dirs={"DRX000001"},
        )
        fastq_dist = [d for d in dists if d.encodingFormat == "FASTQ"]

        assert len(fastq_dist) == 1
        assert fastq_dist[0].contentUrl == f"{DRA_PUBLIC_BASE_URL}/fastq/DRA000/DRA000001/DRX000001/"

    def test_dra_run_sra_url_pattern(self) -> None:
        """DRA SRA URL パターンが正しい。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_ddbj_origin=True,
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
        """DRA で FASTQ ディレクトリなし -> FASTQ distribution なし。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_ddbj_origin=True,
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
        """DRA で experiment が None -> FASTQ/SRA distribution なし。"""
        dists = make_sra_distribution(
            "sra-run",
            "DRR000001",
            is_ddbj_origin=True,
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
                is_ddbj_origin=True,
                sra_type=sra_type,
                submission="DRA000001",
            )

            formats = [d.encodingFormat for d in dists]
            assert "FASTQ" not in formats, f"FASTQ should not be in {sra_type}"
            assert "SRA" not in formats, f"SRA should not be in {sra_type}"

    def test_ncbi_study_returns_json_jsonld_xml(self) -> None:
        """NCBI study (is_ddbj_origin=False) -> [JSON, JSON-LD, XML]。"""
        dists = make_sra_distribution(
            "sra-study",
            "SRP000001",
            is_ddbj_origin=False,
            sra_type="study",
            submission="SRA000001",
        )

        assert len(dists) == 3
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML"]

    def test_ncbi_xml_url_pattern(self) -> None:
        """NCBI XML の URL パターンが DRA と同じテンプレート (submission 配下のミラー)。"""
        dists = make_sra_distribution(
            "sra-study",
            "SRP000001",
            is_ddbj_origin=False,
            sra_type="study",
            submission="SRA000001",
        )
        xml_dist = next(d for d in dists if d.encodingFormat == "XML")

        assert xml_dist.contentUrl == f"{DRA_PUBLIC_BASE_URL}/fastq/SRA000/SRA000001/SRA000001.study.xml"

    def test_ebi_submission_xml_url_pattern(self) -> None:
        """EBI submission の XML URL も同じテンプレートで生成される。"""
        dists = make_sra_distribution(
            "sra-submission",
            "ERA000001",
            is_ddbj_origin=False,
            sra_type="submission",
            submission="ERA000001",
        )
        xml_dist = next(d for d in dists if d.encodingFormat == "XML")

        assert xml_dist.contentUrl == f"{DRA_PUBLIC_BASE_URL}/fastq/ERA000/ERA000001/ERA000001.submission.xml"

    def test_ncbi_run_returns_sra_with_mirror_path(self) -> None:
        """NCBI run のミラー SRA URL がユーザー提供例と一致する。

        提供例: https://ddbj.nig.ac.jp/public/ddbj_database/dra/sralite/ByExp/litesra/SRX/SRX004/SRX004004/SRR015417/SRR015417.sra
        """
        dists = make_sra_distribution(
            "sra-run",
            "SRR015417",
            is_ddbj_origin=False,
            sra_type="run",
            submission="SRA015417",
            experiment="SRX004004",
        )

        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML", "SRA"]

        sra_dist = next(d for d in dists if d.encodingFormat == "SRA")
        expected = f"{DRA_PUBLIC_BASE_URL}/sralite/ByExp/litesra/SRX/SRX004/SRX004004/SRR015417/SRR015417.sra"
        assert sra_dist.contentUrl == expected

    def test_ebi_run_returns_sra_with_mirror_path(self) -> None:
        """EBI run も同じミラーテンプレート (ERX/ prefix) で生成される。"""
        dists = make_sra_distribution(
            "sra-run",
            "ERR000001",
            is_ddbj_origin=False,
            sra_type="run",
            submission="ERA000001",
            experiment="ERX000001",
        )
        sra_dist = next(d for d in dists if d.encodingFormat == "SRA")

        expected = f"{DRA_PUBLIC_BASE_URL}/sralite/ByExp/litesra/ERX/ERX000/ERX000001/ERR000001/ERR000001.sra"
        assert sra_dist.contentUrl == expected

    def test_ncbi_run_no_fastq_even_when_fastq_dirs_provided(self) -> None:
        """他極では fastq_dirs を渡しても FASTQ distribution は生成されない (FASTQ は自極のみ)。"""
        dists = make_sra_distribution(
            "sra-run",
            "SRR000001",
            is_ddbj_origin=False,
            sra_type="run",
            submission="SRA000001",
            experiment="SRX000001",
            fastq_dirs={"SRX000001"},
        )

        formats = [d.encodingFormat for d in dists]
        assert "FASTQ" not in formats

    def test_ncbi_run_sra_unconditional(self) -> None:
        """他極では sra_file_runs=None でも SRA distribution が生成される (機械生成・404 容認)。"""
        dists = make_sra_distribution(
            "sra-run",
            "SRR000001",
            is_ddbj_origin=False,
            sra_type="run",
            submission="SRA000001",
            experiment="SRX000001",
            sra_file_runs=None,
        )

        formats = [d.encodingFormat for d in dists]
        assert "SRA" in formats

    def test_ncbi_run_no_experiment_no_sra(self) -> None:
        """他極で experiment が None なら SRA distribution は出ない (URL を組み立てられない)。"""
        dists = make_sra_distribution(
            "sra-run",
            "SRR000001",
            is_ddbj_origin=False,
            sra_type="run",
            submission="SRA000001",
            experiment=None,
        )

        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML"]


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
    def test_bp_distribution_always_has_two_items(self, accession: str) -> None:
        dists = make_bp_distribution(accession)

        assert len(dists) == 2

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_bs_distribution_always_has_two_items(self, accession: str) -> None:
        dists = make_bs_distribution(accession)

        assert len(dists) == 2

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_dra_sra_distribution_has_at_least_three_items(self, accession: str) -> None:
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_ddbj_origin=True,
            sra_type="run",
            submission="DRA000001",
        )

        assert len(dists) >= 3

    @given(accession=_accession_st)
    @settings(max_examples=50)
    def test_all_urls_are_https(self, accession: str) -> None:
        dists = make_bp_distribution(accession)

        for dist in dists:
            assert dist.contentUrl.startswith("https://")

    @given(
        accession=st.from_regex(r"DRR[0-9]{6}", fullmatch=True),
        experiment=st.from_regex(r"DRX[0-9]{6}", fullmatch=True),
        submission=st.from_regex(r"DRA[0-9]{6}", fullmatch=True),
    )
    @settings(max_examples=50)
    def test_dra_run_with_all_files_has_five_items(self, accession: str, experiment: str, submission: str) -> None:
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_ddbj_origin=True,
            sra_type="run",
            submission=submission,
            experiment=experiment,
            fastq_dirs={experiment},
            sra_file_runs={accession},
        )

        assert len(dists) == 5
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML", "FASTQ", "SRA"]

    @given(
        accession=st.from_regex(r"[SE]RP[0-9]{6}", fullmatch=True),
        submission=st.from_regex(r"[SE]RA[0-9]{6}", fullmatch=True),
    )
    @settings(max_examples=50)
    def test_other_origin_study_has_three_items(self, accession: str, submission: str) -> None:
        """他極 study は常に [JSON, JSON-LD, XML] の 3 件。"""
        dists = make_sra_distribution(
            "sra-study",
            accession,
            is_ddbj_origin=False,
            sra_type="study",
            submission=submission,
        )

        assert len(dists) == 3
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML"]

    @given(
        accession=st.from_regex(r"[SE]RR[0-9]{6}", fullmatch=True),
        experiment=st.from_regex(r"[SE]RX[0-9]{6}", fullmatch=True),
        submission=st.from_regex(r"[SE]RA[0-9]{6}", fullmatch=True),
    )
    @settings(max_examples=50)
    def test_other_origin_run_has_four_items(self, accession: str, experiment: str, submission: str) -> None:
        """他極 run + experiment は常に [JSON, JSON-LD, XML, SRA] の 4 件。"""
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_ddbj_origin=False,
            sra_type="run",
            submission=submission,
            experiment=experiment,
        )

        assert len(dists) == 4
        formats = [d.encodingFormat for d in dists]
        assert formats == ["JSON", "JSON-LD", "XML", "SRA"]

    @given(
        accession=st.from_regex(r"[SE]RR[0-9]{6}", fullmatch=True),
        experiment=st.from_regex(r"[SE]RX[0-9]{6}", fullmatch=True),
        submission=st.from_regex(r"[SE]RA[0-9]{6}", fullmatch=True),
    )
    @settings(max_examples=50)
    def test_mirrored_sra_url_contains_path_components(self, accession: str, experiment: str, submission: str) -> None:
        """他極 SRA URL は path に experiment[:3], experiment[:6], experiment, run accession を含む。"""
        dists = make_sra_distribution(
            "sra-run",
            accession,
            is_ddbj_origin=False,
            sra_type="run",
            submission=submission,
            experiment=experiment,
        )
        sra_dist = next(d for d in dists if d.encodingFormat == "SRA")

        assert f"/{experiment[:3]}/" in sra_dist.contentUrl
        assert f"/{experiment[:6]}/" in sra_dist.contentUrl
        assert f"/{experiment}/" in sra_dist.contentUrl
        assert f"/{accession}/{accession}.sra" in sra_dist.contentUrl
