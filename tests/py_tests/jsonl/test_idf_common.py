"""Tests for ddbj_search_converter.jsonl.idf_common module."""

from pathlib import Path

from ddbj_search_converter.jsonl.idf_common import (
    parse_idf,
    parse_pubmed_doi_publications,
    parse_submitter_affiliations,
)


class TestParseIdf:
    """parse_idf: tab 区切り IDF を tag -> values dict に展開する。"""

    def test_basic_parse(self, tmp_path: Path) -> None:
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(
            "Investigation Title\tMy Title\nPerson Affiliation\tOrg A\tOrg B\n",
            encoding="utf-8",
        )
        result = parse_idf(idf_path)
        assert result == {"Investigation Title": ["My Title"], "Person Affiliation": ["Org A", "Org B"]}

    def test_trailing_empty_values_removed(self, tmp_path: Path) -> None:
        """末尾の空 value は除去する (MTBKS102 Line 16 'PubMed ID' 等のケース)."""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text("Title\tMy Title\t\t\nOther\tA\tB\t\n", encoding="utf-8")
        result = parse_idf(idf_path)
        assert result["Title"] == ["My Title"]
        assert result["Other"] == ["A", "B"]

    def test_preserves_intermediate_empty_values(self, tmp_path: Path) -> None:
        """途中の空 value は保持 (MTBKS102 Line 22 'Protocol Parameters' 等のケース)."""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text("Protocol Parameters\t\tPost extraction\tSecond\n", encoding="utf-8")
        result = parse_idf(idf_path)
        assert result["Protocol Parameters"] == ["", "Post extraction", "Second"]

    def test_empty_lines_skipped(self, tmp_path: Path) -> None:
        """空行 (MTBKS102 Line 5, 9, 15 等) は skip する。"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text("Title\tA\n\n\nOther\tB\n\n", encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Title": ["A"], "Other": ["B"]}

    def test_tag_only_line_yields_empty_list(self, tmp_path: Path) -> None:
        """値なし行 (MTBKS102 Line 7 'Experimental Factor Name' 等) は空 list。"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text("Title\n", encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Title": []}

    def test_whitespace_around_values_stripped(self, tmp_path: Path) -> None:
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text("Title\t  Padded value  \n", encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Title": ["Padded value"]}

    def test_gea_fixture_e_gead_1005(self) -> None:
        """実 fixture (E-GEAD-1005) で基本フィールドが取れること。"""
        idf_path = (
            Path(__file__).parent.parent.parent
            / "fixtures"
            / "usr"
            / "local"
            / "resources"
            / "gea"
            / "experiment"
            / "E-GEAD-1000"
            / "E-GEAD-1005"
            / "E-GEAD-1005.idf.txt"
        )
        result = parse_idf(idf_path)
        assert result["Investigation Title"] == ["RT2ProfilerTM PCR Array-Human common cytokines (CBX140)"]
        assert result["Person Affiliation"] == ["Kyushu University"]
        assert result["Person Roles"] == ["submitter"]
        assert result["PubMed ID"] == ["21187441"]
        assert result["Comment[AEExperimentType]"] == ["transcription profiling by array"]
        assert result["Public Release Date"] == ["2025-01-31"]
        assert result["Comment[Last Update Date]"] == ["2025-01-31"]

    def test_metabobank_fixture_mtbks102(self) -> None:
        """実 fixture (MTBKS102) で基本フィールドが取れること。"""
        idf_path = (
            Path(__file__).parent.parent.parent
            / "fixtures"
            / "usr"
            / "local"
            / "shared_data"
            / "metabobank"
            / "study"
            / "MTBKS102"
            / "MTBKS102.idf.txt"
        )
        result = parse_idf(idf_path)
        assert result["Study Title"] == ["Arabidopsis thaliana leaf metabolite analysis"]
        assert result["Person Affiliation"] == ["Kazusa DNA Research Institute"]
        assert result["Person Roles"] == ["submitter"]
        assert result["Comment[Study type]"] == ["untargeted metabolite profiling"]
        assert result["Comment[Experiment type]"] == [
            "liquid chromatography-mass spectrometry",
            "fourier transform ion cyclotron resonance mass spectrometry",
        ]
        assert result["Comment[Submission type]"] == ["LC-DAD-MS"]
        assert result["Comment[Submission Date]"] == ["2022-05-22"]
        # 空行を含む "PubMed ID" は値なし (空 list)
        assert result.get("PubMed ID") == []

    def test_quoted_value_with_newline_kept_as_single_value(self, tmp_path: Path) -> None:
        """MAGE-TAB 仕様: double quote で囲まれた値内の改行は 1 value として保持される。"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text('Title\t"line1\nline2"\n', encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Title": ["line1\nline2"]}

    def test_quoted_value_with_tab_kept_as_single_value(self, tmp_path: Path) -> None:
        """quote 内の tab も区切り扱いされず 1 value として保持される。"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text('Title\t"a\tb"\n', encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Title": ["a\tb"]}

    def test_quoted_value_with_blank_line(self, tmp_path: Path) -> None:
        """quote 内の空行 (MTBKS264 Protocol Description 風のケース) も保持される。"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text('Title\t"l1\n\nl3"\n', encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Title": ["l1\n\nl3"]}

    def test_quoted_multi_value_boundary(self, tmp_path: Path) -> None:
        """quote 境界を挟んだ tab は値区切りとして正しく認識される。"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text('Tag\t"val with\nnewline"\t"next val"\n', encoding="utf-8")
        result = parse_idf(idf_path)
        assert result == {"Tag": ["val with\nnewline", "next val"]}

    def test_metabobank_fixture_mtbks264_no_spurious_keys(self) -> None:
        """MTBKS264 fixture で quote 対応により改行 bleed の spurious key が消える。"""
        idf_path = (
            Path(__file__).parent.parent.parent
            / "fixtures"
            / "usr"
            / "local"
            / "shared_data"
            / "metabobank"
            / "study"
            / "MTBKS264"
            / "MTBKS264.idf.txt"
        )
        result = parse_idf(idf_path)
        # quote 内改行 bleed で本来 value が key になっていたのが消える
        assert not any(k.startswith("Interested individuals") for k in result)
        assert not any(k.startswith("Stool samples") for k in result)
        # bleed 解消前は 58 keys、解消後は 33 keys (正当な IDF tag のみ)。
        # 今後 fixture が差し替わっても「bleed が再発すれば key 数が跳ね上がる」ことを検知する上限。
        assert len(result) <= 40

    def test_metabobank_fixture_mtbks264_protocol_description_multi_value(self) -> None:
        """MTBKS264 の Protocol Description は quote で囲まれた複数行 value の list。"""
        idf_path = (
            Path(__file__).parent.parent.parent
            / "fixtures"
            / "usr"
            / "local"
            / "shared_data"
            / "metabobank"
            / "study"
            / "MTBKS264"
            / "MTBKS264.idf.txt"
        )
        result = parse_idf(idf_path)
        descriptions = result["Protocol Description"]
        assert len(descriptions) >= 6
        assert descriptions[0].startswith("Volunteers were recruited")


class TestParseSubmitterAffiliations:
    """parse_submitter_affiliations: Person Affiliation から Organization list を構築する。"""

    def test_unique_affiliations(self) -> None:
        idf = {"Person Affiliation": ["Tokyo Univ", "Tokyo Univ", "Osaka Univ"]}
        orgs = parse_submitter_affiliations(idf)
        assert [o.name for o in orgs] == ["Tokyo Univ", "Osaka Univ"]
        assert all(o.role == "submitter" for o in orgs)

    def test_empty_values_skipped(self) -> None:
        idf = {"Person Affiliation": ["", "Tokyo Univ", "   ", "Osaka Univ"]}
        orgs = parse_submitter_affiliations(idf)
        assert [o.name for o in orgs] == ["Tokyo Univ", "Osaka Univ"]

    def test_missing_key_returns_empty(self) -> None:
        assert parse_submitter_affiliations({}) == []

    def test_empty_list_returns_empty(self) -> None:
        assert parse_submitter_affiliations({"Person Affiliation": []}) == []

    def test_all_empty_affiliations_returns_empty(self) -> None:
        """'submitter role は付くが Affiliation 空' のケース。"""
        idf = {"Person Affiliation": ["", "", ""]}
        assert parse_submitter_affiliations(idf) == []

    def test_role_is_submitter_by_default(self) -> None:
        idf = {"Person Affiliation": ["Some Lab"]}
        orgs = parse_submitter_affiliations(idf)
        assert orgs[0].role == "submitter"


class TestParsePubmedDoiPublications:
    """parse_pubmed_doi_publications: PubMed ID / DOI を別 entry で Publication list 化 (案 a)."""

    def test_pubmed_only(self) -> None:
        idf = {"PubMed ID": ["12345", "67890"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert [p.id_ for p in pubs] == ["12345", "67890"]
        assert all(p.dbType == "pubmed" for p in pubs)
        assert pubs[0].url == "https://pubmed.ncbi.nlm.nih.gov/12345/"
        assert pubs[1].url == "https://pubmed.ncbi.nlm.nih.gov/67890/"

    def test_doi_only(self) -> None:
        idf = {"Publication DOI": ["10.1038/nature12345"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert len(pubs) == 1
        assert pubs[0].id_ == "10.1038/nature12345"
        assert pubs[0].dbType == "doi"
        assert pubs[0].url == "https://doi.org/10.1038/nature12345"

    def test_both_pubmed_and_doi_separate_entries(self) -> None:
        """案 a: 同一 index 対応は無視、別 entry として list 化。"""
        idf = {"PubMed ID": ["12345"], "Publication DOI": ["10.1038/foo"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert len(pubs) == 2
        assert pubs[0].dbType == "pubmed"
        assert pubs[0].id_ == "12345"
        assert pubs[1].dbType == "doi"
        assert pubs[1].id_ == "10.1038/foo"

    def test_empty_values_skipped(self) -> None:
        idf = {"PubMed ID": ["", "12345", "  "]}
        pubs = parse_pubmed_doi_publications(idf)
        assert [p.id_ for p in pubs] == ["12345"]

    def test_missing_keys_returns_empty(self) -> None:
        assert parse_pubmed_doi_publications({}) == []

    def test_empty_values_for_both_returns_empty(self) -> None:
        idf = {"PubMed ID": [""], "Publication DOI": [""]}
        assert parse_pubmed_doi_publications(idf) == []

    def test_doi_prefix_uppercase_stripped(self) -> None:
        """`DOI: ` prefix (大文字、コロン後空白) が strip される。"""
        idf = {"Publication DOI": ["DOI: 10.5511/foo"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert len(pubs) == 1
        assert pubs[0].id_ == "10.5511/foo"
        assert pubs[0].url == "https://doi.org/10.5511/foo"
        assert pubs[0].dbType == "doi"

    def test_doi_prefix_lowercase_stripped(self) -> None:
        """`doi: ` prefix (小文字、case-insensitive) も strip される。"""
        idf = {"Publication DOI": ["doi: 10.1038/bar"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1038/bar"
        assert pubs[0].url == "https://doi.org/10.1038/bar"

    def test_doi_prefix_no_space(self) -> None:
        """コロン後空白なしの `DOI:` prefix も strip される。"""
        idf = {"Publication DOI": ["DOI:10.1/x"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1/x"
        assert pubs[0].url == "https://doi.org/10.1/x"

    def test_doi_url_https_stripped(self) -> None:
        """`https://doi.org/` URL prefix が strip され、url は doi.org 版に再構築される。"""
        idf = {"Publication DOI": ["https://doi.org/10.1/x"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1/x"
        assert pubs[0].url == "https://doi.org/10.1/x"

    def test_doi_url_http_stripped(self) -> None:
        """`http://doi.org/` も strip され、url は https 版で再構築される (https 優先)。"""
        idf = {"Publication DOI": ["http://doi.org/10.1/x"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1/x"
        assert pubs[0].url == "https://doi.org/10.1/x"

    def test_doi_url_dx_stripped(self) -> None:
        """旧 resolver `https://dx.doi.org/` も strip される。"""
        idf = {"Publication DOI": ["https://dx.doi.org/10.1/x"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1/x"
        assert pubs[0].url == "https://doi.org/10.1/x"

    def test_doi_protocol_less_stripped(self) -> None:
        """protocol なしの `doi.org/` prefix も strip される (MTBKS 実データで観測)。"""
        idf = {"Publication DOI": ["doi.org/10.1038/s41597-025-04518-7"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1038/s41597-025-04518-7"
        assert pubs[0].url == "https://doi.org/10.1038/s41597-025-04518-7"

    def test_doi_already_normalized_unchanged(self) -> None:
        """正規の DOI (`10.xxx/...`) は変更されない。"""
        idf = {"Publication DOI": ["10.1038/nature12345"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1038/nature12345"
        assert pubs[0].url == "https://doi.org/10.1038/nature12345"

    def test_doi_non_doi_url_marked_as_other(self) -> None:
        """DOI 以外の URL (SSRN 等) は生値のまま `id` / `url` に入り、`dbType="other"` に倒す。

        DOI 形式 (`10.xxx/...`) でないのに `dbType="doi"` にすると、
        publication.dbType=doi と URL のセマンティクスが矛盾するのを防ぐ。
        """
        idf = {"Publication DOI": ["http://ssrn.com/abstract=4137686"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "http://ssrn.com/abstract=4137686"
        assert pubs[0].url == "http://ssrn.com/abstract=4137686"
        assert pubs[0].dbType == "other"

    def test_doi_https_non_doi_url_marked_as_other(self) -> None:
        """https:// で始まる非 DOI URL も `dbType="other"`。"""
        idf = {"Publication DOI": ["https://example.com/paper"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].url == "https://example.com/paper"
        assert pubs[0].dbType == "other"

    def test_doi_formatted_value_marked_as_doi(self) -> None:
        """正規の DOI 形式 (`10.xxx/...`) は `dbType="doi"` を維持する。"""
        idf = {"Publication DOI": ["10.1038/nature12345"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].dbType == "doi"

    def test_doi_trailing_dot_preserved(self) -> None:
        """末尾句読点タイポは元データ保持方針で strip しない。"""
        idf = {"Publication DOI": ["10.1073/pnas.2311372120."]}
        pubs = parse_pubmed_doi_publications(idf)
        assert pubs[0].id_ == "10.1073/pnas.2311372120."
        assert pubs[0].url == "https://doi.org/10.1073/pnas.2311372120."

    def test_doi_case_insensitive_variants(self) -> None:
        """`DOI:` / `doi:` / `Doi:` 全て case-insensitive で同じ結果になる。"""
        idf = {"Publication DOI": ["Doi: 10", "DOI:10", "doi:10"]}
        pubs = parse_pubmed_doi_publications(idf)
        assert [p.id_ for p in pubs] == ["10", "10", "10"]
