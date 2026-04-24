"""Tests for ddbj_search_converter.dblink.idf_sdrf module."""

from pathlib import Path

from ddbj_search_converter.dblink.idf_sdrf import (
    IdfSdrfResult,
    _classify_related_study,
    parse_idf_file,
    parse_sdrf_file,
    process_idf_sdrf_dir,
)


class TestParseIdfFile:
    """Tests for parse_idf_file function."""

    def test_extracts_bioproject_id(self, tmp_path: Path) -> None:
        """BioProject ID を正しく抽出する。"""
        idf_content = """Comment[GEAAccession]\tE-GEAD-291
MAGE-TAB Version\t1.1
Investigation Title\tTest Investigation
Comment[BioProject]\tPRJDB7770
Public Release Date\t2022-12-08
"""
        idf_path = tmp_path / "E-GEAD-291.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        bioproject, related_studies = parse_idf_file(idf_path)
        assert bioproject == "PRJDB7770"
        assert related_studies == []

    def test_returns_none_when_no_bioproject(self, tmp_path: Path) -> None:
        """BioProject 行がない場合は bioproject=None を返す。"""
        idf_content = """Comment[GEAAccession]\tE-GEAD-100
MAGE-TAB Version\t1.1
Investigation Title\tTest
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        bioproject, related_studies = parse_idf_file(idf_path)
        assert bioproject is None
        assert related_studies == []

    def test_returns_none_when_bioproject_empty(self, tmp_path: Path) -> None:
        """BioProject の値が空の場合は bioproject=None を返す。"""
        idf_content = """Comment[BioProject]\t
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        bioproject, related_studies = parse_idf_file(idf_path)
        assert bioproject is None
        assert related_studies == []

    def test_extracts_single_related_study(self, tmp_path: Path) -> None:
        """Comment[Related study] の単一値を抽出する。"""
        idf_content = """Comment[BioProject]\tPRJDB1234
Comment[Related study]\tJGA:JGAS000001
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        bioproject, related_studies = parse_idf_file(idf_path)
        assert bioproject == "PRJDB1234"
        assert related_studies == ["JGA:JGAS000001"]

    def test_extracts_multiple_tab_separated_related_studies(self, tmp_path: Path) -> None:
        """Comment[Related study] の同一行 tab-separated 複数値を全て抽出する。"""
        idf_content = """Comment[Related study]\tJGA:JGAS000001\tNBDC:hum0001\tMetabolonote:SE1
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        _, related_studies = parse_idf_file(idf_path)
        assert related_studies == ["JGA:JGAS000001", "NBDC:hum0001", "Metabolonote:SE1"]

    def test_skips_empty_related_study_values(self, tmp_path: Path) -> None:
        """Comment[Related study] の空値 cell は skip する。"""
        idf_content = """Comment[Related study]\tJGA:JGAS000001\t\tNBDC:hum0001
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        _, related_studies = parse_idf_file(idf_path)
        assert related_studies == ["JGA:JGAS000001", "NBDC:hum0001"]

    def test_strips_whitespace_in_related_study(self, tmp_path: Path) -> None:
        """Comment[Related study] の cell 前後空白を strip する。"""
        idf_content = """Comment[Related study]\t  JGA:JGAS000001  \t NBDC:hum0001
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        _, related_studies = parse_idf_file(idf_path)
        assert related_studies == ["JGA:JGAS000001", "NBDC:hum0001"]

    def test_returns_empty_when_no_related_study_tag(self, tmp_path: Path) -> None:
        """Comment[Related study] tag 自体が無い場合は空 list を返す。"""
        idf_content = """Comment[BioProject]\tPRJDB1234
Investigation Title\tTest
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        _, related_studies = parse_idf_file(idf_path)
        assert related_studies == []

    def test_returns_empty_when_related_study_value_empty(self, tmp_path: Path) -> None:
        """Comment[Related study] tag はあるが値が空の場合は空 list を返す (MTBKS208 等の実データ)。"""
        idf_content = """Comment[Related study]\t
"""
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        _, related_studies = parse_idf_file(idf_path)
        assert related_studies == []

    def test_quoted_value_with_tab_kept_as_single_value(self, tmp_path: Path) -> None:
        """MAGE-TAB 仕様で quote 囲み値の中の tab はリテラル保持し、1 値として扱う。"""
        idf_content = 'Comment[BioProject]\t"PRJDB1234\tnote"\n'
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        bioproject, _ = parse_idf_file(idf_path)
        assert bioproject == "PRJDB1234\tnote"

    def test_quoted_value_with_newline_kept_as_single_value(self, tmp_path: Path) -> None:
        """MAGE-TAB 仕様で quote 囲み値の中の newline はリテラル保持し、行分割しない。"""
        idf_content = 'Comment[Related study]\t"JGA:JGAS000001\nline2"\tNBDC:hum0001\n'
        idf_path = tmp_path / "test.idf.txt"
        idf_path.write_text(idf_content, encoding="utf-8")

        _, related_studies = parse_idf_file(idf_path)
        assert related_studies == ["JGA:JGAS000001\nline2", "NBDC:hum0001"]


class TestParseSdrfFile:
    """Tests for parse_sdrf_file function."""

    def test_extracts_biosample_ids(self, tmp_path: Path) -> None:
        """BioSample ID を正しく抽出する。"""
        sdrf_content = """Source Name\tCharacteristics[organism]\tComment[BioSample]\tComment[description]
AF\tHomo sapiens\tSAMD00093430\tSample 1
F1\tHomo sapiens\tSAMD00093431\tSample 2
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == {"SAMD00093430", "SAMD00093431"}
        assert result["sra_run"] == set()
        assert result["sra_experiment"] == set()

    def test_returns_empty_when_no_biosample_column(self, tmp_path: Path) -> None:
        """Comment[BioSample] カラムがない場合は空 set を返す。"""
        sdrf_content = """Source Name\tCharacteristics[organism]\tComment[description]
AF\tHomo sapiens\tSample 1
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == set()
        assert result["sra_run"] == set()
        assert result["sra_experiment"] == set()

    def test_skips_empty_biosample_values(self, tmp_path: Path) -> None:
        """BioSample の空の値はスキップする。"""
        sdrf_content = """Source Name\tComment[BioSample]
AF\tSAMD00001
F1\t
G2\tSAMD00002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == {"SAMD00001", "SAMD00002"}

    def test_extracts_sra_run_ids(self, tmp_path: Path) -> None:
        """Comment[SRA_RUN] カラムから SRA-Run ID を抽出する。"""
        sdrf_content = """Source Name\tComment[BioSample]\tComment[SRA_RUN]
AF\tSAMD00001\tDRR000001
BF\tSAMD00002\tDRR000002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == {"SAMD00001", "SAMD00002"}
        assert result["sra_run"] == {"DRR000001", "DRR000002"}
        assert result["sra_experiment"] == set()

    def test_extracts_sra_experiment_ids(self, tmp_path: Path) -> None:
        """Comment[SRA_EXPERIMENT] カラムから SRA-Experiment ID を抽出する。"""
        sdrf_content = """Source Name\tComment[BioSample]\tComment[SRA_EXPERIMENT]
AF\tSAMD00001\tDRX000001
BF\tSAMD00002\tDRX000002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["sra_experiment"] == {"DRX000001", "DRX000002"}
        assert result["sra_run"] == set()

    def test_extracts_all_three_columns(self, tmp_path: Path) -> None:
        """BioSample / SRA_RUN / SRA_EXPERIMENT 3 列を同時に抽出する。"""
        sdrf_content = """Source Name\tComment[BioSample]\tComment[SRA_EXPERIMENT]\tComment[SRA_RUN]
AF\tSAMD00001\tDRX000001\tDRR000001
BF\tSAMD00002\tDRX000002\tDRR000002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == {"SAMD00001", "SAMD00002"}
        assert result["sra_run"] == {"DRR000001", "DRR000002"}
        assert result["sra_experiment"] == {"DRX000001", "DRX000002"}

    def test_dedupes_sra_run_across_rows(self, tmp_path: Path) -> None:
        """同じ SRA_RUN が複数行に出現する場合は dedup する (set)。"""
        sdrf_content = """Source Name\tComment[SRA_RUN]
AF\tDRR000001
BF\tDRR000001
CF\tDRR000002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["sra_run"] == {"DRR000001", "DRR000002"}

    def test_skips_empty_sra_run_values(self, tmp_path: Path) -> None:
        """SRA_RUN の空 cell / 空白のみ cell は skip する。"""
        sdrf_content = """Source Name\tComment[SRA_RUN]
AF\tDRR000001
BF\t
CF\t
DF\tDRR000002
"""
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["sra_run"] == {"DRR000001", "DRR000002"}

    def test_quoted_cell_with_tab_kept_as_single_value(self, tmp_path: Path) -> None:
        """MAGE-TAB 仕様で quote 囲み cell 内の tab はリテラル保持し、cell 境界として扱わない。"""
        sdrf_content = 'Source Name\tComment[BioSample]\tComment[description]\nAF\t"SAMD00001\tnote"\tdescription\n'
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == {"SAMD00001\tnote"}

    def test_quoted_cell_with_newline_kept_as_single_row(self, tmp_path: Path) -> None:
        """MAGE-TAB 仕様で quote 囲み cell 内の newline はリテラル保持し、行境界として扱わない。"""
        sdrf_content = 'Source Name\tComment[BioSample]\nAF\t"SAMD00001\nsecond-line"\nBF\tSAMD00002\n'
        sdrf_path = tmp_path / "test.sdrf.txt"
        sdrf_path.write_text(sdrf_content, encoding="utf-8")

        result = parse_sdrf_file(sdrf_path)
        assert result["biosample"] == {"SAMD00001\nsecond-line", "SAMD00002"}

    def test_real_fixture_e_gead_1096(self) -> None:
        """実 fixture E-GEAD-1096 で SRA_RUN / SRA_EXPERIMENT を抽出できることを確認 (E2E)。"""
        fixture_path = (
            Path(__file__).parent.parent.parent
            / "fixtures/usr/local/resources/gea/experiment/E-GEAD-1000"
            / "E-GEAD-1096/E-GEAD-1096.sdrf.txt"
        )
        assert fixture_path.exists(), f"fixture missing: {fixture_path}"

        result = parse_sdrf_file(fixture_path)
        # E-GEAD-1096 は 5 データ行で SRA_RUN / SRA_EXPERIMENT 列の両方を持つ (staging 調査済)
        assert len(result["biosample"]) > 0
        assert len(result["sra_run"]) > 0
        assert len(result["sra_experiment"]) > 0
        # 形式確認: SRA_RUN は [SDE]RR\d+、SRA_EXPERIMENT は [SDE]RX\d+
        for run_id in result["sra_run"]:
            assert run_id[0] in "SDE", f"bad sra-run prefix: {run_id}"
            assert run_id[1:3] == "RR", f"bad sra-run body: {run_id}"
        for exp_id in result["sra_experiment"]:
            assert exp_id[0] in "SDE", f"bad sra-experiment prefix: {exp_id}"
            assert exp_id[1:3] == "RX", f"bad sra-experiment body: {exp_id}"


class TestClassifyRelatedStudy:
    """Tests for _classify_related_study helper."""

    def test_classifies_jga_study(self) -> None:
        """JGA:JGAS* → ("jga-study", "JGAS*")"""
        assert _classify_related_study("JGA:JGAS000123") == ("jga-study", "JGAS000123")

    def test_classifies_humandbs(self) -> None:
        """NBDC:hum* → ("humandbs", "hum*")"""
        assert _classify_related_study("NBDC:hum0001") == ("humandbs", "hum0001")

    def test_case_insensitive_prefix_jga(self) -> None:
        """prefix 判定は case-insensitive (防御実装)。accession part は原 case 維持。"""
        assert _classify_related_study("jga:JGAS000001") == ("jga-study", "JGAS000001")
        assert _classify_related_study("Jga:JGAS000001") == ("jga-study", "JGAS000001")

    def test_case_insensitive_prefix_nbdc(self) -> None:
        """NBDC prefix も case-insensitive。"""
        assert _classify_related_study("nbdc:hum0001") == ("humandbs", "hum0001")
        assert _classify_related_study("Nbdc:hum0001") == ("humandbs", "hum0001")

    def test_strips_outer_whitespace(self) -> None:
        """前後の空白 + accession 側の前後空白を strip する。"""
        assert _classify_related_study("  JGA:JGAS000001  ") == ("jga-study", "JGAS000001")
        assert _classify_related_study("JGA: JGAS000001") == ("jga-study", "JGAS000001")

    def test_skips_metabolonote(self) -> None:
        """Metabolonote:SE* は None (XrefType 追加対象外)。"""
        assert _classify_related_study("Metabolonote:SE1") is None

    def test_skips_rpmm(self) -> None:
        """RPMM:RPMM* は None (XrefType 追加対象外)。"""
        assert _classify_related_study("RPMM:RPMM0001") is None

    def test_skips_metabolights(self) -> None:
        """Metabolights:MTBLS* は spec 外 prefix のため None (silent skip)。"""
        assert _classify_related_study("Metabolights:MTBLS1892") is None

    def test_skips_empty_string(self) -> None:
        """空文字列 / 空白のみは None。"""
        assert _classify_related_study("") is None
        assert _classify_related_study("   ") is None

    def test_skips_unknown_prefix(self) -> None:
        """JGA: / NBDC: 以外の不明 prefix は None。"""
        assert _classify_related_study("XYZ:foo") is None
        assert _classify_related_study("ENA:ERP000001") is None

    def test_skips_no_colon(self) -> None:
        """コロンを含まない値は None。"""
        assert _classify_related_study("JGAS000001") is None
        assert _classify_related_study("hum0001") is None

    def test_skips_empty_accession_after_prefix(self) -> None:
        """prefix 後の accession が空 (strip 後) の場合は None。"""
        assert _classify_related_study("JGA:") is None
        assert _classify_related_study("NBDC:   ") is None


class TestProcessIdfSdrfDir:
    """Tests for process_idf_sdrf_dir function (returns IdfSdrfResult)."""

    def test_returns_idf_sdrf_result_type(self, tmp_path: Path) -> None:
        """戻り値は IdfSdrfResult dataclass instance。"""
        entry_dir = tmp_path / "E-GEAD-291"
        entry_dir.mkdir()

        result = process_idf_sdrf_dir(entry_dir)
        assert isinstance(result, IdfSdrfResult)

    def test_processes_complete_dir(self, tmp_path: Path) -> None:
        """IDF と SDRF の両方がある場合を処理する。"""
        entry_dir = tmp_path / "E-GEAD-291"
        entry_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB7770
Comment[Related study]\tJGA:JGAS000001\tNBDC:hum0001
"""
        (entry_dir / "E-GEAD-291.idf.txt").write_text(idf_content, encoding="utf-8")

        sdrf_content = """Source Name\tComment[BioSample]\tComment[SRA_EXPERIMENT]\tComment[SRA_RUN]
AF\tSAMD00001\tDRX000001\tDRR000001
"""
        (entry_dir / "E-GEAD-291.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        result = process_idf_sdrf_dir(entry_dir)
        assert result.entry_id == "E-GEAD-291"
        assert result.bioproject == "PRJDB7770"
        assert result.related_studies == ["JGA:JGAS000001", "NBDC:hum0001"]
        assert result.biosamples == {"SAMD00001"}
        assert result.sra_runs == {"DRR000001"}
        assert result.sra_experiments == {"DRX000001"}

    def test_handles_missing_idf(self, tmp_path: Path) -> None:
        """IDF がない場合は bioproject=None / related_studies=[] を返す。"""
        entry_dir = tmp_path / "E-GEAD-100"
        entry_dir.mkdir()

        sdrf_content = """Source Name\tComment[BioSample]
AF\tSAMD00001
"""
        (entry_dir / "E-GEAD-100.sdrf.txt").write_text(sdrf_content, encoding="utf-8")

        result = process_idf_sdrf_dir(entry_dir)
        assert result.entry_id == "E-GEAD-100"
        assert result.bioproject is None
        assert result.related_studies == []
        assert result.biosamples == {"SAMD00001"}
        assert result.sra_runs == set()
        assert result.sra_experiments == set()

    def test_handles_missing_sdrf(self, tmp_path: Path) -> None:
        """SDRF がない場合は biosamples/sra_runs/sra_experiments が全て空 set。"""
        entry_dir = tmp_path / "E-GEAD-100"
        entry_dir.mkdir()

        idf_content = """Comment[BioProject]\tPRJDB1234
"""
        (entry_dir / "E-GEAD-100.idf.txt").write_text(idf_content, encoding="utf-8")

        result = process_idf_sdrf_dir(entry_dir)
        assert result.entry_id == "E-GEAD-100"
        assert result.bioproject == "PRJDB1234"
        assert result.biosamples == set()
        assert result.sra_runs == set()
        assert result.sra_experiments == set()

    def test_handles_empty_dir(self, tmp_path: Path) -> None:
        """IDF も SDRF もない場合は全て空。"""
        entry_dir = tmp_path / "MTBKS100"
        entry_dir.mkdir()

        result = process_idf_sdrf_dir(entry_dir)
        assert result.entry_id == "MTBKS100"
        assert result.bioproject is None
        assert result.related_studies == []
        assert result.biosamples == set()
        assert result.sra_runs == set()
        assert result.sra_experiments == set()


class TestIdfSdrfResult:
    """Tests for IdfSdrfResult dataclass defaults."""

    def test_default_factory_sets(self) -> None:
        """dataclass のデフォルト factory が各インスタンスで独立している。"""
        r1 = IdfSdrfResult(entry_id="E-GEAD-1")
        r2 = IdfSdrfResult(entry_id="E-GEAD-2")
        r1.biosamples.add("SAMD00001")
        r1.sra_runs.add("DRR000001")
        r1.related_studies.append("JGA:JGAS000001")
        # r2 は r1 の変更の影響を受けない
        assert r2.biosamples == set()
        assert r2.sra_runs == set()
        assert r2.related_studies == []

    def test_defaults(self) -> None:
        """全フィールドのデフォルト値確認。"""
        r = IdfSdrfResult(entry_id="E-GEAD-1")
        assert r.entry_id == "E-GEAD-1"
        assert r.bioproject is None
        assert r.related_studies == []
        assert r.biosamples == set()
        assert r.sra_runs == set()
        assert r.sra_experiments == set()
