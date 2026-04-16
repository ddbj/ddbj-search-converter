"""Tests for ddbj_search_converter.dblink.jga module."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from ddbj_search_converter.config import (
    JGA_DATASET_HUM_ID_REL_PATH,
    JGA_STUDY_HUM_ID_REL_PATH,
    Config,
)
from ddbj_search_converter.dblink.jga import (
    _load_jga_humandbs_file,
    extract_pubmed_ids,
    join_relations,
    read_relation_csv,
    reverse_relation,
)
from ddbj_search_converter.logging.logger import _ctx, init_logger


class TestReadRelationCsv:
    """Tests for read_relation_csv function."""

    def test_reads_valid_csv(self, tmp_path: Path) -> None:
        """正常な CSV を読み込む。"""
        csv_content = """id,from_id,to_id
1,JGAD001,JGAP001
2,JGAD002,JGAP002
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content, encoding="utf-8")

        result = read_relation_csv(csv_path)

        assert result == {("JGAD001", "JGAP001"), ("JGAD002", "JGAP002")}

    def test_skips_invalid_rows(self, tmp_path: Path) -> None:
        """不正な行をスキップする。"""
        csv_content = """id,from_id,to_id
1,JGAD001,JGAP001
2,JGAD002
3,JGAD003,JGAP003
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content, encoding="utf-8")

        result = read_relation_csv(csv_path)

        assert result == {("JGAD001", "JGAP001"), ("JGAD003", "JGAP003")}

    def test_raises_when_file_not_exists(self, test_config: pytest.fixture) -> None:  # type: ignore[valid-type]
        """ファイルが存在しない場合は FileNotFoundError を発生する。"""
        from ddbj_search_converter.logging.logger import run_logger

        with run_logger(config=test_config), pytest.raises(FileNotFoundError):
            read_relation_csv(Path("/nonexistent/path/test.csv"))


class TestJoinRelations:
    """Tests for join_relations function."""

    def test_simple_join(self) -> None:
        """シンプルな join。"""
        ab: set[tuple[str, str]] = {("A1", "B1"), ("A2", "B2")}
        bc: set[tuple[str, str]] = {("B1", "C1"), ("B2", "C2")}

        result = join_relations(ab, bc)

        assert result == {("A1", "C1"), ("A2", "C2")}

    def test_one_to_many(self) -> None:
        """1 対多の join。"""
        ab: set[tuple[str, str]] = {("A1", "B1")}
        bc: set[tuple[str, str]] = {("B1", "C1"), ("B1", "C2")}

        result = join_relations(ab, bc)

        assert result == {("A1", "C1"), ("A1", "C2")}

    def test_no_match(self) -> None:
        """マッチがない場合は空。"""
        ab: set[tuple[str, str]] = {("A1", "B1")}
        bc: set[tuple[str, str]] = {("B2", "C1")}

        result = join_relations(ab, bc)

        assert result == set()

    def test_empty_input(self) -> None:
        """空の入力。"""
        ab: set[tuple[str, str]] = set()
        bc: set[tuple[str, str]] = {("B1", "C1")}

        result = join_relations(ab, bc)

        assert result == set()


class TestReverseRelation:
    """Tests for reverse_relation function."""

    def test_reverse(self) -> None:
        """関連を逆転する。"""
        relation: set[tuple[str, str]] = {("A1", "B1"), ("A2", "B2")}

        result = reverse_relation(relation)

        assert result == {("B1", "A1"), ("B2", "A2")}

    def test_empty(self) -> None:
        """空の入力。"""
        relation: set[tuple[str, str]] = set()

        result = reverse_relation(relation)

        assert result == set()


@pytest.fixture
def jga_config(tmp_path: Path) -> Config:
    config = Config(
        result_dir=tmp_path,
        const_dir=tmp_path.joinpath("const"),
    )
    config.const_dir.joinpath("dblink").mkdir(parents=True, exist_ok=True)

    return config


@pytest.fixture
def _setup_logger(jga_config: Config) -> Iterator[None]:
    init_logger(run_name="test_jga", config=jga_config)
    yield
    _ctx.set(None)


@pytest.mark.usefixtures("_setup_logger")
class TestLoadJgaHumandbsFile:
    """Tests for _load_jga_humandbs_file function."""

    def test_loads_study_humandbs(self, jga_config: Config) -> None:
        """jga-study -> humandbs を読み込む。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nJGAS000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004"), ("JGAS000002", "hum0001")}

    def test_loads_dataset_humandbs(self, jga_config: Config) -> None:
        """jga-dataset -> humandbs を読み込む。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_DATASET_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAD000001\thum0004\nJGAD000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_DATASET_HUM_ID_REL_PATH, "jga-dataset")

        assert result == {("JGAD000001", "hum0004"), ("JGAD000002", "hum0001")}

    def test_skips_empty_lines(self, jga_config: Config) -> None:
        """空行をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\n\nJGAS000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert len(result) == 2

    def test_skips_malformed_lines(self, jga_config: Config) -> None:
        """カラム不足の行をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nBADLINE\nJGAS000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004"), ("JGAS000002", "hum0001")}

    def test_skips_invalid_src_accession(self, jga_config: Config) -> None:
        """無効な src accession をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nINVALID\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004")}

    def test_skips_invalid_humandbs(self, jga_config: Config) -> None:
        """無効な humandbs をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nJGAS000002\tINVALID_HUM\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004")}

    def test_raises_when_file_missing(self, jga_config: Config) -> None:
        """ファイルが存在しない場合は FileNotFoundError。"""
        with pytest.raises(FileNotFoundError):
            _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")


class TestExtractPubmedIds:
    """Tests for extract_pubmed_ids function."""

    def test_extracts_pubmed_ids(self) -> None:
        """PUBMED ID を抽出する。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {
                "PUBLICATION": [
                    {"id": "12345678", "DB_TYPE": "PUBMED"},
                    {"id": "87654321", "DB_TYPE": "PUBMED"},
                ]
            },
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678", "87654321"}

    def test_single_publication(self) -> None:
        """単一の PUBLICATION の場合 (dict)。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {"PUBLICATION": {"id": "12345678", "DB_TYPE": "PUBMED"}},
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}

    def test_filters_non_pubmed(self) -> None:
        """PUBMED 以外は除外。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {
                "PUBLICATION": [
                    {"id": "12345678", "DB_TYPE": "PUBMED"},
                    {"id": "DOI12345", "DB_TYPE": "DOI"},
                ]
            },
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}

    def test_no_publications(self) -> None:
        """PUBLICATIONS がない場合は空。"""
        study_entry = {"accession": "JGAS000001"}

        result = extract_pubmed_ids(study_entry)

        assert result == set()

    def test_integer_id(self) -> None:
        """ID が整数の場合も文字列として返す。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {"PUBLICATION": {"id": 12345678, "DB_TYPE": "PUBMED"}},
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}
