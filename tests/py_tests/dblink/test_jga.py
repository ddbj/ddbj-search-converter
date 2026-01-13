"""Tests for ddbj_search_converter.dblink.jga module."""
from pathlib import Path
from typing import Set, Tuple

import pytest

from ddbj_search_converter.dblink.jga import (
    extract_hum_id,
    extract_pubmed_ids,
    join_relations,
    read_relation_csv,
    reverse_relation,
)


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

    def test_returns_empty_when_file_not_exists(
        self, tmp_path: Path, test_config: pytest.fixture  # type: ignore
    ) -> None:
        """ファイルが存在しない場合は空を返す。"""
        # Logger is required because read_relation_csv calls log_warn
        from ddbj_search_converter.logging.logger import run_logger
        with run_logger(config=test_config):
            result = read_relation_csv(tmp_path / "nonexistent.csv")

        assert result == set()


class TestJoinRelations:
    """Tests for join_relations function."""

    def test_simple_join(self) -> None:
        """シンプルな join。"""
        ab: Set[Tuple[str, str]] = {("A1", "B1"), ("A2", "B2")}
        bc: Set[Tuple[str, str]] = {("B1", "C1"), ("B2", "C2")}

        result = join_relations(ab, bc)

        assert result == {("A1", "C1"), ("A2", "C2")}

    def test_one_to_many(self) -> None:
        """1 対多の join。"""
        ab: Set[Tuple[str, str]] = {("A1", "B1")}
        bc: Set[Tuple[str, str]] = {("B1", "C1"), ("B1", "C2")}

        result = join_relations(ab, bc)

        assert result == {("A1", "C1"), ("A1", "C2")}

    def test_no_match(self) -> None:
        """マッチがない場合は空。"""
        ab: Set[Tuple[str, str]] = {("A1", "B1")}
        bc: Set[Tuple[str, str]] = {("B2", "C1")}

        result = join_relations(ab, bc)

        assert result == set()

    def test_empty_input(self) -> None:
        """空の入力。"""
        ab: Set[Tuple[str, str]] = set()
        bc: Set[Tuple[str, str]] = {("B1", "C1")}

        result = join_relations(ab, bc)

        assert result == set()


class TestReverseRelation:
    """Tests for reverse_relation function."""

    def test_reverse(self) -> None:
        """関連を逆転する。"""
        relation: Set[Tuple[str, str]] = {("A1", "B1"), ("A2", "B2")}

        result = reverse_relation(relation)

        assert result == {("B1", "A1"), ("B2", "A2")}

    def test_empty(self) -> None:
        """空の入力。"""
        relation: Set[Tuple[str, str]] = set()

        result = reverse_relation(relation)

        assert result == set()


class TestExtractHumId:
    """Tests for extract_hum_id function."""

    def test_extracts_hum_id(self) -> None:
        """NBDC Number を抽出する。"""
        study_entry = {
            "accession": "JGAS000001",
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": [
                    {"TAG": "NBDC Number", "VALUE": "hum0004"},
                    {"TAG": "Other", "VALUE": "value"},
                ]
            },
        }

        result = extract_hum_id(study_entry)

        assert result == "hum0004"

    def test_single_attribute(self) -> None:
        """単一の STUDY_ATTRIBUTE の場合 (dict)。"""
        study_entry = {
            "accession": "JGAS000001",
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": {"TAG": "NBDC Number", "VALUE": "hum0005"}
            },
        }

        result = extract_hum_id(study_entry)

        assert result == "hum0005"

    def test_no_nbdc_number(self) -> None:
        """NBDC Number がない場合は None。"""
        study_entry = {
            "accession": "JGAS000001",
            "STUDY_ATTRIBUTES": {
                "STUDY_ATTRIBUTE": [{"TAG": "Other", "VALUE": "value"}]
            },
        }

        result = extract_hum_id(study_entry)

        assert result is None

    def test_no_study_attributes(self) -> None:
        """STUDY_ATTRIBUTES がない場合は None。"""
        study_entry = {"accession": "JGAS000001"}

        result = extract_hum_id(study_entry)

        assert result is None


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
            "PUBLICATIONS": {
                "PUBLICATION": {"id": "12345678", "DB_TYPE": "PUBMED"}
            },
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
            "PUBLICATIONS": {
                "PUBLICATION": {"id": 12345678, "DB_TYPE": "PUBMED"}
            },
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}
