"""Tests for ddbj_search_converter.dblink.insdc module."""

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import init_dblink_db, normalize_edge
from ddbj_search_converter.config import INSDC_BP_PRESERVED_REL_PATH, INSDC_BS_PRESERVED_REL_PATH
from ddbj_search_converter.dblink.insdc import (
    INSDC_TO_BP_QUERY,
    INSDC_TO_BS_QUERY,
    TRAD_DBS,
    _load_insdc_preserved_file,
    _write_insdc_relations,
    main,
)
from ddbj_search_converter.logging.logger import _ctx, init_logger


def _make_mock_connect(
    data_by_db: dict[str, list[tuple[str, str]]] | None = None,
) -> Callable[..., MagicMock]:
    """psycopg2.connect のモック生成ヘルパー。

    data_by_db: dbname -> rows マッピング。未指定の DB は空結果を返す。
    """
    if data_by_db is None:
        data_by_db = {}

    def mock_connect(**kwargs: Any) -> MagicMock:
        rows = data_by_db.get(kwargs.get("dbname", ""), [])
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter(rows))
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn

    return mock_connect


@pytest.fixture
def insdc_config(tmp_path: Path) -> Config:
    config = Config(
        result_dir=tmp_path,
        const_dir=tmp_path.joinpath("const"),
        trad_postgres_url="postgresql://user:pass@host:5432",
    )
    config.const_dir.joinpath("dblink").mkdir(parents=True, exist_ok=True)
    config.const_dir.joinpath("bp").mkdir(parents=True, exist_ok=True)
    config.const_dir.joinpath("bs").mkdir(parents=True, exist_ok=True)

    return config


@pytest.fixture(autouse=True)
def _setup_logger(insdc_config: Config) -> Iterator[None]:
    init_logger(run_name="test_insdc", config=insdc_config)
    yield
    _ctx.set(None)


class TestWriteInsdcRelations:
    """_write_insdc_relations のテスト。"""

    def test_writes_bioproject_relations(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        sample_data = [
            ("AB000001", "PRJDB12345"),
            ("CP035466", "PRJNA999999"),
        ]

        mock_connect = _make_mock_connect({"g-actual": sample_data})
        with patch("ddbj_search_converter.dblink.insdc.psycopg2.connect", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM relation ORDER BY src_accession").fetchall()

        assert len(rows) == 2
        # normalize_edge("insdc", "AB000001", "bioproject", "PRJDB12345")
        # "bioproject" < "insdc" => ("bioproject", "PRJDB12345", "insdc", "AB000001")
        assert rows[0] == ("bioproject", "PRJDB12345", "insdc", "AB000001")
        assert rows[1] == ("bioproject", "PRJNA999999", "insdc", "CP035466")

    def test_writes_biosample_relations(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        sample_data = [
            ("AB000001", "SAMD00000001"),
            ("CP035466", "SAMN12345678"),
        ]

        mock_connect = _make_mock_connect({"g-actual": sample_data})
        with patch("ddbj_search_converter.dblink.insdc.psycopg2.connect", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "biosample", INSDC_TO_BS_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM relation ORDER BY src_accession").fetchall()

        assert len(rows) == 2
        # normalize_edge("insdc", "AB000001", "biosample", "SAMD00000001")
        # "biosample" < "insdc" => ("biosample", "SAMD00000001", "insdc", "AB000001")
        assert rows[0] == ("biosample", "SAMD00000001", "insdc", "AB000001")
        assert rows[1] == ("biosample", "SAMN12345678", "insdc", "CP035466")

    def test_blacklist_filters_target_ids(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        sample_data = [
            ("AB000001", "PRJDB12345"),
            ("AB000002", "PRJDB99999"),
            ("AB000003", "PRJDB12345"),
        ]

        blacklist = {"PRJDB99999"}
        mock_connect = _make_mock_connect({"g-actual": sample_data})
        with patch("ddbj_search_converter.dblink.insdc.psycopg2.connect", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, blacklist)

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM relation").fetchall()

        assert len(rows) == 2
        accessions = {row[3] for row in rows}  # insdc accessions are in dst_accession
        assert "AB000002" not in accessions

    def test_iterates_all_trad_dbs(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        mock_connect = _make_mock_connect()
        with patch(
            "ddbj_search_converter.dblink.insdc.psycopg2.connect",
            side_effect=mock_connect,
        ) as patched:
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        assert patched.call_count == len(TRAD_DBS)
        for i, (dbname, port) in enumerate(TRAD_DBS):
            call_kwargs = patched.call_args_list[i].kwargs
            assert call_kwargs["dbname"] == dbname
            assert call_kwargs["port"] == port

    def test_tsv_content_is_normalized(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        mock_connect = _make_mock_connect({"g-actual": [("AB000001", "PRJDB12345")]})
        with patch("ddbj_search_converter.dblink.insdc.psycopg2.connect", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        # TSV ファイルの内容を確認 (g-actual のみデータあり)
        from ddbj_search_converter.config import TODAY_STR

        tmp_dir = insdc_config.result_dir.joinpath("dblink", "tmp", TODAY_STR)
        tsv_path = tmp_dir.joinpath("insdc_to_bioproject_g-actual.tsv")
        assert tsv_path.exists()

        content = tsv_path.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 1
        cols = lines[0].split("\t")
        assert len(cols) == 4
        # normalize_edge("insdc", "AB000001", "bioproject", "PRJDB12345")
        # => ("bioproject", "PRJDB12345", "insdc", "AB000001")
        assert cols == ["bioproject", "PRJDB12345", "insdc", "AB000001"]


class TestLoadInsdcPreservedFile:
    """_load_insdc_preserved_file のテスト。"""

    def test_loads_bioproject_preserved_file(self, insdc_config: Config) -> None:
        """BioProject preserved file を読み込む。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text("AB000001\tPRJDB12345\nCP035466\tPRJNA999999\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert pairs == {("AB000001", "PRJDB12345"), ("CP035466", "PRJNA999999")}

    def test_loads_biosample_preserved_file(self, insdc_config: Config) -> None:
        """BioSample preserved file を読み込む。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bs_preserved.tsv")
        preserved_path.write_text("AB000001\tSAMD00000001\nCP035466\tSAMN12345678\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BS_PRESERVED_REL_PATH, "biosample")

        assert pairs == {("AB000001", "SAMD00000001"), ("CP035466", "SAMN12345678")}

    def test_raises_when_file_missing(self, insdc_config: Config) -> None:
        """ファイルが存在しない場合は FileNotFoundError。"""
        with pytest.raises(FileNotFoundError):
            _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

    def test_skips_invalid_target_accession(self, insdc_config: Config) -> None:
        """無効なターゲット accession をスキップする。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text("AB000001\tPRJDB12345\nCP035466\tINVALID_ID\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert pairs == {("AB000001", "PRJDB12345")}

    def test_skips_empty_lines(self, insdc_config: Config) -> None:
        """空行をスキップする。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text("AB000001\tPRJDB12345\n\nCP035466\tPRJNA999999\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert len(pairs) == 2

    def test_does_not_validate_insdc_accession(self, insdc_config: Config) -> None:
        """INSDC 側の accession はバリデーションしない。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text("WEIRD_ACC_123\tPRJDB12345\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert pairs == {("WEIRD_ACC_123", "PRJDB12345")}


class TestNormalizeEdgeInsdc:
    """normalize_edge で insdc タイプの正規化テスト。"""

    def test_insdc_bioproject_normalization(self) -> None:
        result = normalize_edge("insdc", "AB000001", "bioproject", "PRJDB12345")
        assert result == ("bioproject", "PRJDB12345", "insdc", "AB000001")

    def test_insdc_biosample_normalization(self) -> None:
        result = normalize_edge("insdc", "CP035466", "biosample", "SAMD00000001")
        assert result == ("biosample", "SAMD00000001", "insdc", "CP035466")


class TestMain:
    """main() のテスト。"""

    def test_skips_trad_when_postgres_url_is_empty(self, tmp_path: Path) -> None:
        """trad_postgres_url が空の場合、TRAD 処理はスキップするが preserved は処理する。"""
        config = Config(
            result_dir=tmp_path,
            const_dir=tmp_path.joinpath("const"),
            trad_postgres_url="",
        )
        config.const_dir.joinpath("bp").mkdir(parents=True, exist_ok=True)
        config.const_dir.joinpath("bs").mkdir(parents=True, exist_ok=True)

        with (
            patch("ddbj_search_converter.dblink.insdc.get_config", return_value=config),
            patch("ddbj_search_converter.dblink.insdc._write_insdc_relations") as mock_write,
            patch(
                "ddbj_search_converter.dblink.insdc._load_insdc_preserved_file",
                return_value=set(),
            ) as mock_preserved,
        ):
            main()

        mock_write.assert_not_called()
        assert mock_preserved.call_count == 2

    def test_calls_write_for_both_types(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        with (
            patch("ddbj_search_converter.dblink.insdc.get_config", return_value=insdc_config),
            patch("ddbj_search_converter.dblink.insdc._write_insdc_relations") as mock_write,
            patch(
                "ddbj_search_converter.dblink.insdc._load_insdc_preserved_file",
                return_value=set(),
            ),
        ):
            main()

        assert mock_write.call_count == 2
        call_args_list = mock_write.call_args_list
        assert call_args_list[0][0][1] == "bioproject"
        assert call_args_list[1][0][1] == "biosample"
