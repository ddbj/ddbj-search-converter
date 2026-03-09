"""Tests for ddbj_search_converter.dblink.insdc module."""

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.dblink.db import init_dblink_db, normalize_edge
from ddbj_search_converter.dblink.insdc import (
    INSDC_TO_BP_QUERY,
    INSDC_TO_BS_QUERY,
    TRAD_DBS,
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

    def test_skips_when_trad_postgres_url_is_empty(self, tmp_path: Path) -> None:
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
        ):
            main()

        mock_write.assert_not_called()

    def test_calls_write_for_both_types(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        with (
            patch("ddbj_search_converter.dblink.insdc.get_config", return_value=insdc_config),
            patch("ddbj_search_converter.dblink.insdc._write_insdc_relations") as mock_write,
        ):
            main()

        assert mock_write.call_count == 2
        call_args_list = mock_write.call_args_list
        assert call_args_list[0][0][1] == "bioproject"
        assert call_args_list[1][0][1] == "biosample"
