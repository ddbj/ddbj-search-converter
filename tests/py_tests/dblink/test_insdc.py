"""Tests for ddbj_search_converter.dblink.insdc module."""

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from ddbj_search_converter.config import (
    INSDC_BP_PRESERVED_REL_PATH,
    INSDC_BS_PRESERVED_REL_PATH,
    Config,
)
from ddbj_search_converter.dblink.db import init_dblink_db, normalize_edge
from ddbj_search_converter.dblink.insdc import (
    INSDC_TO_BP_QUERY,
    INSDC_TO_BS_QUERY,
    MANAGER_BLACKLIST_QUERY,
    TRAD_DBS,
    _load_insdc_preserved_file,
    _write_insdc_relations,
    main,
)
from ddbj_search_converter.logging.logger import _ctx, init_logger


def _make_mock_connect(
    data_by_db: dict[str, tuple[list[int], list[tuple[int, str, str]]]] | None = None,
) -> Callable[..., MagicMock]:
    """psycopg2.connect のモック生成ヘルパー。

    data_by_db: ``dbname -> (manager_blacklist_acids, main_rows)`` のマッピング。
    未指定の DB は両方空。

    - manager_blacklist_acids: ``MANAGER_BLACKLIST_QUERY`` の戻り値 (ac_id の list)
    - main_rows: ``INSDC_TO_BP/BS_QUERY`` の戻り値 (``(ac_id, accession, target_id)`` の 3-tuple)

    本番実装 ``_fetch_from_db`` は同一 connection 内で ``conn.cursor()`` を
    2 回呼ぶ (manager blacklist 用 client cursor → 本クエリ用 server-side
    cursor) ため、``conn.cursor.side_effect`` で呼び順契約を表現する。
    """
    if data_by_db is None:
        data_by_db = {}

    def _make_cursor(rows: list[Any]) -> MagicMock:
        cur = MagicMock()
        cur.__iter__ = MagicMock(return_value=iter(rows))
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        return cur

    def mock_connect(**kwargs: Any) -> MagicMock:
        manager_blacklist_acids, main_rows = data_by_db.get(kwargs.get("dbname", ""), ([], []))
        mgr_cursor = _make_cursor([(acid,) for acid in manager_blacklist_acids])
        main_cursor = _make_cursor(main_rows)
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = [mgr_cursor, main_cursor]
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
            (1, "AB000001", "PRJDB12345"),
            (2, "CP035466", "PRJNA999999"),
        ]

        mock_connect = _make_mock_connect({"g-actual": ([], sample_data)})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges ORDER BY src_accession").fetchall()

        assert len(rows) == 2
        # normalize_edge("insdc", "AB000001", "bioproject", "PRJDB12345")
        # "bioproject" < "insdc" => ("bioproject", "PRJDB12345", "insdc", "AB000001")
        assert rows[0] == ("bioproject", "PRJDB12345", "insdc", "AB000001")
        assert rows[1] == ("bioproject", "PRJNA999999", "insdc", "CP035466")

    def test_writes_biosample_relations(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        sample_data = [
            (1, "AB000001", "SAMD00000001"),
            (2, "CP035466", "SAMN12345678"),
        ]

        mock_connect = _make_mock_connect({"g-actual": ([], sample_data)})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "biosample", INSDC_TO_BS_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges ORDER BY src_accession").fetchall()

        assert len(rows) == 2
        # normalize_edge("insdc", "AB000001", "biosample", "SAMD00000001")
        # "biosample" < "insdc" => ("biosample", "SAMD00000001", "insdc", "AB000001")
        assert rows[0] == ("biosample", "SAMD00000001", "insdc", "AB000001")
        assert rows[1] == ("biosample", "SAMN12345678", "insdc", "CP035466")

    def test_blacklist_filters_target_ids(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        sample_data = [
            (1, "AB000001", "PRJDB12345"),
            (2, "AB000002", "PRJDB99999"),
            (3, "AB000003", "PRJDB12345"),
        ]

        blacklist = {"PRJDB99999"}
        mock_connect = _make_mock_connect({"g-actual": ([], sample_data)})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, blacklist)

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges").fetchall()

        assert len(rows) == 2
        accessions = {row[3] for row in rows}  # insdc accessions are in dst_accession of raw_edges
        assert "AB000002" not in accessions

    def test_iterates_all_trad_dbs(self, insdc_config: Config) -> None:
        init_dblink_db(insdc_config)

        mock_connect = _make_mock_connect()
        with patch(
            "ddbj_search_converter.dblink.insdc.connect_with_retry",
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

        mock_connect = _make_mock_connect({"g-actual": ([], [(1, "AB000001", "PRJDB12345")])})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
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

    def test_skips_rows_in_manager_blacklist(self, insdc_config: Config) -> None:
        """manager blacklist の ac_id を持つ row は TSV に出ない。"""
        init_dblink_db(insdc_config)

        main_rows = [
            (1, "AB000001", "PRJDB12345"),  # 採用
            (2, "AB000002", "PRJDB99999"),  # blacklist
            (3, "AB000003", "PRJDB11111"),  # 採用
        ]

        mock_connect = _make_mock_connect({"g-actual": ([2], main_rows)})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges").fetchall()

        assert len(rows) == 2
        accessions = {row[3] for row in rows}
        assert "AB000002" not in accessions
        assert "AB000001" in accessions
        assert "AB000003" in accessions

    def test_manager_blacklist_skips_all_rows_sharing_ac_id(self, insdc_config: Config) -> None:
        """1 ac_id が複数 row (primary + secondary accession の併存等) に展開されているとき、
        manager blacklist hit で **全 row** が skip されること。`in set` 比較が ac_id 単位で
        作用することの境界 guard。
        """
        init_dblink_db(insdc_config)

        main_rows = [
            (1, "AB000001", "PRJDB12345"),
            (1, "AB000001.1", "PRJDB12345"),  # ac_id=1 の別 accession (secondary 想定)
            (2, "AB000002", "PRJDB99999"),  # 採用
        ]

        mock_connect = _make_mock_connect({"g-actual": ([1], main_rows)})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges").fetchall()

        accessions = {row[3] for row in rows}
        assert "AB000001" not in accessions
        assert "AB000001.1" not in accessions
        assert "AB000002" in accessions

    def test_empty_manager_blacklist_passes_all_rows(self, insdc_config: Config) -> None:
        """manager blacklist が空のとき、全 row が通る (filter 反転 mutation を検出)。"""
        init_dblink_db(insdc_config)

        main_rows = [
            (1, "AB000001", "PRJDB12345"),
            (2, "AB000002", "PRJDB99999"),
        ]

        mock_connect = _make_mock_connect({"g-actual": ([], main_rows)})
        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=mock_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges").fetchall()

        assert len(rows) == 2

    def test_manager_blacklist_fetch_failure_triggers_retry(self, insdc_config: Config) -> None:
        """manager blacklist fetch の OperationalError で retry が発火し、2 回目成功で全件処理される。

        retry スコープが connect だけでなく blacklist fetch も含んでいることの guard。
        """
        import psycopg2

        init_dblink_db(insdc_config)

        g_actual_call_count = {"n": 0}

        def flaky_connect(**kwargs: Any) -> MagicMock:
            dbname = kwargs.get("dbname", "")
            if dbname != "g-actual":
                # 他 DB は空結果で素通り
                mgr = MagicMock()
                mgr.__iter__ = MagicMock(return_value=iter([]))
                mgr.__enter__ = MagicMock(return_value=mgr)
                mgr.__exit__ = MagicMock(return_value=False)
                main = MagicMock()
                main.__iter__ = MagicMock(return_value=iter([]))
                main.__enter__ = MagicMock(return_value=main)
                main.__exit__ = MagicMock(return_value=False)
                mock_conn = MagicMock()
                mock_conn.cursor.side_effect = [mgr, main]
                return mock_conn

            g_actual_call_count["n"] += 1
            if g_actual_call_count["n"] == 1:
                # 1 回目: blacklist fetch の execute で OperationalError
                bad = MagicMock()
                bad.__enter__ = MagicMock(return_value=bad)
                bad.__exit__ = MagicMock(return_value=False)
                bad.execute.side_effect = psycopg2.OperationalError("connection lost")
                mock_conn = MagicMock()
                mock_conn.cursor.return_value = bad
                return mock_conn

            # 2 回目以降: 正常
            main_rows = [(1, "AB000001", "PRJDB12345")]
            mgr = MagicMock()
            mgr.__iter__ = MagicMock(return_value=iter([]))
            mgr.__enter__ = MagicMock(return_value=mgr)
            mgr.__exit__ = MagicMock(return_value=False)
            main = MagicMock()
            main.__iter__ = MagicMock(return_value=iter(main_rows))
            main.__enter__ = MagicMock(return_value=main)
            main.__exit__ = MagicMock(return_value=False)
            mock_conn = MagicMock()
            mock_conn.cursor.side_effect = [mgr, main]
            return mock_conn

        with (
            patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=flaky_connect),
            patch("ddbj_search_converter.dblink.insdc.time.sleep"),
        ):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        assert g_actual_call_count["n"] == 2

        db_path = insdc_config.const_dir.joinpath("dblink", "dblink.tmp.duckdb")
        with duckdb.connect(str(db_path)) as conn:
            rows = conn.execute("SELECT * FROM raw_edges WHERE dst_accession = 'AB000001'").fetchall()
        assert len(rows) == 1

    def test_no_commit_called_between_cursors(self, insdc_config: Config) -> None:
        """blacklist fetch と本クエリの間で commit が呼ばれない (server-side cursor 保護)。

        psycopg2 の DECLARE ... CURSOR は transaction 内でだけ生きる。manager
        blacklist fetch で開いた transaction を commit() で閉じると、続く
        server-side cursor が無効化される。
        """
        init_dblink_db(insdc_config)

        captured: list[MagicMock] = []

        def capturing_connect(**kwargs: Any) -> MagicMock:
            mgr = MagicMock()
            mgr.__iter__ = MagicMock(return_value=iter([]))
            mgr.__enter__ = MagicMock(return_value=mgr)
            mgr.__exit__ = MagicMock(return_value=False)
            main = MagicMock()
            main.__iter__ = MagicMock(
                return_value=iter([(1, "AB000001", "PRJDB12345")] if kwargs.get("dbname") == "g-actual" else [])
            )
            main.__enter__ = MagicMock(return_value=main)
            main.__exit__ = MagicMock(return_value=False)
            mock_conn = MagicMock()
            mock_conn.cursor.side_effect = [mgr, main]
            captured.append(mock_conn)
            return mock_conn

        with patch("ddbj_search_converter.dblink.insdc.connect_with_retry", side_effect=capturing_connect):
            _write_insdc_relations(insdc_config, "bioproject", INSDC_TO_BP_QUERY, set())

        assert captured, "_write_insdc_relations が 1 度も connect しなかった"
        for conn in captured:
            conn.commit.assert_not_called()


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

    def test_skips_empty_insdc_accession(self, insdc_config: Config) -> None:
        """INSDC 側が空文字列の行 ('\\tPRJDB...') は silent ghost edge を作らずスキップする。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text("\tPRJDB12345\nAB000001\tPRJDB99999\n   \tPRJDB00001\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert pairs == {("AB000001", "PRJDB99999")}

    def test_skips_single_column_line(self, insdc_config: Config) -> None:
        """タブ区切りが崩れた 1 カラムの行はスキップする。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text("AB000001_PRJDB12345\nAB000001\tPRJDB99999\n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert pairs == {("AB000001", "PRJDB99999")}

    def test_strips_surrounding_whitespace(self, insdc_config: Config) -> None:
        """各カラムの前後空白は除去する (BOM 混入等の保険)。"""
        preserved_path = insdc_config.const_dir.joinpath("dblink", "insdc_bp_preserved.tsv")
        preserved_path.write_text(" AB000001 \t PRJDB12345 \n")

        pairs = _load_insdc_preserved_file(insdc_config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")

        assert pairs == {("AB000001", "PRJDB12345")}


class TestQueryShape:
    """INSDC クエリ群の SQL 構造 guard。

    既存テストは ``psycopg2.connect`` を mock 化しており SQL の WHERE 文字列を実行しないため、
    constant assertion で SQL 構造の退化を検出する。

    SSOT: ``docs/data-architecture.md`` §「公開状態の判定 (manager テーブル)」。
    """

    @pytest.mark.parametrize(
        ("query", "label"),
        [
            (INSDC_TO_BP_QUERY, "INSDC_TO_BP_QUERY"),
            (INSDC_TO_BS_QUERY, "INSDC_TO_BS_QUERY"),
        ],
    )
    def test_main_query_does_not_join_manager(self, query: str, label: str) -> None:
        # 本クエリは manager を JOIN しない (planner 劣化を避けるため別クエリで取得)。
        # ``manager`` 文字列の混入だけで JOIN/WHERE 双方の退化を検出できる。
        assert "manager" not in query.lower(), label
        # JOIN は link_pr_ac と project の 2 つだけ (manager / entry / その他の JOIN 混入を検出)。
        assert query.count("JOIN") == 2, label
        # post-filter 用 ac_id が SELECT 列の 1 番目に含まれる (列順退化で in-set 比較が
        # silent に常時 False 化する事故を防ぐ)。
        assert "SELECT acc.ac_id," in query, label

    def test_manager_blacklist_query_selects_blacklist_acids(self) -> None:
        # 採用 status (1002 public / 1005 secondary) 以外を blacklist として返す形であること。
        # IN ↔ NOT IN の反転 mutation を検出。
        assert "FROM manager" in MANAGER_BLACKLIST_QUERY
        assert "status NOT IN (1002, 1005)" in MANAGER_BLACKLIST_QUERY
        assert "status IN (1002, 1005)" not in MANAGER_BLACKLIST_QUERY.replace("NOT IN", "")

    @pytest.mark.parametrize("forbidden_status", [1000, 1001, 1004, 1006, 1007])
    def test_manager_blacklist_query_keeps_white_set_minimal(self, forbidden_status: int) -> None:
        # 採用外 status を白リスト側 (NOT IN リスト) に混入させる mutation を検出。
        # 1000/1001/1004/1006/1007 のいずれかが NOT IN リストに追加されると、
        # 該当 status の ac_id が blacklist から漏れて dbXrefs に出てしまう。
        token_tail = f", {forbidden_status}"
        token_head = f"({forbidden_status},"
        assert token_tail not in MANAGER_BLACKLIST_QUERY, (
            f"status {forbidden_status} が NOT IN リスト末尾に混入 (採用外なのに blacklist から外れる)"
        )
        assert token_head not in MANAGER_BLACKLIST_QUERY, (
            f"status {forbidden_status} が NOT IN リスト先頭に混入 (採用外なのに blacklist から外れる)"
        )


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
