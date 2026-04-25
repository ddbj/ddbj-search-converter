"""Tests for ddbj_search_converter.cli.cleanup_old_results module."""

import os
import stat
from datetime import date, datetime
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from ddbj_search_converter.cli.cleanup_old_results import (
    cleanup,
    find_date_dirs,
    get_cleanup_target_parents,
    main,
    parse_args,
)
from ddbj_search_converter.config import (
    BP_BASE_DIR_NAME,
    BS_BASE_DIR_NAME,
    DBLINK_DIR_NAME,
    DBLINK_TMP_DIR_NAME,
    GEA_BASE_DIR_NAME,
    JGA_BASE_DIR_NAME,
    JSONL_DIR_NAME,
    LOG_DIR_NAME,
    METABOBANK_BASE_DIR_NAME,
    REGENERATE_DIR_NAME,
    SRA_BASE_DIR_NAME,
    TMP_XML_DIR_NAME,
    Config,
)


def _make_date_dirs(parent: Path, dates: list[str]) -> None:
    """Helper: parent 以下に YYYYMMDD ディレクトリを作成する。"""
    parent.mkdir(parents=True, exist_ok=True)
    for d in dates:
        parent.joinpath(d).mkdir()


class TestFindDateDirs:
    """Tests for find_date_dirs function."""

    def test_find_date_dirs_returns_sorted(self, tmp_path: Path) -> None:
        """複数の日付 dir を降順ソートで返す。"""
        _make_date_dirs(tmp_path, ["20260101", "20260315", "20260210"])

        result = find_date_dirs(tmp_path)

        assert len(result) == 3
        assert result[0] == ("20260315", tmp_path.joinpath("20260315"))
        assert result[1] == ("20260210", tmp_path.joinpath("20260210"))
        assert result[2] == ("20260101", tmp_path.joinpath("20260101"))

    def test_find_date_dirs_ignores_non_date_dirs(self, tmp_path: Path) -> None:
        """非日付名のディレクトリやファイルは無視する。"""
        _make_date_dirs(tmp_path, ["20260101", "20260201"])
        tmp_path.joinpath("latest").mkdir()
        tmp_path.joinpath("not_a_date").mkdir()
        tmp_path.joinpath("README.txt").write_text("hello")
        # 桁数が合わない
        tmp_path.joinpath("2026011").mkdir()
        # 無効な月
        tmp_path.joinpath("20261301").mkdir()

        result = find_date_dirs(tmp_path)

        assert len(result) == 2
        date_strs = [d for d, _ in result]
        assert "20260201" in date_strs
        assert "20260101" in date_strs

    def test_find_date_dirs_ignores_symlinks(self, tmp_path: Path) -> None:
        """symlink は無視する。"""
        _make_date_dirs(tmp_path, ["20260101"])
        target = tmp_path.joinpath("somewhere")
        target.mkdir()
        tmp_path.joinpath("20260201").symlink_to(target)

        result = find_date_dirs(tmp_path)

        assert len(result) == 1
        assert result[0][0] == "20260101"

    def test_find_date_dirs_nonexistent_parent(self, tmp_path: Path) -> None:
        """存在しない親ディレクトリでは空リストを返す。"""
        nonexistent = tmp_path.joinpath("does_not_exist")

        result = find_date_dirs(nonexistent)

        assert result == []

    def test_find_date_dirs_empty_directory(self, tmp_path: Path) -> None:
        """空ディレクトリでは空リストを返す。"""
        result = find_date_dirs(tmp_path)

        assert result == []


class TestFindDateDirsPBT:
    """Property-based tests for find_date_dirs."""

    @given(
        st.lists(
            st.dates(
                min_value=date(2000, 1, 1),
                max_value=date(2099, 12, 31),
            ).map(lambda d: d.strftime("%Y%m%d")),
            unique=True,
            min_size=0,
            max_size=10,
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_result_is_always_descending(self, tmp_path_factory: pytest.TempPathFactory, dates: list[str]) -> None:
        """返り値は常に日付降順でソートされている。"""
        tmp_path = tmp_path_factory.mktemp("pbt")
        _make_date_dirs(tmp_path, dates)

        result = find_date_dirs(tmp_path)

        date_strs = [d for d, _ in result]
        assert date_strs == sorted(date_strs, reverse=True)
        assert len(result) == len(dates)

    @given(
        st.lists(
            st.text(
                alphabet=st.sampled_from("0123456789abcdefghijklmnopqrstuvwxyz_-"),
                min_size=1,
                max_size=12,
            ),
            min_size=0,
            max_size=10,
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_result_contains_only_valid_dates(self, tmp_path_factory: pytest.TempPathFactory, names: list[str]) -> None:
        """返り値には有効な YYYYMMDD 文字列のみが含まれる。"""
        tmp_path = tmp_path_factory.mktemp("pbt")
        tmp_path.mkdir(parents=True, exist_ok=True)
        for name in names:
            try:
                tmp_path.joinpath(name).mkdir(exist_ok=True)
            except OSError:
                continue

        result = find_date_dirs(tmp_path)

        for date_str, _ in result:
            assert len(date_str) == 8
            datetime.strptime(date_str, "%Y%m%d")


class TestCleanup:
    """Tests for cleanup function."""

    def _make_config(self, result_dir: Path) -> Config:
        return Config(result_dir=result_dir)

    def test_cleanup_removes_old_dirs(self, tmp_path: Path) -> None:
        """keep=2 で古いディレクトリが削除される。"""
        jsonl_dir = tmp_path.joinpath(BP_BASE_DIR_NAME, JSONL_DIR_NAME)
        dates = ["20260101", "20260201", "20260301", "20260315"]
        _make_date_dirs(jsonl_dir, dates)
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=2, dry_run=False)

        assert len(failed) == 0
        assert len(removed) == 2
        assert jsonl_dir.joinpath("20260315").exists()
        assert jsonl_dir.joinpath("20260301").exists()
        assert not jsonl_dir.joinpath("20260201").exists()
        assert not jsonl_dir.joinpath("20260101").exists()

    def test_cleanup_dry_run_does_not_delete(self, tmp_path: Path) -> None:
        """dry_run=True では何も削除しない。"""
        jsonl_dir = tmp_path.joinpath(SRA_BASE_DIR_NAME, JSONL_DIR_NAME)
        dates = ["20260101", "20260201", "20260301", "20260315"]
        _make_date_dirs(jsonl_dir, dates)
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=2, dry_run=True)

        assert len(removed) == 2
        assert len(failed) == 0
        for d in dates:
            assert jsonl_dir.joinpath(d).exists()

    def test_cleanup_skips_nonexistent_parents(self, tmp_path: Path) -> None:
        """存在しない親ディレクトリはスキップしてエラーにならない。"""
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=3, dry_run=False)

        assert removed == []
        assert failed == []

    def test_cleanup_per_location_independence(self, tmp_path: Path) -> None:
        """各親ディレクトリで独立して keep が適用される。"""
        bp_jsonl = tmp_path.joinpath(BP_BASE_DIR_NAME, JSONL_DIR_NAME)
        sra_jsonl = tmp_path.joinpath(SRA_BASE_DIR_NAME, JSONL_DIR_NAME)
        _make_date_dirs(bp_jsonl, ["20260101", "20260201", "20260301", "20260315", "20260320"])
        _make_date_dirs(sra_jsonl, ["20260201", "20260301"])
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=3, dry_run=False)

        assert len(failed) == 0
        # bp_jsonl: 5 dirs -> keep 3, remove 2
        assert bp_jsonl.joinpath("20260320").exists()
        assert bp_jsonl.joinpath("20260315").exists()
        assert bp_jsonl.joinpath("20260301").exists()
        assert not bp_jsonl.joinpath("20260201").exists()
        assert not bp_jsonl.joinpath("20260101").exists()
        # sra_jsonl: 2 dirs -> keep 3, remove 0
        assert sra_jsonl.joinpath("20260201").exists()
        assert sra_jsonl.joinpath("20260301").exists()
        assert len(removed) == 2

    def test_cleanup_keeps_all_when_fewer_than_keep(self, tmp_path: Path) -> None:
        """ディレクトリ数が keep 以下なら何も削除しない。"""
        jsonl_dir = tmp_path.joinpath(JGA_BASE_DIR_NAME, JSONL_DIR_NAME)
        dates = ["20260101", "20260201"]
        _make_date_dirs(jsonl_dir, dates)
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=3, dry_run=False)

        assert removed == []
        assert failed == []
        for d in dates:
            assert jsonl_dir.joinpath(d).exists()

    def test_cleanup_keeps_all_when_exactly_keep(self, tmp_path: Path) -> None:
        """ディレクトリ数がちょうど keep と一致する場合、何も削除しない。"""
        jsonl_dir = tmp_path.joinpath(BP_BASE_DIR_NAME, JSONL_DIR_NAME)
        dates = ["20260101", "20260201", "20260301"]
        _make_date_dirs(jsonl_dir, dates)
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=3, dry_run=False)

        assert removed == []
        assert failed == []
        for d in dates:
            assert jsonl_dir.joinpath(d).exists()

    @pytest.mark.skipif(os.getuid() == 0, reason="root ignores filesystem permissions")
    def test_cleanup_reports_failed_deletions(self, tmp_path: Path) -> None:
        """削除権限がない場合、failed に追加される。"""
        jsonl_dir = tmp_path.joinpath(BP_BASE_DIR_NAME, JSONL_DIR_NAME)
        dates = ["20260101", "20260201", "20260301"]
        _make_date_dirs(jsonl_dir, dates)
        # 20260101 を削除不可にする
        target = jsonl_dir.joinpath("20260101")
        target.chmod(0o000)
        # 親ディレクトリの sticky bit を設定して削除を防ぐ
        jsonl_dir.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_ISVTX)
        config = self._make_config(tmp_path)

        try:
            _, failed = cleanup(config, keep=1, dry_run=False)

            assert len(failed) >= 1
            failed_paths = [p for p, _ in failed]
            assert target in failed_paths
        finally:
            # テスト後にパーミッションを戻してクリーンアップを許可
            jsonl_dir.chmod(stat.S_IRWXU)
            target.chmod(stat.S_IRWXU)


class TestGetCleanupTargetParents:
    """Tests for get_cleanup_target_parents function.

    parent リストはハードコードでなく、(a) result_dir 配下、(b) 重複なし、
    (c) config 定数経由で組み立てた既知のサブツリーを過不足なく含む、
    という構造的不変条件で検証する。
    """

    def test_all_parents_are_under_result_dir(self) -> None:
        """全 parent は result_dir 配下である。"""
        result_dir = Path("/tmp/cleanup_root")
        config = Config(result_dir=result_dir)

        parents = get_cleanup_target_parents(config)

        for p in parents:
            assert p.is_relative_to(result_dir)

    def test_parents_are_unique(self) -> None:
        """parent リストに重複がない (同一パスが二重に削除対象にならない)。"""
        config = Config(result_dir=Path("/tmp/cleanup_root"))

        parents = get_cleanup_target_parents(config)

        assert len(parents) == len(set(parents))

    def test_parents_cover_known_dir_names(self) -> None:
        """既知の DIR_NAME 定数による組み立て (config が SSOT) と整合する。"""
        result_dir = Path("/tmp/cleanup_root")
        config = Config(result_dir=result_dir)

        parents = set(get_cleanup_target_parents(config))

        # 単独 (logs / regenerate / dblink/tmp)
        assert result_dir / LOG_DIR_NAME in parents
        assert result_dir / REGENERATE_DIR_NAME in parents
        assert result_dir / DBLINK_DIR_NAME / DBLINK_TMP_DIR_NAME in parents

        # tmp_xml は BP/BS のみ (SRA/JGA/GEA/MTB は持たない)
        assert result_dir / BP_BASE_DIR_NAME / TMP_XML_DIR_NAME in parents
        assert result_dir / BS_BASE_DIR_NAME / TMP_XML_DIR_NAME in parents
        for base in (SRA_BASE_DIR_NAME, JGA_BASE_DIR_NAME, GEA_BASE_DIR_NAME, METABOBANK_BASE_DIR_NAME):
            assert result_dir / base / TMP_XML_DIR_NAME not in parents

        # jsonl は BP/BS/SRA/JGA/GEA/MetaboBank
        for base in (
            BP_BASE_DIR_NAME,
            BS_BASE_DIR_NAME,
            SRA_BASE_DIR_NAME,
            JGA_BASE_DIR_NAME,
            GEA_BASE_DIR_NAME,
            METABOBANK_BASE_DIR_NAME,
        ):
            assert result_dir / base / JSONL_DIR_NAME in parents

    def test_count_matches_categories(self) -> None:
        """件数 = 1 (logs) + 2 (BP/BS の tmp_xml) + 6 (各 jsonl) + 1 (regenerate) + 1 (dblink/tmp) = 11。"""
        config = Config(result_dir=Path("/tmp/cleanup_root"))

        parents = get_cleanup_target_parents(config)

        assert len(parents) == 11

    def test_parents_follow_result_dir(self) -> None:
        """result_dir を変えても、相対構造は保たれる。"""
        config_a = Config(result_dir=Path("/tmp/a"))
        config_b = Config(result_dir=Path("/var/x/y"))

        rel_a = {p.relative_to(Path("/tmp/a")) for p in get_cleanup_target_parents(config_a)}
        rel_b = {p.relative_to(Path("/var/x/y")) for p in get_cleanup_target_parents(config_b)}

        assert rel_a == rel_b


def _stub_get_config(monkeypatch: pytest.MonkeyPatch, config: Config) -> None:
    """cleanup_old_results.get_config を test config を返すように差し替える。"""
    monkeypatch.setattr(
        "ddbj_search_converter.cli.cleanup_old_results.get_config",
        lambda: config,
    )


def _set_argv(monkeypatch: pytest.MonkeyPatch, argv: list[str]) -> None:
    """sys.argv を差し替える (argv[0] はダミー)。"""
    monkeypatch.setattr("sys.argv", ["cleanup_old_results", *argv])


class TestMain:
    """Tests for main entrypoint.

    parse_args / cleanup の単体テストと別に、main が:
    (a) failed があれば sys.exit(1) する
    (b) failed がなければ正常終了する (SystemExit を投げない)
    (c) --dry-run のとき shutil.rmtree が呼ばれない
    という end-to-end の挙動を持つことを保証する。
    """

    def test_main_no_dirs_exits_normally(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        clean_ctx: None,
    ) -> None:
        """対象ディレクトリが存在しない場合、SystemExit を投げず正常終了する。"""
        config = Config(result_dir=tmp_path)
        _stub_get_config(monkeypatch, config)
        _set_argv(monkeypatch, ["--keep", "3"])

        # SystemExit が出たら test 失敗 (それ自体が assertion)。signature `-> None` の
        # 回帰は mypy 側で検出する。
        main()

    def test_main_removes_old_dirs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        clean_ctx: None,
    ) -> None:
        """keep=2 で古い dir が物理削除される。"""
        jsonl_dir = tmp_path / BP_BASE_DIR_NAME / JSONL_DIR_NAME
        for d in ("20260101", "20260201", "20260301"):
            (jsonl_dir / d).mkdir(parents=True)

        config = Config(result_dir=tmp_path)
        _stub_get_config(monkeypatch, config)
        _set_argv(monkeypatch, ["--keep", "2"])

        main()

        assert (jsonl_dir / "20260301").exists()
        assert (jsonl_dir / "20260201").exists()
        assert not (jsonl_dir / "20260101").exists()

    def test_main_dry_run_does_not_call_rmtree(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        clean_ctx: None,
        mocker: MockerFixture,
    ) -> None:
        """--dry-run のとき shutil.rmtree が一切呼ばれず、ファイルも残る。"""
        jsonl_dir = tmp_path / BP_BASE_DIR_NAME / JSONL_DIR_NAME
        all_dates = ("20260101", "20260201", "20260301", "20260401")
        for d in all_dates:
            (jsonl_dir / d).mkdir(parents=True)

        rmtree_mock = mocker.patch("ddbj_search_converter.cli.cleanup_old_results.shutil.rmtree")
        config = Config(result_dir=tmp_path)
        _stub_get_config(monkeypatch, config)
        _set_argv(monkeypatch, ["--keep", "2", "--dry-run"])

        main()

        rmtree_mock.assert_not_called()
        for d in all_dates:
            assert (jsonl_dir / d).exists()

    def test_main_failed_rmtree_exits_with_code_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        clean_ctx: None,
        mocker: MockerFixture,
    ) -> None:
        """rmtree が例外を投げた場合、main は SystemExit(1) を投げる。"""
        jsonl_dir = tmp_path / BP_BASE_DIR_NAME / JSONL_DIR_NAME
        for d in ("20260101", "20260201", "20260301"):
            (jsonl_dir / d).mkdir(parents=True)

        mocker.patch(
            "ddbj_search_converter.cli.cleanup_old_results.shutil.rmtree",
            side_effect=PermissionError("denied"),
        )
        config = Config(result_dir=tmp_path)
        _stub_get_config(monkeypatch, config)
        _set_argv(monkeypatch, ["--keep", "1"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_args_defaults(self) -> None:
        """デフォルト値で keep=3, dry_run=False。"""
        _, keep, dry_run = parse_args([])

        assert keep == 3
        assert dry_run is False

    def test_parse_args_custom_values(self) -> None:
        """--keep 5 --dry-run を正しくパースする。"""
        _, keep, dry_run = parse_args(["--keep", "5", "--dry-run"])

        assert keep == 5
        assert dry_run is True

    def test_parse_args_keep_zero_raises_error(self) -> None:
        """--keep 0 はエラーになる。"""
        with pytest.raises(SystemExit):
            parse_args(["--keep", "0"])

    def test_parse_args_keep_negative_raises_error(self) -> None:
        """--keep に負の値はエラーになる。"""
        with pytest.raises(SystemExit):
            parse_args(["--keep", "-1"])

    def test_parse_args_keep_one_is_minimum_allowed(self) -> None:
        """--keep 1 は最小許容値 (境界の正常側)。"""
        _, keep, _ = parse_args(["--keep", "1"])

        assert keep == 1
