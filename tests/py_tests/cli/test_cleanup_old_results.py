"""Tests for ddbj_search_converter.cli.cleanup_old_results module."""

import stat
from datetime import date, datetime
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ddbj_search_converter.cli.cleanup_old_results import (
    cleanup,
    find_date_dirs,
    get_cleanup_target_parents,
    parse_args,
)
from ddbj_search_converter.config import Config


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
        jsonl_dir = tmp_path.joinpath("bioproject", "jsonl")
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
        jsonl_dir = tmp_path.joinpath("sra", "jsonl")
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
        bp_jsonl = tmp_path.joinpath("bioproject", "jsonl")
        sra_jsonl = tmp_path.joinpath("sra", "jsonl")
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
        jsonl_dir = tmp_path.joinpath("jga", "jsonl")
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
        jsonl_dir = tmp_path.joinpath("bioproject", "jsonl")
        dates = ["20260101", "20260201", "20260301"]
        _make_date_dirs(jsonl_dir, dates)
        config = self._make_config(tmp_path)

        removed, failed = cleanup(config, keep=3, dry_run=False)

        assert removed == []
        assert failed == []
        for d in dates:
            assert jsonl_dir.joinpath(d).exists()

    def test_cleanup_reports_failed_deletions(self, tmp_path: Path) -> None:
        """削除権限がない場合、failed に追加される。"""
        jsonl_dir = tmp_path.joinpath("bioproject", "jsonl")
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
    """Tests for get_cleanup_target_parents function."""

    def test_returns_expected_parents(self) -> None:
        """期待する親ディレクトリを返す。"""
        config = Config(result_dir=Path("/tmp/test"))

        parents = get_cleanup_target_parents(config)

        assert len(parents) == 9
        parent_strs = [str(p) for p in parents]
        assert "/tmp/test/logs" in parent_strs
        assert "/tmp/test/bioproject/tmp_xml" in parent_strs
        assert "/tmp/test/biosample/tmp_xml" in parent_strs
        assert "/tmp/test/bioproject/jsonl" in parent_strs
        assert "/tmp/test/biosample/jsonl" in parent_strs
        assert "/tmp/test/sra/jsonl" in parent_strs
        assert "/tmp/test/jga/jsonl" in parent_strs
        assert "/tmp/test/regenerate" in parent_strs
        assert "/tmp/test/dblink/tmp" in parent_strs


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
