"""cli/sync_dra_tar.py のテスト。"""

import pytest

from ddbj_search_converter.cli.sync_dra_tar import parse_args


class TestParseArgs:
    def test_defaults_to_incremental_sync(self) -> None:
        _, force_rebuild, repair = parse_args([])
        assert force_rebuild is False
        assert repair is False

    def test_force_rebuild(self) -> None:
        _, force_rebuild, repair = parse_args(["--force-rebuild"])
        assert force_rebuild is True
        assert repair is False

    def test_repair(self) -> None:
        _, force_rebuild, repair = parse_args(["--repair"])
        assert force_rebuild is False
        assert repair is True

    def test_force_rebuild_and_repair_are_exclusive(self) -> None:
        """両方を受け付けると、作り直しと追記のどちらが起きたか読めなくなる。"""
        with pytest.raises(SystemExit):
            parse_args(["--force-rebuild", "--repair"])
