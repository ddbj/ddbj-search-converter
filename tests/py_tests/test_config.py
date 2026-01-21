"""config.py のテスト。"""
import json
from pathlib import Path

import pytest

from ddbj_search_converter.config import (Config, get_last_run_path,
                                          read_last_run, write_last_run)


class TestLastRunUtilities:
    """last_run.json ユーティリティのテスト。"""

    def test_get_last_run_path(self, tmp_path: Path) -> None:
        """last_run.json のパスを正しく返す。"""
        config = Config(result_dir=tmp_path)
        path = get_last_run_path(config)
        assert path == tmp_path / "last_run.json"

    def test_read_last_run_returns_none_when_file_not_exists(self, tmp_path: Path) -> None:
        """ファイルが存在しない場合は全て None を返す。"""
        config = Config(result_dir=tmp_path)
        result = read_last_run(config)
        assert result == {
            "bioproject": None,
            "biosample": None,
            "sra": None,
            "jga": None,
        }

    def test_read_last_run_returns_values_from_file(self, tmp_path: Path) -> None:
        """ファイルが存在する場合は値を返す。"""
        config = Config(result_dir=tmp_path)
        last_run_path = tmp_path / "last_run.json"
        last_run_path.write_text(json.dumps({
            "bioproject": "2026-01-19T00:00:00Z",
            "biosample": "2026-01-18T00:00:00Z",
            "sra": None,
            "jga": None,
        }))

        result = read_last_run(config)
        assert result == {
            "bioproject": "2026-01-19T00:00:00Z",
            "biosample": "2026-01-18T00:00:00Z",
            "sra": None,
            "jga": None,
        }

    def test_write_last_run_creates_file(self, tmp_path: Path) -> None:
        """ファイルが存在しない場合は作成する。"""
        config = Config(result_dir=tmp_path)
        write_last_run(config, "bioproject", "2026-01-20T00:00:00Z")

        last_run_path = tmp_path / "last_run.json"
        assert last_run_path.exists()

        with last_run_path.open("r") as f:
            data = json.load(f)
        assert data["bioproject"] == "2026-01-20T00:00:00Z"
        assert data["biosample"] is None
        assert data["sra"] is None
        assert data["jga"] is None

    def test_write_last_run_updates_existing_file(self, tmp_path: Path) -> None:
        """既存ファイルを更新する。"""
        config = Config(result_dir=tmp_path)
        last_run_path = tmp_path / "last_run.json"
        last_run_path.write_text(json.dumps({
            "bioproject": "2026-01-19T00:00:00Z",
            "biosample": "2026-01-18T00:00:00Z",
            "sra": None,
            "jga": None,
        }))

        write_last_run(config, "bioproject", "2026-01-20T00:00:00Z")

        with last_run_path.open("r") as f:
            data = json.load(f)
        assert data["bioproject"] == "2026-01-20T00:00:00Z"
        assert data["biosample"] == "2026-01-18T00:00:00Z"  # 他の値は保持

    def test_write_last_run_uses_current_time_when_no_timestamp(self, tmp_path: Path) -> None:
        """timestamp が指定されない場合は現在時刻を使用する。"""
        config = Config(result_dir=tmp_path)
        write_last_run(config, "sra")

        last_run_path = tmp_path / "last_run.json"
        with last_run_path.open("r") as f:
            data = json.load(f)
        assert data["sra"] is not None
        assert data["sra"].endswith("Z")  # ISO8601 UTC 形式

    def test_write_last_run_creates_parent_directory(self, tmp_path: Path) -> None:
        """親ディレクトリが存在しない場合は作成する。"""
        result_dir = tmp_path / "subdir" / "nested"
        config = Config(result_dir=result_dir)
        write_last_run(config, "jga", "2026-01-20T00:00:00Z")

        last_run_path = result_dir / "last_run.json"
        assert last_run_path.exists()
