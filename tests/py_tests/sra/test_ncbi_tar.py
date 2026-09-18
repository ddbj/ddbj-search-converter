"""sra/ncbi_tar.py のテスト。"""

import io
import subprocess
import tarfile
import tempfile
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from unittest.mock import MagicMock, patch

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra import ncbi_tar
from ddbj_search_converter.sra.ncbi_tar import (
    _check_for_newer_full,
    download_full_tar_gz,
    get_ncbi_base_full_path,
    get_ncbi_daily_tar_gz_url,
    get_ncbi_full_tar_gz_url,
    get_ncbi_last_merged_path,
    get_ncbi_tar_path,
    sync_ncbi_tar,
)

TODAY = date(2026, 9, 15)


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _write_tar(path: Path, members: dict[str, bytes], mode: Literal["w", "w:gz"]) -> None:
    with tarfile.open(path, mode) as tf:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def _read_tar_last_wins(path: Path) -> dict[str, bytes]:
    """tar の「名前 -> 末尾側メンバーの中身」を返す (TarXMLReader と同じ後勝ち)。"""
    result: dict[str, bytes] = {}
    with tarfile.open(path, "r") as tf:
        for member in tf.getmembers():
            extracted = tf.extractfile(member)
            assert extracted is not None
            result[member.name] = extracted.read()
    return result


class _FakeHttpClient:
    """httpx.Client の HEAD だけを差し替える (外部 HTTP 境界)。"""

    def __init__(self, ok_urls: set[str], probed: list[str]) -> None:
        self._ok_urls = ok_urls
        self._probed = probed

    def __enter__(self) -> "_FakeHttpClient":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def head(self, url: str) -> SimpleNamespace:
        self._probed.append(url)
        return SimpleNamespace(status_code=200 if url in self._ok_urls else 404)


@pytest.fixture
def ncbi_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """ローカルミラー・TODAY・HTTP を差し替えた sync_ncbi_tar 用の環境。"""
    mirror = tmp_path / "mirror"
    mirror.mkdir()
    config = Config(result_dir=tmp_path / "result", const_dir=tmp_path / "const")
    get_ncbi_tar_path(config).parent.mkdir(parents=True)

    ok_urls: set[str] = set()
    probed: list[str] = []
    monkeypatch.setattr(ncbi_tar, "NCBI_SRA_METADATA_LOCAL_PATH", mirror)
    monkeypatch.setattr(ncbi_tar, "TODAY", TODAY)
    monkeypatch.setattr(httpx, "Client", lambda **_: _FakeHttpClient(ok_urls, probed))

    def put_full(date_str: str, members: dict[str, bytes]) -> Path:
        path = mirror / f"NCBI_SRA_Metadata_Full_{date_str}.tar.gz"
        _write_tar(path, members, "w:gz")
        return path

    def put_daily(date_str: str, members: dict[str, bytes]) -> Path:
        path = mirror / f"NCBI_SRA_Metadata_{date_str}.tar.gz"
        _write_tar(path, members, "w:gz")
        return path

    def set_state(*, base_full: str | None, last_merged: str | None) -> None:
        if base_full is not None:
            get_ncbi_base_full_path(config).write_text(base_full)
        if last_merged is not None:
            get_ncbi_last_merged_path(config).write_text(last_merged)

    return SimpleNamespace(
        config=config,
        mirror=mirror,
        ok_urls=ok_urls,
        probed=probed,
        put_full=put_full,
        put_daily=put_daily,
        set_state=set_state,
    )


class TestGetNcbiFullTarGzUrl:
    """get_ncbi_full_tar_gz_url 関数のテスト。"""

    def test_returns_correct_url(self) -> None:
        result = get_ncbi_full_tar_gz_url("20240115")
        expected = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/NCBI_SRA_Metadata_Full_20240115.tar.gz"
        assert result == expected

    def test_different_date(self) -> None:
        result = get_ncbi_full_tar_gz_url("20231201")
        assert "20231201" in result
        assert "Full" in result


class TestGetNcbiDailyTarGzUrl:
    """get_ncbi_daily_tar_gz_url 関数のテスト。"""

    def test_returns_correct_url(self) -> None:
        result = get_ncbi_daily_tar_gz_url("20240115")
        expected = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/NCBI_SRA_Metadata_20240115.tar.gz"
        assert result == expected

    def test_daily_does_not_contain_full(self) -> None:
        result = get_ncbi_daily_tar_gz_url("20240115")
        assert "Full" not in result


class TestGetNcbiTarPath:
    """get_ncbi_tar_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.result_dir = Path("/data/result")

        result = get_ncbi_tar_path(mock_config)

        assert result == Path("/data/result/sra_tar/NCBI_SRA_Metadata.tar")


class TestGetNcbiLastMergedPath:
    """get_ncbi_last_merged_path 関数のテスト。"""

    def test_returns_correct_path(self) -> None:
        mock_config = MagicMock()
        mock_config.result_dir = Path("/data/result")

        result = get_ncbi_last_merged_path(mock_config)

        assert result == Path("/data/result/sra_tar/ncbi_last_merged.txt")


@pytest.mark.usefixtures("with_logger_isolated")
class TestCheckForNewerFull:
    """土台の Full より新しい Full を検出する。"""

    def test_full_older_than_last_merged_but_newer_than_base_is_detected(self, ncbi_env: SimpleNamespace) -> None:
        # Full は日付から数日遅れて届くので、届いた時点で last_merged は Full の日付を追い越している
        ncbi_env.set_state(base_full="20260814", last_merged="20260914")
        ncbi_env.put_full("20260913", {"a": b"x"})

        assert _check_for_newer_full(ncbi_env.config) == "20260913"

    def test_full_equal_to_base_is_not_newer(self, ncbi_env: SimpleNamespace) -> None:
        ncbi_env.set_state(base_full="20260913", last_merged="20260914")
        ncbi_env.put_full("20260913", {"a": b"x"})
        ncbi_env.put_full("20260814", {"a": b"x"})

        assert _check_for_newer_full(ncbi_env.config) is None

    def test_latest_is_chosen_when_multiple_newer_fulls_exist(self, ncbi_env: SimpleNamespace) -> None:
        ncbi_env.set_state(base_full="20260715", last_merged="20260914")
        ncbi_env.put_full("20260814", {"a": b"x"})
        ncbi_env.put_full("20260913", {"a": b"x"})

        assert _check_for_newer_full(ncbi_env.config) == "20260913"

    def test_missing_base_file_returns_latest_available_full(self, ncbi_env: SimpleNamespace) -> None:
        ncbi_env.set_state(base_full=None, last_merged="20260914")
        ncbi_env.put_full("20260814", {"a": b"x"})
        ncbi_env.put_full("20260913", {"a": b"x"})

        assert _check_for_newer_full(ncbi_env.config) == "20260913"

    def test_falls_back_to_http_and_never_probes_dates_up_to_base(self, ncbi_env: SimpleNamespace) -> None:
        ncbi_env.set_state(base_full="20260901", last_merged="20260914")
        ncbi_env.ok_urls.add(get_ncbi_full_tar_gz_url("20260913"))

        assert _check_for_newer_full(ncbi_env.config) == "20260913"
        probed_dates = [url.rsplit("_", 1)[1].removesuffix(".tar.gz") for url in ncbi_env.probed]
        assert probed_dates
        assert min(probed_dates) > "20260901"

    def test_no_full_anywhere_returns_none(self, ncbi_env: SimpleNamespace) -> None:
        ncbi_env.set_state(base_full="20260814", last_merged="20260914")

        assert _check_for_newer_full(ncbi_env.config) is None

    @given(
        base_days_ago=st.integers(min_value=1, max_value=120),
        full_days_ago=st.integers(min_value=0, max_value=59),
        last_merged_days_ago=st.integers(min_value=0, max_value=60),
    )
    def test_detection_depends_only_on_base_not_on_last_merged(
        self, base_days_ago: int, full_days_ago: int, last_merged_days_ago: int
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mirror = root / "mirror"
            mirror.mkdir()
            config = Config(result_dir=root / "result", const_dir=root / "const")
            get_ncbi_tar_path(config).parent.mkdir(parents=True)
            get_ncbi_base_full_path(config).write_text(_ymd(TODAY - timedelta(days=base_days_ago)))
            get_ncbi_last_merged_path(config).write_text(_ymd(TODAY - timedelta(days=last_merged_days_ago)))
            full_date = _ymd(TODAY - timedelta(days=full_days_ago))
            (mirror / f"NCBI_SRA_Metadata_Full_{full_date}.tar.gz").write_bytes(b"")

            with (
                patch.object(ncbi_tar, "NCBI_SRA_METADATA_LOCAL_PATH", mirror),
                patch.object(ncbi_tar, "TODAY", TODAY),
                patch.object(httpx, "Client", lambda **_: _FakeHttpClient(set(), [])),
            ):
                result = _check_for_newer_full(config)

        expected = full_date if full_days_ago < base_days_ago else None
        assert result == expected


@pytest.mark.usefixtures("with_logger_isolated")
class TestDownloadFullTarGz:
    """Full から tar を作り直す。既存の tar は成功するまで壊さない。"""

    def test_success_replaces_tar_and_records_base_full_and_last_merged(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        _write_tar(tar_path, {"OLD/OLD.run.xml": b"old"}, "w")
        ncbi_env.set_state(base_full="20260814", last_merged="20260914")
        ncbi_env.put_full("20260913", {"NEW/NEW.run.xml": b"new"})

        download_full_tar_gz(ncbi_env.config, "20260913")

        assert _read_tar_last_wins(tar_path) == {"NEW/NEW.run.xml": b"new"}
        assert get_ncbi_base_full_path(ncbi_env.config).read_text() == "20260913"
        assert get_ncbi_last_merged_path(ncbi_env.config).read_text() == "20260913"

    def test_corrupt_full_keeps_existing_tar_and_state_intact(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        _write_tar(tar_path, {"OLD/OLD.run.xml": b"old"}, "w")
        before = tar_path.read_bytes()
        ncbi_env.set_state(base_full="20260814", last_merged="20260914")
        (ncbi_env.mirror / "NCBI_SRA_Metadata_Full_20260913.tar.gz").write_bytes(b"this is not gzip")

        with pytest.raises(subprocess.CalledProcessError):
            download_full_tar_gz(ncbi_env.config, "20260913")

        assert tar_path.read_bytes() == before
        assert get_ncbi_base_full_path(ncbi_env.config).read_text() == "20260814"
        assert get_ncbi_last_merged_path(ncbi_env.config).read_text() == "20260914"
        assert sorted(p.name for p in tar_path.parent.iterdir()) == sorted(
            [tar_path.name, "ncbi_base_full.txt", "ncbi_last_merged.txt"]
        )

    def test_leftover_temp_file_from_a_failed_run_does_not_leak_into_the_tar(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        ncbi_env.put_full("20260913", {"NEW/NEW.run.xml": b"new"})
        (ncbi_env.mirror / "NCBI_SRA_Metadata_Full_20260912.tar.gz").write_bytes(b"this is not gzip")
        with pytest.raises(subprocess.CalledProcessError):
            download_full_tar_gz(ncbi_env.config, "20260912")

        download_full_tar_gz(ncbi_env.config, "20260913")

        assert _read_tar_last_wins(tar_path) == {"NEW/NEW.run.xml": b"new"}


@pytest.mark.usefixtures("with_logger_isolated")
class TestSyncNcbiTar:
    """Full の作り直しと daily の追記の組み合わせ。"""

    def test_rebuilds_from_newer_full_even_when_last_merged_is_ahead_of_it(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        _write_tar(tar_path, {"SRA1/SRA1.run.xml": b"v1", "GONE/GONE.run.xml": b"superseded"}, "w")
        ncbi_env.set_state(base_full="20260814", last_merged="20260914")
        ncbi_env.put_full("20260913", {"SRA1/SRA1.run.xml": b"v2", "SRA2/SRA2.run.xml": b"v1"})
        ncbi_env.put_daily("20260914", {"SRA2/SRA2.run.xml": b"v2"})

        sync_ncbi_tar(ncbi_env.config)

        # GONE が消えている = 追記ではなく作り直し。SRA2 は Full の後に daily が追記されて v2
        assert _read_tar_last_wins(tar_path) == {"SRA1/SRA1.run.xml": b"v2", "SRA2/SRA2.run.xml": b"v2"}
        assert get_ncbi_base_full_path(ncbi_env.config).read_text() == "20260913"
        assert get_ncbi_last_merged_path(ncbi_env.config).read_text() == "20260914"

    def test_appends_daily_without_rebuild_when_base_is_the_latest_full(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        _write_tar(tar_path, {"SRA1/SRA1.run.xml": b"v1", "KEEP/KEEP.run.xml": b"keep"}, "w")
        ncbi_env.set_state(base_full="20260913", last_merged="20260913")
        ncbi_env.put_full("20260913", {"SRA1/SRA1.run.xml": b"from-full"})
        ncbi_env.put_daily("20260914", {"SRA1/SRA1.run.xml": b"v2"})

        sync_ncbi_tar(ncbi_env.config)

        assert _read_tar_last_wins(tar_path) == {"SRA1/SRA1.run.xml": b"v2", "KEEP/KEEP.run.xml": b"keep"}
        assert get_ncbi_base_full_path(ncbi_env.config).read_text() == "20260913"
        assert get_ncbi_last_merged_path(ncbi_env.config).read_text() == "20260914"

    def test_tar_without_base_file_is_rebuilt_from_latest_full(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        _write_tar(tar_path, {"GONE/GONE.run.xml": b"superseded"}, "w")
        ncbi_env.set_state(base_full=None, last_merged="20260914")
        ncbi_env.put_full("20260913", {"SRA1/SRA1.run.xml": b"v1"})

        sync_ncbi_tar(ncbi_env.config)

        assert _read_tar_last_wins(tar_path) == {"SRA1/SRA1.run.xml": b"v1"}
        assert get_ncbi_base_full_path(ncbi_env.config).read_text() == "20260913"

    def test_missing_tar_is_built_from_latest_full_and_following_dailies(self, ncbi_env: SimpleNamespace) -> None:
        tar_path = get_ncbi_tar_path(ncbi_env.config)
        ncbi_env.put_full("20260913", {"SRA1/SRA1.run.xml": b"v1"})
        ncbi_env.put_daily("20260913", {"SRA1/SRA1.run.xml": b"must-not-be-appended"})
        ncbi_env.put_daily("20260915", {"SRA1/SRA1.run.xml": b"v2"})

        sync_ncbi_tar(ncbi_env.config)

        # Full と同日の daily は Full に含まれているので追記しない。欠けた日 (0914) は飛ばして続ける
        assert _read_tar_last_wins(tar_path) == {"SRA1/SRA1.run.xml": b"v2"}
        assert get_ncbi_base_full_path(ncbi_env.config).read_text() == "20260913"
        assert get_ncbi_last_merged_path(ncbi_env.config).read_text() == "20260915"
