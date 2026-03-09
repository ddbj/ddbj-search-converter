"""Tests for ddbj_search_converter.status_cache.build module."""

import datetime
from pathlib import Path

from ddbj_search_converter.config import Config
from ddbj_search_converter.status_cache.build import (
    _parse_livelist_file,
    build_status_cache,
    find_latest_livelist_date,
)
from ddbj_search_converter.status_cache.db import (
    fetch_bp_statuses_from_cache,
    fetch_bs_statuses_from_cache,
)


class TestParseLivelistFile:
    def test_parse_livelist_file(self, tmp_path):
        tsv_file = tmp_path.joinpath("test.txt")
        tsv_file.write_text(
            "Accession\tUpdated\tStatus\nPRJDB1\t2020-03-30\tpublic\nPRJDB2\t2021-01-15\tpublic\n",
            encoding="utf-8",
        )

        result = list(_parse_livelist_file(tsv_file, "live"))
        assert result == [("PRJDB1", "live"), ("PRJDB2", "live")]

    def test_parse_livelist_file_header_skip(self, tmp_path):
        tsv_file = tmp_path.joinpath("test.txt")
        tsv_file.write_text(
            "Accession\tUpdated\tStatus\nPRJDB1\t2020-03-30\tpublic\n",
            encoding="utf-8",
        )

        result = list(_parse_livelist_file(tsv_file, "suppressed"))
        assert len(result) == 1
        assert result[0] == ("PRJDB1", "suppressed")

    def test_parse_livelist_file_empty_lines_skipped(self, tmp_path):
        tsv_file = tmp_path.joinpath("test.txt")
        tsv_file.write_text(
            "Accession\tUpdated\tStatus\nPRJDB1\t2020-03-30\tpublic\n\nPRJDB2\t2021-01-15\tpublic\n",
            encoding="utf-8",
        )

        result = list(_parse_livelist_file(tsv_file, "live"))
        assert len(result) == 2


def _create_livelist_files(base_path: Path, kind: str, date_str: str) -> None:
    """Helper to create all 3 livelist files for a given date."""
    for status in ("public", "suppressed", "withdrawn"):
        file_path = base_path.joinpath(f"{date_str}.{kind}.ddbj.{status}.txt")
        file_path.write_text(
            f"Accession\tUpdated\tStatus\nTEST001\t2020-01-01\t{status}\n",
            encoding="utf-8",
        )


class TestFindLatestLivelistDate:
    def test_find_latest_livelist_date(self, tmp_path, monkeypatch):
        monkeypatch.setattr("ddbj_search_converter.status_cache.build.TODAY", datetime.date(2026, 3, 8))

        _create_livelist_files(tmp_path, "bioproject", "20260308")

        result = find_latest_livelist_date(tmp_path, "bioproject")
        assert result == "20260308"

    def test_find_latest_livelist_date_missing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr("ddbj_search_converter.status_cache.build.TODAY", datetime.date(2026, 3, 8))

        # Today has only 2 of 3 files (missing withdrawn)
        for status in ("public", "suppressed"):
            file_path = tmp_path.joinpath(f"20260308.bioproject.ddbj.{status}.txt")
            file_path.write_text("Accession\tUpdated\tStatus\n", encoding="utf-8")

        # Yesterday has all 3
        _create_livelist_files(tmp_path, "bioproject", "20260307")

        result = find_latest_livelist_date(tmp_path, "bioproject")
        assert result == "20260307"

    def test_find_latest_livelist_date_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr("ddbj_search_converter.status_cache.build.TODAY", datetime.date(2026, 3, 8))

        result = find_latest_livelist_date(tmp_path, "bioproject")
        assert result is None


class TestBuildStatusCacheIntegration:
    def test_build_status_cache_integration(self, tmp_path, monkeypatch):
        monkeypatch.setattr("ddbj_search_converter.status_cache.build.TODAY", datetime.date(2026, 3, 8))

        # Create BP livelist files
        bp_dir = tmp_path.joinpath("bp_livelist")
        bp_dir.mkdir()
        monkeypatch.setattr("ddbj_search_converter.status_cache.build.BP_LIVELIST_BASE_PATH", bp_dir)

        bp_dir.joinpath("20260308.bioproject.ddbj.public.txt").write_text(
            "Accession\tUpdated\tStatus\nPRJDB1\t2020-03-30\tpublic\nPRJDB2\t2021-01-15\tpublic\n",
            encoding="utf-8",
        )
        bp_dir.joinpath("20260308.bioproject.ddbj.suppressed.txt").write_text(
            "Accession\tUpdated\tStatus\nPRJDB3\t2022-05-01\tsuppressed\n",
            encoding="utf-8",
        )
        bp_dir.joinpath("20260308.bioproject.ddbj.withdrawn.txt").write_text(
            "Accession\tUpdated\tStatus\nPRJDB4\t2023-01-01\twithdrawn\n",
            encoding="utf-8",
        )

        # Create BS livelist files
        bs_dir = tmp_path.joinpath("bs_livelist")
        bs_dir.mkdir()
        monkeypatch.setattr("ddbj_search_converter.status_cache.build.BS_LIVELIST_BASE_PATH", bs_dir)

        bs_dir.joinpath("20260308.biosample.ddbj.public.txt").write_text(
            "Accession\tUpdated\tStatus\nSAMD00000001\t2020-01-01\tpublic\n",
            encoding="utf-8",
        )
        bs_dir.joinpath("20260308.biosample.ddbj.suppressed.txt").write_text(
            "Accession\tUpdated\tStatus\nSAMD00000002\t2021-01-01\tsuppressed\n",
            encoding="utf-8",
        )
        bs_dir.joinpath("20260308.biosample.ddbj.withdrawn.txt").write_text(
            "Accession\tUpdated\tStatus\n",
            encoding="utf-8",
        )

        config = Config(result_dir=tmp_path)
        build_status_cache(config)

        # Verify BP statuses
        bp_result = fetch_bp_statuses_from_cache(config, ["PRJDB1", "PRJDB2", "PRJDB3", "PRJDB4"])
        assert bp_result == {
            "PRJDB1": "live",
            "PRJDB2": "live",
            "PRJDB3": "suppressed",
            "PRJDB4": "withdrawn",
        }

        # Verify BS statuses
        bs_result = fetch_bs_statuses_from_cache(config, ["SAMD00000001", "SAMD00000002"])
        assert bs_result == {
            "SAMD00000001": "live",
            "SAMD00000002": "suppressed",
        }
