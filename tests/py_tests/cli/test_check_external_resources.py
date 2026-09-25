"""Tests for check_external_resources: files must exist, mounted trees must not be empty."""

from pathlib import Path

import pytest

from ddbj_search_converter.cli.check_external_resources import find_missing, get_required_nonempty_dirs
from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import run_logger


@pytest.fixture
def logged(test_config: Config):  # type: ignore[no-untyped-def]
    with run_logger(config=test_config):
        yield


@pytest.mark.usefixtures("logged")
class TestFindMissing:
    def test_all_present_returns_nothing(self, tmp_path: Path) -> None:
        f = tmp_path / "a.txt"
        f.write_text("x")
        d = tmp_path / "tree"
        (d / "sub").mkdir(parents=True)

        assert find_missing([("A", f)], [("Tree", d)]) == []

    def test_unresolved_and_absent_files_are_missing(self, tmp_path: Path) -> None:
        assert find_missing([("Unresolved", None), ("Absent", tmp_path / "nope")], []) == ["Unresolved", "Absent"]

    def test_empty_directory_is_missing(self, tmp_path: Path) -> None:
        """A mount point left behind without its mount: the directory exists but has nothing in it."""
        d = tmp_path / "sra"
        d.mkdir()

        assert find_missing([], [("DRA SRA file tree", d)]) == ["DRA SRA file tree"]

    def test_absent_directory_and_file_in_place_of_directory_are_missing(self, tmp_path: Path) -> None:
        f = tmp_path / "not_a_dir"
        f.write_text("x")

        assert find_missing([], [("Absent", tmp_path / "nope"), ("File", f)]) == ["Absent", "File"]

    def test_directory_with_only_a_hidden_entry_counts_as_non_empty(self, tmp_path: Path) -> None:
        d = tmp_path / "tree"
        d.mkdir()
        (d / ".keep").write_text("")

        assert find_missing([], [("Tree", d)]) == []


class TestRequiredNonemptyDirs:
    def test_covers_the_dra_sra_tree_the_file_index_scans(self) -> None:
        paths = {p for _, p in get_required_nonempty_dirs()}

        assert any(p.parts[-4:] == ("sra", "ByExp", "sra", "DRX") for p in paths)
