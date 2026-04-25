"""Integration: external resource reachability smoke.

Verifies that external I/O paths the converter depends on are reachable from
the test environment. A failure here means the converter would crash at the
data-fetch step on the same host.

Mount-based paths (Livelist, SRA Accessions) are skipped on environments where
the path is not mounted (e.g. local compose), so the test only fires meaningfully
on staging / production.
"""

from pathlib import Path

import httpx
import pytest

from ddbj_search_converter.config import (
    ASSEMBLY_SUMMARY_URL,
    BP_LIVELIST_BASE_PATH,
    BS_LIVELIST_BASE_PATH,
    SRA_ACCESSIONS_BASE_PATH,
)


def test_ncbi_assembly_summary_is_reachable() -> None:
    """IT-RESOURCE-01: NCBI FTP の assembly_summary_genbank.txt が HEAD で到達できる。

    本パイプラインの Assembly 同期 (`sync_ncbi_tar` 系) はこの URL を起点に動く。
    DL 自体ではなく HEAD でリーチャビリティだけを確認する。
    """
    response = httpx.head(ASSEMBLY_SUMMARY_URL, timeout=30, follow_redirects=True)
    assert response.status_code == 200, f"unexpected status {response.status_code} for {ASSEMBLY_SUMMARY_URL}"


def _assert_directory_has_at_least_one_file(path: Path, label: str) -> None:
    if not path.exists():
        pytest.skip(f"{label} {path} not mounted in this environment")
    if not path.is_dir():
        pytest.fail(f"{label} {path} exists but is not a directory")
    entries = [p for p in path.iterdir() if p.name not in {".", ".."}]
    assert entries, f"{label} {path} is empty"


def test_bp_livelist_directory_is_readable() -> None:
    """IT-RESOURCE-02-bp: BP livelist のディレクトリが読めて非空。"""
    _assert_directory_has_at_least_one_file(BP_LIVELIST_BASE_PATH, "BP livelist")


def test_bs_livelist_directory_is_readable() -> None:
    """IT-RESOURCE-02-bs: BS livelist のディレクトリが読めて非空。"""
    _assert_directory_has_at_least_one_file(BS_LIVELIST_BASE_PATH, "BS livelist")


def test_sra_accessions_directory_is_readable() -> None:
    """IT-RESOURCE-03: DRA / SRA Accessions tab の base path が読めて非空。"""
    _assert_directory_has_at_least_one_file(SRA_ACCESSIONS_BASE_PATH, "SRA Accessions")
