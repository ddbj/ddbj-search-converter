"""\
NCBI SRA Metadata tar.gz sync module.

Downloads and processes NCBI SRA Metadata tar.gz files:
- Full tar.gz: Creates new tar file from scratch
- Daily tar.gz: Appends entries to existing tar file

Uses curl + pigz for fast download and decompression.
"""
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional

import httpx

from ddbj_search_converter.config import (NCBI_LAST_MERGED_FILE_NAME,
                                          NCBI_SRA_METADATA_BASE_URL,
                                          NCBI_SRA_TAR_FILE_NAME,
                                          SRA_TAR_DIR_NAME, TODAY, Config)
from ddbj_search_converter.logging.logger import log_info, log_warn


def get_sra_tar_dir(config: Config) -> Path:
    """Get the directory for SRA tar files."""
    return config.const_dir.joinpath(SRA_TAR_DIR_NAME)


def get_ncbi_tar_path(config: Config) -> Path:
    """Get the path to the NCBI SRA Metadata tar file."""
    return get_sra_tar_dir(config).joinpath(NCBI_SRA_TAR_FILE_NAME)


def get_ncbi_last_merged_path(config: Config) -> Path:
    """Get the path to the ncbi_last_merged.txt file."""
    return get_sra_tar_dir(config).joinpath(NCBI_LAST_MERGED_FILE_NAME)


def get_ncbi_full_tar_gz_url(date_str: str) -> str:
    """Get the URL for NCBI SRA Metadata Full tar.gz."""
    return f"{NCBI_SRA_METADATA_BASE_URL}/NCBI_SRA_Metadata_Full_{date_str}.tar.gz"


def get_ncbi_daily_tar_gz_url(date_str: str) -> str:
    """Get the URL for NCBI SRA Metadata daily tar.gz."""
    return f"{NCBI_SRA_METADATA_BASE_URL}/NCBI_SRA_Metadata_{date_str}.tar.gz"


def find_latest_ncbi_full_date(max_days_back: int = 60) -> Optional[str]:
    """Find the latest available NCBI Full tar.gz by checking URLs.

    NCBI publishes Full tar.gz approximately once a month.
    """
    for days_back in range(max_days_back):
        check_date = TODAY - timedelta(days=days_back)
        date_str = check_date.strftime("%Y%m%d")
        url = get_ncbi_full_tar_gz_url(date_str)

        try:
            with httpx.Client(timeout=10) as client:
                response = client.head(url)
                if response.status_code == 200:
                    return date_str
        except httpx.RequestError:
            continue

    return None


def _get_last_merged_date(config: Config) -> Optional[date]:
    """Get the last merged date from ncbi_last_merged.txt."""
    last_merged_path = get_ncbi_last_merged_path(config)
    if not last_merged_path.exists():
        return None
    last_merged_str = last_merged_path.read_text().strip()
    return date(
        int(last_merged_str[:4]),
        int(last_merged_str[4:6]),
        int(last_merged_str[6:8])
    )


def find_ncbi_daily_dates_to_sync(
    config: Config,
    max_days_back: int = 30
) -> Iterator[str]:
    """Find daily tar.gz dates that need to be synced.

    Yields dates from (last_merged + 1) to today.
    """
    last_merged = _get_last_merged_date(config)
    if last_merged is None:
        last_merged = TODAY - timedelta(days=max_days_back)

    current = last_merged + timedelta(days=1)
    while current <= TODAY:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def download_full_tar_gz(config: Config, date_str: str) -> None:
    """Download NCBI Full tar.gz and create new tar file.

    Uses curl + pigz for fast download and decompression.
    """
    url = get_ncbi_full_tar_gz_url(date_str)
    tar_path = get_ncbi_tar_path(config)
    tar_dir = get_sra_tar_dir(config)
    tar_dir.mkdir(parents=True, exist_ok=True)

    log_info(f"Downloading NCBI Full tar.gz: {url}")
    log_info(f"Output: {tar_path}")

    cmd = f'curl -L -s "{url}" | pigz -d > "{tar_path}"'
    subprocess.run(cmd, shell=True, check=True)

    # Update last_merged file
    last_merged_path = get_ncbi_last_merged_path(config)
    last_merged_path.write_text(date_str)
    log_info(f"Updated last_merged: {date_str}")


def append_daily_tar_gz(config: Config, date_str: str) -> bool:
    """Append NCBI daily tar.gz entries to existing tar file.

    Returns True if successful, False if daily tar.gz not found.
    """
    url = get_ncbi_daily_tar_gz_url(date_str)
    tar_path = get_ncbi_tar_path(config)

    if not tar_path.exists():
        log_warn(f"NCBI tar file does not exist: {tar_path}")
        return False

    # Check if daily tar.gz exists
    try:
        with httpx.Client(timeout=10) as client:
            response = client.head(url)
            if response.status_code != 200:
                log_warn(f"Daily tar.gz not found: {url}")
                return False
    except httpx.RequestError as e:
        log_warn(f"Failed to check daily tar.gz: {e}")
        return False

    log_info(f"Appending daily tar.gz: {url}")

    # Download, decompress, and concatenate to existing tar
    tmp_tar_path = tar_path.parent.joinpath(f"daily_{date_str}.tar")
    try:
        # Download and decompress to temp file
        cmd = f'curl -L -s "{url}" | pigz -d > "{tmp_tar_path}"'
        subprocess.run(cmd, shell=True, check=True)

        # Concatenate temp tar to existing tar
        cmd = f'tar -Af "{tar_path}" "{tmp_tar_path}"'
        subprocess.run(cmd, shell=True, check=True)
    finally:
        # Clean up temp file
        tmp_tar_path.unlink(missing_ok=True)

    # Update last_merged file
    last_merged_path = get_ncbi_last_merged_path(config)
    last_merged_path.write_text(date_str)
    log_info(f"Updated last_merged: {date_str}")

    return True


def _check_for_newer_full(config: Config) -> Optional[str]:
    """Check if a newer Full tar.gz is available than last_merged.

    Returns the date string if a newer Full is found, None otherwise.
    """
    last_merged = _get_last_merged_date(config)
    if last_merged is None:
        return None

    current = TODAY
    while current > last_merged:
        date_str = current.strftime("%Y%m%d")
        url = get_ncbi_full_tar_gz_url(date_str)
        try:
            with httpx.Client(timeout=10) as client:
                response = client.head(url)
                if response.status_code == 200:
                    log_info(f"Found newer Full tar.gz: {date_str}")
                    return date_str
        except httpx.RequestError:
            pass
        current -= timedelta(days=1)

    return None


def _append_daily_updates(config: Config) -> int:
    """Append daily tar.gz files since last_merged.

    Returns the number of daily files synced.
    """
    synced_count = 0
    for date_str in find_ncbi_daily_dates_to_sync(config):
        if append_daily_tar_gz(config, date_str):
            synced_count += 1
    return synced_count


def sync_ncbi_tar(config: Config, force_full: bool = False) -> None:
    """Sync NCBI SRA Metadata tar with latest data.

    1. If tar doesn't exist or force_full: download Full, then append dailies
    2. If a newer Full is available: download Full, then append dailies
    3. Otherwise: append daily tar.gz files since last sync
    """
    tar_path = get_ncbi_tar_path(config)

    if force_full or not tar_path.exists():
        # Find latest Full tar.gz
        full_date = find_latest_ncbi_full_date()
        if full_date is None:
            log_warn("Could not find NCBI Full tar.gz")
            return
        download_full_tar_gz(config, full_date)
        # Append dailies from Full date to today
        synced_count = _append_daily_updates(config)
        if synced_count > 0:
            log_info(f"Appended {synced_count} daily tar.gz files after Full")
        return

    # Check if a newer Full tar.gz is available
    newer_full_date = _check_for_newer_full(config)
    if newer_full_date is not None:
        log_info(f"Downloading newer Full tar.gz: {newer_full_date}")
        download_full_tar_gz(config, newer_full_date)
        # Append dailies from Full date to today
        synced_count = _append_daily_updates(config)
        if synced_count > 0:
            log_info(f"Appended {synced_count} daily tar.gz files after Full")
        return

    # Append daily tar.gz files
    synced_count = _append_daily_updates(config)
    log_info(f"Synced {synced_count} daily tar.gz files")
