"""\
NCBI SRA Metadata tar.gz sync module.

Downloads and processes NCBI SRA Metadata tar.gz files:
- Full tar.gz: Creates new tar file from scratch
- Daily tar.gz: Appends entries to existing tar file

The tar is rebuilt whenever a Full newer than the one it was built from becomes
available, so its size stays bounded by one Full plus about a month of dailies.

Uses aria2c for fast, reliable downloads with:
- Multi-connection parallel download (-x 16)
- Automatic retry (--max-tries=10)
- Resume capability (-c)
"""

import subprocess
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import httpx

from ddbj_search_converter.config import (
    NCBI_BASE_FULL_FILE_NAME,
    NCBI_LAST_MERGED_FILE_NAME,
    NCBI_SRA_METADATA_BASE_URL,
    NCBI_SRA_METADATA_LOCAL_PATH,
    NCBI_SRA_TAR_FILE_NAME,
    TODAY,
    Config,
)
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.sra.paths import get_sra_tar_dir


def get_ncbi_tar_path(config: Config) -> Path:
    """Get the path to the NCBI SRA Metadata tar file."""
    return get_sra_tar_dir(config).joinpath(NCBI_SRA_TAR_FILE_NAME)


def get_ncbi_last_merged_path(config: Config) -> Path:
    """Get the path to the ncbi_last_merged.txt file."""
    return get_sra_tar_dir(config).joinpath(NCBI_LAST_MERGED_FILE_NAME)


def get_ncbi_base_full_path(config: Config) -> Path:
    """Get the path to the ncbi_base_full.txt file."""
    return get_sra_tar_dir(config).joinpath(NCBI_BASE_FULL_FILE_NAME)


def get_ncbi_full_tar_gz_url(date_str: str) -> str:
    """Get the URL for NCBI SRA Metadata Full tar.gz."""
    return f"{NCBI_SRA_METADATA_BASE_URL}/NCBI_SRA_Metadata_Full_{date_str}.tar.gz"


def get_ncbi_daily_tar_gz_url(date_str: str) -> str:
    """Get the URL for NCBI SRA Metadata daily tar.gz."""
    return f"{NCBI_SRA_METADATA_BASE_URL}/NCBI_SRA_Metadata_{date_str}.tar.gz"


def get_ncbi_full_tar_gz_local_path(date_str: str) -> Path:
    """Get the local mirror path for NCBI SRA Metadata Full tar.gz."""
    return NCBI_SRA_METADATA_LOCAL_PATH / f"NCBI_SRA_Metadata_Full_{date_str}.tar.gz"


def get_ncbi_daily_tar_gz_local_path(date_str: str) -> Path:
    """Get the local mirror path for NCBI SRA Metadata daily tar.gz."""
    return NCBI_SRA_METADATA_LOCAL_PATH / f"NCBI_SRA_Metadata_{date_str}.tar.gz"


FULL_LOOKBACK_DAYS = 60


def find_latest_ncbi_full_date(max_days_back: int = FULL_LOOKBACK_DAYS) -> str | None:
    """Find the latest available NCBI Full tar.gz.

    First checks the local mirror, then falls back to HTTP HEAD.
    NCBI publishes Full tar.gz approximately once a month.
    """
    # Try local mirror first
    for days_back in range(max_days_back):
        check_date = TODAY - timedelta(days=days_back)
        date_str = check_date.strftime("%Y%m%d")
        local_path = get_ncbi_full_tar_gz_local_path(date_str)
        if local_path.exists():
            log_info(f"found full tar.gz in local mirror: {local_path}")
            return date_str

    # Fall back to HTTP HEAD
    with httpx.Client(timeout=10) as client:
        for days_back in range(max_days_back):
            check_date = TODAY - timedelta(days=days_back)
            date_str = check_date.strftime("%Y%m%d")
            url = get_ncbi_full_tar_gz_url(date_str)

            try:
                response = client.head(url)
                if response.status_code == 200:
                    return date_str
            except httpx.RequestError:
                continue

    return None


def _read_date_file(path: Path) -> date | None:
    if not path.exists():
        return None
    date_str = path.read_text().strip()
    return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))


def _get_last_merged_date(config: Config) -> date | None:
    """Get the last merged date from ncbi_last_merged.txt."""
    return _read_date_file(get_ncbi_last_merged_path(config))


def _get_base_full_date(config: Config) -> date | None:
    """Get the date of the Full tar.gz the current tar was built from."""
    return _read_date_file(get_ncbi_base_full_path(config))


def find_ncbi_daily_dates_to_sync(config: Config, max_days_back: int = 30) -> Iterator[str]:
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


def _download_with_aria2c(url: str, output_path: Path) -> None:
    """Download a file using aria2c with optimized settings.

    Args:
        url: URL to download
        output_path: Path to save the downloaded file
    """
    output_dir = output_path.parent
    output_name = output_path.name

    cmd = [
        "aria2c",
        "-x",
        "16",  # 16 connections for parallel download
        "-s",
        "16",  # 16 splits
        "--max-tries=10",  # Retry up to 10 times
        "--retry-wait=5",  # Wait 5 seconds between retries
        "--timeout=600",  # 10 minute timeout
        "--connect-timeout=60",  # 60 second connection timeout
        "-c",  # Enable resume
        "-d",
        str(output_dir),
        "-o",
        output_name,
        url,
    ]
    subprocess.run(cmd, check=True)


def download_full_tar_gz(config: Config, date_str: str) -> None:
    """Download NCBI Full tar.gz and create new tar file.

    Uses local mirror if available, otherwise aria2c for download.
    The Full is decompressed into a temp file and swapped in only on success,
    so a failed run leaves the existing tar untouched.
    """
    tar_path = get_ncbi_tar_path(config)
    tar_dir = get_sra_tar_dir(config)
    tar_dir.mkdir(parents=True, exist_ok=True)
    tmp_tar_path = tar_dir.joinpath(f"{tar_path.name}.tmp")

    local_path = get_ncbi_full_tar_gz_local_path(date_str)

    try:
        if local_path.exists():
            # Use local mirror: decompress directly (avoid copying 15GB+ file)
            log_info(f"using local mirror: {local_path}")
            log_info(f"output: {tar_path}")
            log_info("decompressing with pigz...")
            cmd = f'pigz -d -c "{local_path}" > "{tmp_tar_path}"'
            subprocess.run(cmd, shell=True, check=True)
        else:
            # Fall back to aria2c download
            url = get_ncbi_full_tar_gz_url(date_str)
            tar_gz_path = tar_dir.joinpath(f"ncbi_full_{date_str}.tar.gz")

            log_info(f"downloading ncbi full tar.gz: {url}")
            log_info(f"output: {tar_path}")

            try:
                _download_with_aria2c(url, tar_gz_path)

                log_info("decompressing with pigz...")
                cmd = f'pigz -d -c "{tar_gz_path}" > "{tmp_tar_path}"'
                subprocess.run(cmd, shell=True, check=True)
            finally:
                tar_gz_path.unlink(missing_ok=True)

        # last_merged goes back to the Full date before the swap: if the run dies
        # in between, the old tar merely gets the following dailies appended again
        # (last entry wins). The opposite order would skip those dailies for good.
        get_ncbi_last_merged_path(config).write_text(date_str)
        tmp_tar_path.replace(tar_path)
    finally:
        tmp_tar_path.unlink(missing_ok=True)

    get_ncbi_base_full_path(config).write_text(date_str)
    log_info(f"updated base_full and last_merged: {date_str}")


def append_daily_tar_gz(config: Config, date_str: str) -> bool:
    """Append NCBI daily tar.gz entries to existing tar file.

    Uses local mirror if available, otherwise aria2c for download.
    Returns True if successful, False if daily tar.gz not found.
    """
    tar_path = get_ncbi_tar_path(config)

    if not tar_path.exists():
        raise FileNotFoundError(f"NCBI tar file does not exist: {tar_path}")

    local_path = get_ncbi_daily_tar_gz_local_path(date_str)
    tmp_tar_path = tar_path.parent.joinpath(f"daily_{date_str}.tar")

    if local_path.exists():
        # Use local mirror
        log_info(f"appending daily tar.gz from local mirror: {local_path}")

        try:
            cmd = f'pigz -d -c "{local_path}" > "{tmp_tar_path}"'
            subprocess.run(cmd, shell=True, check=True)

            cmd = f'tar -Af "{tar_path}" "{tmp_tar_path}"'
            subprocess.run(cmd, shell=True, check=True)
        finally:
            tmp_tar_path.unlink(missing_ok=True)
    else:
        # Fall back to HTTP check + aria2c download
        url = get_ncbi_daily_tar_gz_url(date_str)

        try:
            with httpx.Client(timeout=10) as client:
                response = client.head(url)
                if response.status_code != 200:
                    log_info(f"daily tar.gz not found: {url}")
                    return False
        except httpx.RequestError as e:
            log_info(f"failed to check daily tar.gz: {e}")
            return False

        log_info(f"appending daily tar.gz: {url}")

        tar_gz_path = tar_path.parent.joinpath(f"daily_{date_str}.tar.gz")

        try:
            _download_with_aria2c(url, tar_gz_path)

            cmd = f'pigz -d -c "{tar_gz_path}" > "{tmp_tar_path}"'
            subprocess.run(cmd, shell=True, check=True)

            cmd = f'tar -Af "{tar_path}" "{tmp_tar_path}"'
            subprocess.run(cmd, shell=True, check=True)
        finally:
            tar_gz_path.unlink(missing_ok=True)
            tmp_tar_path.unlink(missing_ok=True)

    # Update last_merged file
    last_merged_path = get_ncbi_last_merged_path(config)
    last_merged_path.write_text(date_str)
    log_info(f"updated last_merged: {date_str}")

    return True


def _check_for_newer_full(config: Config) -> str | None:
    """Check if a Full tar.gz newer than the one the tar was built from is available.

    The comparison is against base_full, not last_merged: a Full shows up a few
    days after its date, by which time the dailies have already pushed
    last_merged past it.

    Returns the date string of the latest such Full, None otherwise. A tar
    without base_full has an unknown base, so any available Full counts.
    """
    base_full = _get_base_full_date(config)
    if base_full is None:
        return find_latest_ncbi_full_date()

    days_since_base = (TODAY - base_full).days
    if days_since_base <= 0:
        return None
    newer_full_date = find_latest_ncbi_full_date(max_days_back=min(days_since_base, FULL_LOOKBACK_DAYS))
    if newer_full_date is not None:
        log_info(f"found full tar.gz newer than base {base_full:%Y%m%d}: {newer_full_date}")
    return newer_full_date


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
            raise FileNotFoundError("Could not find NCBI Full tar.gz")
        download_full_tar_gz(config, full_date)
        # Append dailies from Full date to today
        synced_count = _append_daily_updates(config)
        if synced_count > 0:
            log_info(f"appended {synced_count} daily tar.gz files after full")
        return

    # Check if a newer Full tar.gz is available
    newer_full_date = _check_for_newer_full(config)
    if newer_full_date is not None:
        log_info(f"downloading newer full tar.gz: {newer_full_date}")
        download_full_tar_gz(config, newer_full_date)
        # Append dailies from Full date to today
        synced_count = _append_daily_updates(config)
        if synced_count > 0:
            log_info(f"appended {synced_count} daily tar.gz files after full")
        return

    # Append daily tar.gz files
    synced_count = _append_daily_updates(config)
    log_info(f"synced {synced_count} daily tar.gz files")
