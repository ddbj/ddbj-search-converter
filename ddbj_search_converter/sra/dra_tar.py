"""\
DRA Metadata tar builder module.

Creates and updates DRA_Metadata.tar from DRA XML files.

DRA XML files are located at:
    {DRA_BASE_PATH}/fastq/{submission[:6]}/{submission}/{submission}.{type}.xml

The tar structure is flattened to match NCBI format:
    {submission}/{submission}.{type}.xml
"""
import os
import tarfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional, Set

import duckdb

from ddbj_search_converter.config import (DRA_BASE_PATH, DRA_DB_FILE_NAME,
                                          DRA_LAST_UPDATED_FILE_NAME,
                                          DRA_TAR_FILE_NAME, SRA_TAR_DIR_NAME,
                                          TODAY, Config)
from ddbj_search_converter.logging.logger import log_info, log_warn

XML_TYPES = ["submission", "study", "experiment", "run", "sample", "analysis"]


def get_dra_tar_path(config: Config) -> Path:
    """Get the path to the DRA Metadata tar file."""
    return config.const_dir.joinpath(SRA_TAR_DIR_NAME, DRA_TAR_FILE_NAME)


def get_dra_last_updated_path(config: Config) -> Path:
    """Get the path to the dra_last_updated.txt file."""
    return config.const_dir.joinpath(SRA_TAR_DIR_NAME, DRA_LAST_UPDATED_FILE_NAME)


def get_dra_accessions_db_path(config: Config) -> Path:
    """Get the path to the DRA Accessions DuckDB."""
    return config.const_dir.joinpath("sra", DRA_DB_FILE_NAME)


def get_dra_xml_dir_path(submission: str) -> Path:
    """Get the directory path for a DRA submission's XML files."""
    # fastq/{submission[:6]}/{submission}/
    return DRA_BASE_PATH.joinpath(
        "fastq",
        submission[:6],
        submission
    )


def get_dra_xml_file_path(submission: str, xml_type: str) -> Path:
    """Get the path to a specific DRA XML file."""
    return get_dra_xml_dir_path(submission).joinpath(
        f"{submission}.{xml_type}.xml"
    )


def get_tar_member_name(submission: str, xml_type: str) -> str:
    """Get the tar member name for a DRA XML file (flattened NCBI format)."""
    return f"{submission}/{submission}.{xml_type}.xml"


def iter_all_dra_submissions(config: Config) -> Iterator[str]:
    """Iterate over all DRA submission IDs from DRA_Accessions DB.

    Only returns submissions where Type is 'SUBMISSION'.
    """
    db_path = get_dra_accessions_db_path(config)
    if not db_path.exists():
        log_warn(f"DRA Accessions DB not found: {db_path}")
        return

    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT Accession
            FROM accessions
            WHERE Type = 'SUBMISSION'
            ORDER BY Accession
            """
        ).fetchall()

    for row in rows:
        yield row[0]


def iter_updated_dra_submissions(
    config: Config,
    since_date: date,
    margin_days: int = 3
) -> Iterator[str]:
    """Iterate over DRA submissions updated since a given date.

    Args:
        config: Config instance
        since_date: Date to check from (exclusive)
        margin_days: Safety margin in days to catch delayed updates
    """
    db_path = get_dra_accessions_db_path(config)
    if not db_path.exists():
        log_warn(f"DRA Accessions DB not found: {db_path}")
        return

    # Apply safety margin
    check_date = since_date - timedelta(days=margin_days)
    check_date_str = check_date.strftime("%Y-%m-%d")

    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT Accession
            FROM accessions
            WHERE Type = 'SUBMISSION'
              AND Updated >= '{check_date_str}'
            ORDER BY Accession
            """
        ).fetchall()

    for row in rows:
        yield row[0]


def add_submission_to_tar(
    tar: tarfile.TarFile,
    submission: str,
    added_members: Optional[Set[str]] = None
) -> int:
    """Add all XML files for a submission to the tar.

    Uses os.listdir() once per submission instead of exists() per file
    to minimize Lustre inode access.
    """
    if added_members is None:
        added_members = set()

    xml_dir = get_dra_xml_dir_path(submission)
    try:
        existing_files = set(os.listdir(xml_dir))
    except FileNotFoundError:
        return 0

    count = 0
    for xml_type in XML_TYPES:
        filename = f"{submission}.{xml_type}.xml"
        if filename not in existing_files:
            continue

        tar_name = get_tar_member_name(submission, xml_type)
        if tar_name in added_members:
            continue

        tar.add(xml_dir.joinpath(filename), arcname=tar_name)
        added_members.add(tar_name)
        count += 1

    return count


def build_dra_tar(config: Config) -> None:
    """Build DRA Metadata tar from scratch.

    Iterates over all DRA submissions and adds their XML files.
    """
    tar_path = get_dra_tar_path(config)
    tar_dir = tar_path.parent
    tar_dir.mkdir(parents=True, exist_ok=True)

    log_info(f"Building DRA tar: {tar_path}")

    # Remove existing tar file
    if tar_path.exists():
        tar_path.unlink()

    added_members: Set[str] = set()
    total_files = 0
    submission_count = 0

    with tarfile.open(tar_path, "w") as tar:
        for submission in iter_all_dra_submissions(config):
            count = add_submission_to_tar(tar, submission, added_members)
            if count > 0:
                submission_count += 1
                total_files += count
                if submission_count % 1000 == 0:
                    log_info(f"Added {submission_count} submissions ({total_files} files)")

    log_info(f"DRA tar built: {submission_count} submissions, {total_files} files")

    # Update last_updated file
    last_updated_path = get_dra_last_updated_path(config)
    last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
    log_info(f"Updated dra_last_updated: {TODAY.strftime('%Y%m%d')}")


def sync_dra_tar(config: Config) -> None:
    """Sync DRA Metadata tar with latest data.

    If tar doesn't exist, builds from scratch.
    Otherwise, appends XML files for submissions updated since last sync.
    """
    tar_path = get_dra_tar_path(config)
    last_updated_path = get_dra_last_updated_path(config)

    if not tar_path.exists():
        log_info("DRA tar does not exist, building from scratch")
        build_dra_tar(config)
        return

    # Get last update date
    if last_updated_path.exists():
        last_updated_str = last_updated_path.read_text().strip()
        last_updated = date(
            int(last_updated_str[:4]),
            int(last_updated_str[4:6]),
            int(last_updated_str[6:8])
        )
    else:
        # If no last_updated file, rebuild from scratch
        log_warn("No dra_last_updated file found, building from scratch")
        build_dra_tar(config)
        return

    log_info(f"Syncing DRA tar since: {last_updated}")

    total_files = 0
    submission_count = 0

    with tarfile.open(tar_path, "a") as tar:
        for submission in iter_updated_dra_submissions(config, last_updated):
            count = add_submission_to_tar(tar, submission)
            if count > 0:
                submission_count += 1
                total_files += count

    log_info(f"DRA tar synced: {submission_count} submissions, {total_files} files")

    # Update last_updated file
    last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
    log_info(f"Updated dra_last_updated: {TODAY.strftime('%Y%m%d')}")
