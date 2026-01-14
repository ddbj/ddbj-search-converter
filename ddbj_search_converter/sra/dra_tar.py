"""\
DRA Metadata tar builder module.

Creates and updates DRA_Metadata.tar from DRA XML files.

DRA XML files are located at:
    {DRA_BASE_PATH}/fastq/{submission[:6]}/{submission}/{submission}.{type}.xml

The tar structure is flattened to match NCBI format:
    {submission}/{submission}.{type}.xml

Uses tar command with file list for fast bulk operations.
"""
import os
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, List

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


def collect_xml_files_for_submission(submission: str) -> List[str]:
    """Collect existing XML file paths for a submission.

    Uses os.listdir() once to minimize Lustre inode access.
    """
    xml_dir = get_dra_xml_dir_path(submission)
    try:
        existing_files = set(os.listdir(xml_dir))
    except FileNotFoundError:
        return []

    result = []
    for xml_type in XML_TYPES:
        filename = f"{submission}.{xml_type}.xml"
        if filename in existing_files:
            result.append(str(xml_dir.joinpath(filename)))

    return result


def build_dra_tar(config: Config) -> None:
    """Build DRA Metadata tar from scratch using tar command.

    Creates a file list and uses tar with --transform for fast bulk operation.
    """
    tar_path = get_dra_tar_path(config)
    tar_dir = tar_path.parent
    tar_dir.mkdir(parents=True, exist_ok=True)

    log_info(f"Building DRA tar: {tar_path}")

    # Remove existing tar file
    if tar_path.exists():
        tar_path.unlink()

    # Collect all files and generate file list
    file_list_path = tar_dir.joinpath("dra_files.txt")
    total_files = 0
    submission_count = 0

    log_info("Collecting DRA XML files...")
    with open(file_list_path, "w", encoding="utf-8") as f:
        for submission in iter_all_dra_submissions(config):
            files = collect_xml_files_for_submission(submission)
            if files:
                submission_count += 1
                total_files += len(files)
                for src_path in files:
                    f.write(f"{src_path}\n")
                if submission_count % 10000 == 0:
                    log_info(f"Collected {submission_count} submissions ({total_files} files)")

    log_info(f"Collected {submission_count} submissions ({total_files} files)")

    if total_files == 0:
        log_warn("No DRA XML files found")
        file_list_path.unlink(missing_ok=True)
        return

    # Build tar using tar command with --transform
    # Transform: /usr/local/resources/dra/fastq/DRA000/DRA000001/DRA000001.submission.xml
    #         -> DRA000001/DRA000001.submission.xml
    log_info("Creating tar archive...")
    transform_pattern = r"s|.*/fastq/[^/]*/\([^/]*\)/|\1/|"
    cmd = f'tar -cf "{tar_path}" --transform "{transform_pattern}" -T "{file_list_path}"'
    subprocess.run(cmd, shell=True, check=True)

    # Cleanup
    file_list_path.unlink(missing_ok=True)

    log_info(f"DRA tar built: {submission_count} submissions, {total_files} files")

    # Update last_updated file
    last_updated_path = get_dra_last_updated_path(config)
    last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
    log_info(f"Updated dra_last_updated: {TODAY.strftime('%Y%m%d')}")


def sync_dra_tar(config: Config) -> None:
    """Sync DRA Metadata tar with latest data.

    If tar doesn't exist, builds from scratch.
    Otherwise, creates temp tar for updated submissions and concatenates.
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

    # Collect updated files
    tar_dir = tar_path.parent
    file_list_path = tar_dir.joinpath("dra_update_files.txt")
    total_files = 0
    submission_count = 0

    with open(file_list_path, "w", encoding="utf-8") as f:
        for submission in iter_updated_dra_submissions(config, last_updated):
            files = collect_xml_files_for_submission(submission)
            if files:
                submission_count += 1
                total_files += len(files)
                for src_path in files:
                    f.write(f"{src_path}\n")

    if total_files == 0:
        log_info("No updated DRA submissions found")
        file_list_path.unlink(missing_ok=True)
        # Update last_updated file
        last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
        return

    log_info(f"Found {submission_count} updated submissions ({total_files} files)")

    # Create temp tar with updated files
    tmp_tar_path = tar_dir.joinpath("dra_update.tar")
    transform_pattern = r"s|.*/fastq/[^/]*/\([^/]*\)/|\1/|"
    cmd = f'tar -cf "{tmp_tar_path}" --transform "{transform_pattern}" -T "{file_list_path}"'
    subprocess.run(cmd, shell=True, check=True)

    # Concatenate temp tar to main tar
    cmd = f'tar -Af "{tar_path}" "{tmp_tar_path}"'
    subprocess.run(cmd, shell=True, check=True)

    # Cleanup
    file_list_path.unlink(missing_ok=True)
    tmp_tar_path.unlink(missing_ok=True)

    log_info(f"DRA tar synced: {submission_count} submissions, {total_files} files")

    # Update last_updated file
    last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
    log_info(f"Updated dra_last_updated: {TODAY.strftime('%Y%m%d')}")
