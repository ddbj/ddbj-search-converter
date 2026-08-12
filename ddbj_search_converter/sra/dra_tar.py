"""\
DRA Metadata tar builder module.

Creates and updates DRA_Metadata.tar from DRA XML files.

DRA XML files are located at:
    {DRA_BASE_PATH}/fastq/{submission[:6]}/{submission}/{submission}.{type}.xml

The tar structure is flattened to match NCBI format:
    {submission}/{submission}.{type}.xml

Uses tar command with file list for fast bulk operations.
"""

import subprocess
import tarfile
from collections.abc import Iterable, Iterator
from datetime import date, timedelta
from pathlib import Path

import duckdb

from ddbj_search_converter.config import (
    DEFAULT_MARGIN_DAYS,
    DRA_BASE_PATH,
    DRA_DB_FILE_NAME,
    DRA_LAST_UPDATED_FILE_NAME,
    DRA_TAR_FILE_NAME,
    TODAY,
    Config,
)
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.sra.paths import get_sra_tar_dir
from ddbj_search_converter.sra_accessions_tab import UPDATED_SUBMISSION_WHERE

XML_TYPES = ["submission", "study", "experiment", "run", "sample", "analysis"]

# XML の配置 (.../fastq/{prefix}/{submission}/{submission}.{type}.xml) を NCBI 形式
# ({submission}/{submission}.{type}.xml) に潰す。tar の --transform に渡す。
TRANSFORM_PATTERN = r"s|.*/fastq/[^/]*/\([^/]*\)/|\1/|"


def get_dra_tar_path(config: Config) -> Path:
    """Get the path to the DRA Metadata tar file."""
    return get_sra_tar_dir(config).joinpath(DRA_TAR_FILE_NAME)


def get_dra_last_updated_path(config: Config) -> Path:
    """Get the path to the dra_last_updated.txt file."""
    return get_sra_tar_dir(config).joinpath(DRA_LAST_UPDATED_FILE_NAME)


def get_dra_accessions_db_path(config: Config) -> Path:
    """Get the path to the DRA Accessions DuckDB."""
    return config.const_dir.joinpath("sra", DRA_DB_FILE_NAME)


def get_dra_xml_dir_path(submission: str) -> Path:
    """Get the directory path for a DRA submission's XML files."""
    # fastq/{submission[:6]}/{submission}/
    return DRA_BASE_PATH.joinpath("fastq", submission[:6], submission)


def iter_all_dra_submissions(config: Config) -> Iterator[str]:
    """Iterate over all DRA submission IDs from DRA_Accessions DB.

    Only returns submissions where Type is 'SUBMISSION'.

    Raises:
        FileNotFoundError: If DRA Accessions DB is not found.
    """
    db_path = get_dra_accessions_db_path(config)
    if not db_path.exists():
        raise FileNotFoundError(f"DRA Accessions DB not found: {db_path}")

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
    config: Config, since_date: date, margin_days: int = DEFAULT_MARGIN_DAYS
) -> Iterator[str]:
    """Iterate over DRA submissions updated or released since a given date.

    Shares its condition with JSONL generation via ``UPDATED_SUBMISSION_WHERE``;
    a submission missing from the tar cannot produce JSONL at all.

    Args:
        config: Config instance
        since_date: Date to check from (exclusive)
        margin_days: Safety margin in days to catch delayed updates

    Raises:
        FileNotFoundError: If DRA Accessions DB is not found.
    """
    db_path = get_dra_accessions_db_path(config)
    if not db_path.exists():
        raise FileNotFoundError(f"DRA Accessions DB not found: {db_path}")

    # Apply safety margin
    check_date = since_date - timedelta(days=margin_days)
    check_date_str = check_date.strftime("%Y-%m-%d")

    with duckdb.connect(db_path, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT Submission
            FROM accessions
            WHERE {UPDATED_SUBMISSION_WHERE}
            ORDER BY Submission
            """,
            [check_date_str],
        ).fetchall()

    for row in rows:
        yield row[0]


def collect_xml_files_for_submission(submission: str) -> list[str]:
    """Collect existing XML file paths for a submission.

    Uses os.listdir() once to minimize Lustre inode access.
    """
    xml_dir = get_dra_xml_dir_path(submission)
    try:
        existing_files = {p.name for p in xml_dir.iterdir()}
    except FileNotFoundError:
        return []

    result = []
    for xml_type in XML_TYPES:
        filename = f"{submission}.{xml_type}.xml"
        if filename in existing_files:
            result.append(str(xml_dir.joinpath(filename)))

    return result


def collect_tar_submissions(tar_path: Path) -> set[str]:
    """tar に含まれる submission 名を集める。

    メンバ名は ``{submission}/{submission}.{type}.xml`` の形。TarInfo を全部
    保持すると 10 万件規模でメモリを食うので、名前だけ拾って捨てる。
    """
    submissions: set[str] = set()
    with tarfile.open(tar_path, "r") as tar:
        for member in tar:
            name = member.name
            slash_pos = name.find("/")
            if slash_pos > 0:
                submissions.add(name[:slash_pos])

    return submissions


def _append_submissions_to_tar(
    tar_path: Path,
    submissions: Iterable[str],
    file_list_name: str,
) -> tuple[int, int]:
    """submission の XML を一時 tar にまとめ、既存 tar へ連結する。

    Args:
        tar_path: 連結先の tar
        submissions: 取り込む submission
        file_list_name: 一時ファイルリストの名前 (呼び出し元ごとに変えて衝突を避ける)

    Returns:
        (取り込んだ submission 数, ファイル数)。対象が無ければ ``(0, 0)`` を返し tar に触れない。
    """
    tar_dir = tar_path.parent
    file_list_path = tar_dir.joinpath(file_list_name)
    submission_count = 0
    total_files = 0

    try:
        with file_list_path.open("w", encoding="utf-8") as f:
            for submission in submissions:
                files = collect_xml_files_for_submission(submission)
                if files:
                    submission_count += 1
                    total_files += len(files)
                    f.writelines(f"{src_path}\n" for src_path in files)

        if total_files == 0:
            return 0, 0

        log_info(f"collected {submission_count} submissions ({total_files} files)")

        tmp_tar_path = tar_dir.joinpath(f"{file_list_path.stem}.tar")
        try:
            subprocess.run(
                ["tar", "-cf", str(tmp_tar_path), "--transform", TRANSFORM_PATTERN, "-T", str(file_list_path)],
                check=True,
            )
            subprocess.run(["tar", "-Af", str(tar_path), str(tmp_tar_path)], check=True)
        finally:
            tmp_tar_path.unlink(missing_ok=True)
    finally:
        file_list_path.unlink(missing_ok=True)

    return submission_count, total_files


def build_dra_tar(config: Config) -> None:
    """Build DRA Metadata tar from scratch using tar command.

    Creates a file list and uses tar with --transform for fast bulk operation.
    """
    tar_path = get_dra_tar_path(config)
    tar_dir = tar_path.parent
    tar_dir.mkdir(parents=True, exist_ok=True)

    log_info(f"building dra tar: {tar_path}")

    # Remove existing tar file
    if tar_path.exists():
        tar_path.unlink()

    # Collect all files and generate file list
    file_list_path = tar_dir.joinpath("dra_files.txt")
    total_files = 0
    submission_count = 0

    log_info("collecting dra xml files...")
    with file_list_path.open("w", encoding="utf-8") as f:
        for submission in iter_all_dra_submissions(config):
            files = collect_xml_files_for_submission(submission)
            if files:
                submission_count += 1
                total_files += len(files)
                f.writelines(f"{src_path}\n" for src_path in files)
                if submission_count % 10000 == 0:
                    log_info(f"collected {submission_count} submissions ({total_files} files)")

    log_info(f"collected {submission_count} submissions ({total_files} files)")

    if total_files == 0:
        file_list_path.unlink(missing_ok=True)
        raise FileNotFoundError("No DRA XML files found")

    log_info("creating tar archive...")
    subprocess.run(
        ["tar", "-cf", str(tar_path), "--transform", TRANSFORM_PATTERN, "-T", str(file_list_path)],
        check=True,
    )

    # Cleanup
    file_list_path.unlink(missing_ok=True)

    log_info(f"dra tar built: {submission_count} submissions, {total_files} files")

    # Update last_updated file
    last_updated_path = get_dra_last_updated_path(config)
    last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
    log_info(f"updated dra_last_updated: {TODAY.strftime('%Y%m%d')}")

    # Build DRA file index
    from ddbj_search_converter.sra.dra_file_index import build_dra_file_index

    build_dra_file_index(config)


def sync_dra_tar(config: Config) -> None:
    """Sync DRA Metadata tar with latest data.

    If tar doesn't exist, builds from scratch.
    Otherwise, creates temp tar for updated submissions and concatenates.
    """
    tar_path = get_dra_tar_path(config)
    last_updated_path = get_dra_last_updated_path(config)

    if not tar_path.exists():
        log_info("dra tar does not exist, building from scratch")
        build_dra_tar(config)
        return

    # Get last update date
    if last_updated_path.exists():
        last_updated_str = last_updated_path.read_text().strip()
        last_updated = date(int(last_updated_str[:4]), int(last_updated_str[4:6]), int(last_updated_str[6:8]))
    else:
        # If no last_updated file, rebuild from scratch
        log_info("no dra_last_updated file found, building from scratch")
        build_dra_tar(config)
        return

    log_info(f"syncing dra tar since: {last_updated}")

    submission_count, total_files = _append_submissions_to_tar(
        tar_path,
        iter_updated_dra_submissions(config, last_updated),
        "dra_update_files.txt",
    )

    if total_files == 0:
        log_info("no updated dra submissions found")
    else:
        log_info(f"dra tar synced: {submission_count} submissions, {total_files} files")

    # Update last_updated file
    last_updated_path.write_text(TODAY.strftime("%Y%m%d"))
    log_info(f"updated dra_last_updated: {TODAY.strftime('%Y%m%d')}")

    # Build DRA file index
    from ddbj_search_converter.sra.dra_file_index import build_dra_file_index

    build_dra_file_index(config)


def repair_dra_tar(config: Config) -> None:
    """DRA_Accessions.tab にあって tar に無い submission の XML を取り込む。

    差分同期は Accessions.tab の日付列に依存するので、日付が動かないまま状態が
    変わった submission を取りこぼしうる。tar は追記でしか育たないため、取りこぼしは
    以後の同期でも回収されず累積する。集合差分で埋めるのがこの関数の役割。

    ``dra_last_updated.txt`` は進めない。通常の差分同期の起点を動かすと、
    修復ついでに未処理の期間を飛ばしてしまうため。
    DRA ファイルインデックスも作り直さない (tar と独立に全 submission を走査しており、
    tar に入っていない submission の情報も既に持っている)。

    Raises:
        FileNotFoundError: tar が存在しない場合 (先に ``build_dra_tar`` が必要)。
    """
    tar_path = get_dra_tar_path(config)
    if not tar_path.exists():
        raise FileNotFoundError(f"DRA tar does not exist: {tar_path}")

    log_info(f"scanning tar members: {tar_path}")
    in_tar = collect_tar_submissions(tar_path)
    log_info(f"tar contains {len(in_tar)} submissions")

    missing = [sub for sub in iter_all_dra_submissions(config) if sub not in in_tar]
    if not missing:
        log_info("no missing submissions found")
        return

    log_info(f"found {len(missing)} submissions missing from tar")

    submission_count, total_files = _append_submissions_to_tar(tar_path, missing, "dra_repair_files.txt")

    if total_files == 0:
        log_info("no xml files found for the missing submissions")
        return

    log_info(f"dra tar repaired: {submission_count} submissions, {total_files} files")
