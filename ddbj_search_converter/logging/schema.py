from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ddbj_search_converter.config import Config

# log_level usage:
# - DEBUG: Detailed info for debugging (skipped items, pattern mismatches). Not shown in stderr.
# - INFO: Progress, completion, statistics. Shown in stderr.
# - WARNING: Succeeded but incomplete (parsed with empty/default values). Shown in stderr.
# - ERROR: Failed and skipped (single file/record failure). Shown in stderr.
# - CRITICAL: Fatal, processing stops (raises exception). Shown in stderr.
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# lifecycle is expressed in the extra field:
# - lifecycle="start": run started
# - lifecycle="end": run completed successfully
# - lifecycle="failed": run failed
Lifecycle = Literal["start", "end", "failed"]


class DebugCategory(str, Enum):
    """DEBUG log category for aggregation in log.duckdb."""
    # Configuration
    CONFIG = "config"

    # ID pattern mismatch
    INVALID_BIOSAMPLE_ID = "invalid_biosample_id"
    INVALID_BIOPROJECT_ID = "invalid_bioproject_id"
    INVALID_GCF_FORMAT = "invalid_gcf_format"
    INVALID_WGS_RANGE = "invalid_wgs_range"

    # File/resource related
    FILE_NOT_FOUND = "file_not_found"
    EMPTY_RESULT = "empty_result"

    # Filter related
    BLACKLIST_NO_MATCH = "blacklist_no_match"

    # Parse related
    PARSE_FALLBACK = "parse_fallback"

    # Normalize function failures (bp.py)
    NORMALIZE_BIOSAMPLE_SET_ID = "normalize_biosample_set_id"
    NORMALIZE_LOCUS_TAG_PREFIX = "normalize_locus_tag_prefix"
    NORMALIZE_LOCAL_ID = "normalize_local_id"
    NORMALIZE_ORGANIZATION_NAME = "normalize_organization_name"
    NORMALIZE_GRANT_AGENCY = "normalize_grant_agency"

    # Normalize function failures (bs.py)
    NORMALIZE_OWNER_NAME = "normalize_owner_name"
    NORMALIZE_MODEL = "normalize_model"

    # Date fetch failures
    FETCH_DATES_FAILED = "fetch_dates_failed"

    # XML accession collection failure (sra.py)
    XML_ACCESSION_COLLECT_FAILED = "xml_accession_collect_failed"

    # Unsupported external link DB (bp.py)
    UNSUPPORTED_EXTERNAL_LINK_DB = "unsupported_external_link_db"


class Extra(BaseModel):
    """
    Additional structured data for log records.

    Reserved fields have predefined meanings.
    Additional arbitrary fields are allowed via extra="allow".
    """
    model_config = ConfigDict(extra="allow")

    lifecycle: Optional[Lifecycle] = Field(
        default=None,
        description="Run lifecycle stage: start, end, or failed",
    )
    file: Optional[str] = Field(
        default=None,
        description="File path being processed",
        examples=["/path/to/data.xml"],
    )
    accession: Optional[str] = Field(
        default=None,
        description="Accession ID being processed",
        examples=["PRJDB12345", "DRR000001"],
    )
    index: Optional[str] = Field(
        default=None,
        description="Elasticsearch index name",
        examples=["bioproject", "sra-run"],
    )
    table: Optional[str] = Field(
        default=None,
        description="Database table name",
        examples=["accessions", "relation"],
    )
    row: Optional[int] = Field(
        default=None,
        description="Row number in file or table",
        ge=0,
    )
    debug_category: Optional[DebugCategory] = Field(
        default=None,
        description="DEBUG log category for aggregation",
        examples=["invalid_biosample_id", "file_not_found"],
    )
    source: Optional[str] = Field(
        default=None,
        description="Data source identifier",
        examples=["ncbi", "ddbj", "sra", "dra"],
    )
    relation_type: Optional[str] = Field(
        default=None,
        description="Type of relation being processed",
        examples=["umbrella", "hum_id", "geo"],
    )
    count: Optional[int] = Field(
        default=None,
        description="Count of items (for summary logs)",
        ge=0,
    )


class LoggerContext(BaseModel):
    """Runtime context for logger."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_name: str = Field(
        ...,
        description="Name of the run",
    )
    run_id: str = Field(
        ...,
        description="Unique run identifier: {YYYYMMDD}_{run_name}_{hex4}",
    )
    run_date: date = Field(
        ...,
        description="Run date (TODAY when logger was initialized)",
    )
    log_file: Path = Field(
        ...,
        description="Path to the JSONL log file",
    )
    config: Config = Field(
        ...,
        description="Config instance",
    )


class ErrorInfo(BaseModel):
    """Exception information for error logs."""

    type: str = Field(
        ...,
        description="Exception class name",
        examples=["ValueError", "FileNotFoundError"],
    )
    message: str = Field(
        ...,
        description="Exception message (str(e))",
    )
    traceback: Optional[str] = Field(
        default=None,
        description="Full traceback string",
    )


class LogRecord(BaseModel):
    """Single log record."""

    # timestamp (Asia/Tokyo)
    timestamp: datetime = Field(
        ...,
        description="Log timestamp in Asia/Tokyo timezone",
        examples=["2026-01-13T10:30:00+09:00"],
    )

    # run identifiers
    run_date: date = Field(
        ...,
        description="Run date (TODAY when logger was initialized)",
        examples=["2026-01-13"],
    )
    run_id: str = Field(
        ...,
        description="Unique run identifier: {YYYYMMDD}_{run_name}_{hex4}",
        examples=["20260113_init_dblink_db_a1b2"],
    )
    run_name: str = Field(
        ...,
        description="Name of the run (CLI command name or 'adhoc')",
        examples=["init_dblink_db", "build_sra_dra_accessions_db"],
    )

    # log source (module path)
    source: str = Field(
        ...,
        description="Python module path where log was emitted",
        examples=["ddbj_search_converter.dblink.assembly_and_master"],
    )

    log_level: LogLevel = Field(
        ...,
        description="Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )

    message: Optional[str] = Field(
        default=None,
        description="Human-readable log message",
    )
    error: Optional[ErrorInfo] = Field(
        default=None,
        description="Error information (set when exception occurred)",
    )
    extra: Extra = Field(
        default_factory=Extra,
        description="Additional structured data (lifecycle, file, accession, etc.)",
    )
