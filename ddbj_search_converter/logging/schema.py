from datetime import date, datetime
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ddbj_search_converter.config import Config

# log_level usage:
# - DEBUG: Detailed debug info (variable values, internal state)
# - INFO: Normal progress, successful completion
# - WARNING: Problem occurred but processing succeeded
# - ERROR: Failed but processing continues (e.g., single record conversion failure)
# - CRITICAL: Fatal, processing stops
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# lifecycle is expressed in the extra field:
# - lifecycle="start": run started
# - lifecycle="end": run completed successfully
# - lifecycle="failed": run failed
Lifecycle = Literal["start", "end", "failed"]


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
