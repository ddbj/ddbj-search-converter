"""Elasticsearch bulk insert operations."""

import json
import re
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from elastic_transport import ConnectionTimeout
from pydantic import BaseModel

from ddbj_search_converter.config import Config
from ddbj_search_converter.es._error_utils import sanitize_error_info as _sanitize_error_info
from ddbj_search_converter.es._error_utils import sanitize_value as _sanitize_value
from ddbj_search_converter.es.client import check_index_exists, get_es_client, refresh_index, set_refresh_interval
from ddbj_search_converter.es.index import IndexName
from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS
from ddbj_search_converter.logging.logger import log_warn
from elasticsearch import helpers

__all__ = [
    "BulkInsertResult",
    "_extract_prefix",
    "_sanitize_error_info",
    "_sanitize_value",
    "bulk_insert_from_dir",
    "bulk_insert_jsonl",
    "generate_bulk_actions",
]


class BulkInsertResult(BaseModel):
    """Result of a bulk insert operation."""

    index: str
    total_docs: int
    success_count: int
    error_count: int
    errors: list[dict[str, Any]]


def _extract_prefix(identifier: str) -> str:
    """Extract alphabetic prefix from an identifier.

    >>> _extract_prefix("JGAS000001")
    'JGAS'
    >>> _extract_prefix("AGDD_000001")
    'AGDD'
    """
    m = re.match(r"^[A-Za-z]+", identifier)
    return m.group(0) if m else ""


def generate_bulk_actions(
    jsonl_file: Path,
    index: str,
    logical_index: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Generate bulk actions from a JSONL file.

    For documents with ``sameAs`` entries whose type matches the target index
    and whose identifier prefix matches the primary identifier, additional
    alias documents are yielded so that Secondary IDs are also retrievable.

    Args:
        jsonl_file: Path to the JSONL file
        index: Target index name (physical name used for ``_index``)
        logical_index: Logical index name for ``sameAs`` type comparison.
            When writing to a dated physical index (e.g. ``jga-study-20260413``),
            pass the logical name (e.g. ``jga-study``) here so that the
            ``sameAs`` type matching still works.  Defaults to *index*.

    Yields:
        Bulk action dictionaries
    """
    type_match_name = logical_index or index
    with jsonl_file.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            doc = json.loads(line)
            identifier = doc.get("identifier")
            if not identifier:
                continue
            yield {
                "_op_type": "index",
                "_index": index,
                "_id": identifier,
                "_source": doc,
            }
            primary_prefix = _extract_prefix(identifier)
            for same_as in doc.get("sameAs", []):
                same_as_id = same_as.get("identifier")
                if (
                    same_as_id
                    and same_as_id != identifier
                    and same_as.get("type") == type_match_name
                    and _extract_prefix(same_as_id) == primary_prefix
                ):
                    yield {
                        "_op_type": "index",
                        "_index": index,
                        "_id": same_as_id,
                        "_source": doc,
                    }


def bulk_insert_jsonl(
    config: Config,
    jsonl_files: list[Path],
    index: IndexName,
    batch_size: int = BULK_INSERT_SETTINGS["batch_size"],
    max_errors: int = 100,
    target_index: str | None = None,
) -> BulkInsertResult:
    """Bulk insert JSONL files into Elasticsearch.

    Args:
        config: Configuration object
        jsonl_files: List of JSONL file paths to insert
        index: Logical index name (used for ``sameAs`` type matching)
        batch_size: Number of documents per bulk request
        max_errors: Maximum number of error details to keep
        target_index: Physical index name to write to.  When ``None``,
            data is written to *index* (the alias / logical name).
            For Blue-Green updates, pass a dated name like
            ``bioproject-20260413``.

    Returns:
        BulkInsertResult with success/error counts and error details

    Raises:
        Exception: If the target index does not exist
    """
    es_client = get_es_client(config)
    write_index = target_index or index

    # ES が過負荷の場合に備えてリトライする
    for attempt in range(3):
        try:
            if not check_index_exists(es_client, write_index):
                raise Exception(f"Index '{write_index}' does not exist.")
            break
        except ConnectionTimeout:
            if attempt == 2:
                raise
            wait = 30 * (attempt + 1)
            log_warn(f"ES connection timed out, retrying in {wait}s (attempt {attempt + 1}/3)")
            time.sleep(wait)

    total_docs = 0
    success_count = 0
    error_count = 0
    errors: list[dict[str, Any]] = []

    # Disable refresh during bulk insert for better performance
    set_refresh_interval(es_client, write_index, BULK_INSERT_SETTINGS["bulk_refresh_interval"])

    try:
        for jsonl_file in jsonl_files:
            logical = index if target_index else None
            actions = generate_bulk_actions(jsonl_file, write_index, logical_index=logical)

            for ok, info in helpers.parallel_bulk(
                es_client,
                actions,
                thread_count=BULK_INSERT_SETTINGS["thread_count"],
                chunk_size=batch_size,
                raise_on_error=False,
                raise_on_exception=False,
                request_timeout=BULK_INSERT_SETTINGS["request_timeout"],
            ):
                if ok:
                    success_count += 1
                else:
                    error_count += 1
                    if len(errors) < max_errors:
                        errors.append(_sanitize_error_info(info))
            total_docs = success_count + error_count

    finally:
        # Re-enable refresh and manually refresh to make docs searchable
        set_refresh_interval(es_client, write_index, BULK_INSERT_SETTINGS["normal_refresh_interval"])
        refresh_index(es_client, write_index)

    return BulkInsertResult(
        index=write_index,
        total_docs=total_docs,
        success_count=success_count,
        error_count=error_count,
        errors=errors,
    )


def bulk_insert_from_dir(
    config: Config,
    jsonl_dir: Path,
    index: IndexName,
    pattern: str = "*.jsonl",
    batch_size: int = BULK_INSERT_SETTINGS["batch_size"],
    max_errors: int = 100,
    target_index: str | None = None,
) -> BulkInsertResult:
    """Bulk insert all JSONL files from a directory.

    Args:
        config: Configuration object
        jsonl_dir: Directory containing JSONL files
        index: Logical index name
        pattern: Glob pattern to match JSONL files
        batch_size: Number of documents per bulk request
        max_errors: Maximum number of error details to keep
        target_index: Physical index name to write to (Blue-Green).
            See :func:`bulk_insert_jsonl` for details.

    Returns:
        BulkInsertResult with success/error counts
    """
    if not jsonl_dir.is_dir():
        raise Exception(f"Directory '{jsonl_dir}' does not exist.")

    jsonl_files = sorted(jsonl_dir.glob(pattern))
    if not jsonl_files:
        write_index = target_index or index
        return BulkInsertResult(
            index=write_index,
            total_docs=0,
            success_count=0,
            error_count=0,
            errors=[],
        )

    return bulk_insert_jsonl(
        config=config,
        jsonl_files=jsonl_files,
        index=index,
        batch_size=batch_size,
        max_errors=max_errors,
        target_index=target_index,
    )
