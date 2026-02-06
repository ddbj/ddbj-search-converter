"""Elasticsearch bulk insert operations."""

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List

from pydantic import BaseModel

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import (check_index_exists, get_es_client,
                                             refresh_index,
                                             set_refresh_interval)
from ddbj_search_converter.es.index import IndexName
from ddbj_search_converter.es.settings import BULK_INSERT_SETTINGS
from elasticsearch import helpers


class BulkInsertResult(BaseModel):
    """Result of a bulk insert operation."""

    index: str
    total_docs: int
    success_count: int
    error_count: int
    errors: List[Dict[str, Any]]


def generate_bulk_actions(
    jsonl_file: Path,
    index: str,
) -> Iterator[Dict[str, Any]]:
    """Generate bulk actions from a JSONL file.

    Args:
        jsonl_file: Path to the JSONL file
        index: Target index name

    Yields:
        Bulk action dictionaries
    """
    with jsonl_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
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


def bulk_insert_jsonl(
    config: Config,
    jsonl_files: List[Path],
    index: IndexName,
    batch_size: int = BULK_INSERT_SETTINGS["batch_size"],
    max_errors: int = 100,
) -> BulkInsertResult:
    """Bulk insert JSONL files into Elasticsearch.

    Args:
        config: Configuration object
        jsonl_files: List of JSONL file paths to insert
        index: Target index name
        batch_size: Number of documents per bulk request
        max_errors: Maximum number of error details to keep

    Returns:
        BulkInsertResult with success/error counts and error details

    Raises:
        Exception: If the target index does not exist
    """
    es_client = get_es_client(config)

    if not check_index_exists(es_client, index):
        raise Exception(f"Index '{index}' does not exist.")

    total_docs = 0
    success_count = 0
    error_count = 0
    errors: List[Dict[str, Any]] = []

    # Disable refresh during bulk insert for better performance
    set_refresh_interval(es_client, index, BULK_INSERT_SETTINGS["bulk_refresh_interval"])

    try:
        for jsonl_file in jsonl_files:
            actions = generate_bulk_actions(jsonl_file, index)

            success, failed = helpers.bulk(
                es_client,
                actions,
                chunk_size=batch_size,
                stats_only=False,
                raise_on_error=False,
                max_retries=BULK_INSERT_SETTINGS["max_retries"],
                request_timeout=BULK_INSERT_SETTINGS["request_timeout"],
            )

            success_count += success
            # failed can be int (when stats_only=True) or list
            if isinstance(failed, list) and failed:
                for err in failed:
                    error_count += 1
                    total_docs += 1
                    if len(errors) < max_errors:
                        errors.append(err)
            total_docs += success

    finally:
        # Re-enable refresh and manually refresh to make docs searchable
        set_refresh_interval(es_client, index, BULK_INSERT_SETTINGS["normal_refresh_interval"])
        refresh_index(es_client, index)

    return BulkInsertResult(
        index=index,
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
) -> BulkInsertResult:
    """Bulk insert all JSONL files from a directory.

    Args:
        config: Configuration object
        jsonl_dir: Directory containing JSONL files
        index: Target index name
        pattern: Glob pattern to match JSONL files
        batch_size: Number of documents per bulk request
        max_errors: Maximum number of error details to keep

    Returns:
        BulkInsertResult with success/error counts
    """
    if not jsonl_dir.is_dir():
        raise Exception(f"Directory '{jsonl_dir}' does not exist.")

    jsonl_files = sorted(jsonl_dir.glob(pattern))
    if not jsonl_files:
        return BulkInsertResult(
            index=index,
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
    )
