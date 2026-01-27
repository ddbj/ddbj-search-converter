"""Elasticsearch CLI commands.

Usage:
    es_create_index --index bioproject
    es_create_index --index all
    es_delete_index --index bioproject
    es_delete_index --index all --force
    es_bulk_insert --index bioproject --dir /path/to/jsonl/
    es_bulk_insert --index sra-run --file /path/to/sra_run.jsonl
    es_list_indexes
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.es.bulk_insert import (bulk_insert_from_dir,
                                                  bulk_insert_jsonl)
from ddbj_search_converter.es.index import (create_index, delete_index,
                                            get_indexes_for_group,
                                            list_indexes)
from ddbj_search_converter.logging.logger import (log_debug, log_error,
                                                  log_info, run_logger)

# === Create Index ===


def parse_create_index_args(args: List[str]) -> Tuple[Config, str, bool]:
    parser = argparse.ArgumentParser(
        description="Create Elasticsearch indexes."
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Index name or group to create (bioproject, biosample, sra, jga, all, or specific like sra-run)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip indexes that already exist instead of raising an error",
    )
    parser.add_argument(
        "--es-url",
        help="Elasticsearch URL (overrides env var)",
    )

    parsed = parser.parse_args(args)
    config = get_config()
    if parsed.es_url:
        config = Config(
            result_dir=config.result_dir,
            const_dir=config.const_dir,
            postgres_url=config.postgres_url,
            es_url=parsed.es_url,
        )

    return config, parsed.index, parsed.skip_existing


def main_create_index() -> None:
    config, index, skip_existing = parse_create_index_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info("creating elasticsearch indexes", index=index)

        try:
            created = create_index(config, index, skip_existing=skip_existing)  # type: ignore
            if created:
                log_info("created indexes", indexes=created)
            else:
                log_info("No indexes created (all already exist)")
        except Exception as e:
            log_error("failed to create index", error=e)
            sys.exit(1)


# === Delete Index ===


def parse_delete_index_args(args: List[str]) -> Tuple[Config, str, bool, bool]:
    parser = argparse.ArgumentParser(
        description="Delete Elasticsearch indexes."
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Index name or group to delete (bioproject, biosample, sra, jga, all, or specific like sra-run)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete without confirmation",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip indexes that don't exist instead of raising an error",
    )
    parser.add_argument(
        "--es-url",
        help="Elasticsearch URL (overrides env var)",
    )

    parsed = parser.parse_args(args)
    config = get_config()
    if parsed.es_url:
        config = Config(
            result_dir=config.result_dir,
            const_dir=config.const_dir,
            postgres_url=config.postgres_url,
            es_url=parsed.es_url,
        )

    return config, parsed.index, parsed.force, parsed.skip_missing


def main_delete_index() -> None:
    config, index, force, skip_missing = parse_delete_index_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        # Show warning for destructive operation
        indexes_to_delete = get_indexes_for_group(index)  # type: ignore
        log_info("indexes to delete", indexes=indexes_to_delete)

        if not force:
            confirm = input(f"Are you sure you want to delete {len(indexes_to_delete)} index(es)? [y/N]: ")
            if confirm.lower() != "y":
                log_info("operation cancelled")
                return

        try:
            deleted = delete_index(config, index, skip_missing=skip_missing)  # type: ignore
            if deleted:
                log_info("deleted indexes", indexes=deleted)
            else:
                log_info("No indexes deleted (none exist)")
        except Exception as e:
            log_error("failed to delete index", error=e)
            sys.exit(1)


# === Bulk Insert ===


def parse_bulk_insert_args(args: List[str]) -> Tuple[Config, str, Path, List[Path], int]:
    parser = argparse.ArgumentParser(
        description="Bulk insert JSONL files into Elasticsearch."
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Target index name (e.g., bioproject, sra-run)",
    )
    parser.add_argument(
        "--dir",
        help="Directory containing JSONL files",
    )
    parser.add_argument(
        "--file",
        action="append",
        dest="files",
        help="JSONL file to insert (can be specified multiple times)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of documents per bulk request (default: 500)",
    )
    parser.add_argument(
        "--es-url",
        help="Elasticsearch URL (overrides env var)",
    )

    parsed = parser.parse_args(args)
    config = get_config()
    if parsed.es_url:
        config = Config(
            result_dir=config.result_dir,
            const_dir=config.const_dir,
            postgres_url=config.postgres_url,
            es_url=parsed.es_url,
        )

    if not parsed.dir and not parsed.files:
        parser.error("Either --dir or --file must be specified")

    jsonl_dir = Path(parsed.dir) if parsed.dir else Path(".")
    jsonl_files = [Path(f) for f in (parsed.files or [])]

    return config, parsed.index, jsonl_dir, jsonl_files, parsed.batch_size


def main_bulk_insert() -> None:
    config, index, jsonl_dir, jsonl_files, batch_size = parse_bulk_insert_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info("bulk inserting into elasticsearch", index=index)

        try:
            if jsonl_files:
                result = bulk_insert_jsonl(
                    config=config,
                    jsonl_files=jsonl_files,
                    index=index,  # type: ignore
                    batch_size=batch_size,
                )
            else:
                result = bulk_insert_from_dir(
                    config=config,
                    jsonl_dir=jsonl_dir,
                    index=index,  # type: ignore
                    batch_size=batch_size,
                )

            log_info(
                "Bulk insert completed",
                index=result.index,
                total_docs=result.total_docs,
                success_count=result.success_count,
                error_count=result.error_count,
            )

            if result.errors:
                log_error("Some documents failed to insert", errors=result.errors[:10])
                sys.exit(1)

        except Exception as e:
            log_error("failed to bulk insert", error=e)
            sys.exit(1)


# === List Indexes ===


def parse_list_indexes_args(args: List[str]) -> Config:
    parser = argparse.ArgumentParser(
        description="List Elasticsearch indexes and their document counts."
    )
    parser.add_argument(
        "--es-url",
        help="Elasticsearch URL (overrides env var)",
    )

    parsed = parser.parse_args(args)
    config = get_config()
    if parsed.es_url:
        config = Config(
            result_dir=config.result_dir,
            const_dir=config.const_dir,
            postgres_url=config.postgres_url,
            es_url=parsed.es_url,
        )

    return config


def main_list_indexes() -> None:
    config = parse_list_indexes_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        try:
            indexes = list_indexes(config)

            print("\nDDBJ-Search Elasticsearch Indexes:")
            print("-" * 50)
            print(f"{'Index':<20} {'Exists':<10} {'Doc Count':<15}")
            print("-" * 50)

            for idx_info in indexes:
                exists_str = "Yes" if idx_info["exists"] else "No"
                doc_count = idx_info["doc_count"] if idx_info["exists"] else "-"
                print(f"{idx_info['index']:<20} {exists_str:<10} {doc_count:<15}")

            print("-" * 50)

        except Exception as e:
            log_error("failed to list indexes", error=e)
            sys.exit(1)
