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
from ddbj_search_converter.es.monitoring import (check_health, format_bytes,
                                                 get_cluster_health,
                                                 get_index_stats,
                                                 get_node_stats)
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

    parsed = parser.parse_args(args)
    config = get_config()

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

    parsed = parser.parse_args(args)
    config = get_config()

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
        default=5000,
        help="Number of documents per bulk request (default: 5000)",
    )

    parsed = parser.parse_args(args)
    config = get_config()

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

    parser.parse_args(args)
    config = get_config()

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


# === Health Check ===


def parse_health_check_args(args: List[str]) -> Tuple[Config, bool]:
    parser = argparse.ArgumentParser(
        description="Check Elasticsearch cluster health."
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed node and index information",
    )

    parsed = parser.parse_args(args)
    config = get_config()

    return config, parsed.verbose


def main_health_check() -> None:
    config, verbose = parse_health_check_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        try:
            # Cluster health
            cluster = get_cluster_health(config)

            status_color = {
                "green": "\033[92m",  # Green
                "yellow": "\033[93m",  # Yellow
                "red": "\033[91m",  # Red
            }
            reset = "\033[0m"
            color = status_color.get(cluster.status, "")

            print("\n=== Cluster Health ===")
            print(f"Cluster Name: {cluster.cluster_name}")
            print(f"Status: {color}{cluster.status.upper()}{reset}")
            print(f"Nodes: {cluster.number_of_nodes} (data: {cluster.number_of_data_nodes})")
            print(f"Shards: {cluster.active_shards} active ({cluster.active_primary_shards} primary)")
            if cluster.unassigned_shards > 0:
                print(f"Unassigned Shards: {cluster.unassigned_shards}")

            if verbose:
                # Node stats
                print("\n=== Node Statistics ===")
                nodes = get_node_stats(config)
                for node in nodes:
                    print(f"\nNode: {node.name} ({node.host})")
                    print(f"  Disk: {format_bytes(node.disk_total_bytes - node.disk_free_bytes)} / "
                          f"{format_bytes(node.disk_total_bytes)} ({node.disk_used_percent:.1f}% used)")
                    print(f"  Heap: {format_bytes(node.heap_used_bytes)} / "
                          f"{format_bytes(node.heap_max_bytes)} ({node.heap_used_percent:.1f}% used)")

                # Index stats
                print("\n=== Index Statistics ===")
                print("-" * 60)
                print(f"{'Index':<25} {'Docs':<12} {'Size':<12} {'Shards':<10}")
                print("-" * 60)

                index_stats = get_index_stats(config)
                for idx in sorted(index_stats, key=lambda x: x.name):
                    shards_str = f"{idx.primary_shards}p/{idx.replica_shards}r"
                    print(f"{idx.name:<25} {idx.docs_count:<12} "
                          f"{format_bytes(idx.store_size_bytes):<12} {shards_str:<10}")

                print("-" * 60)

            # Health issues
            issues = check_health(config)
            if issues:
                print("\n=== Health Issues ===")
                for issue in issues:
                    if issue.level == "critical":
                        print(f"\033[91m[CRITICAL]\033[0m {issue.message}")
                    elif issue.level == "warning":
                        print(f"\033[93m[WARNING]\033[0m {issue.message}")
                    else:
                        print(f"[{issue.level.upper()}] {issue.message}")
                sys.exit(1)
            else:
                print("\n\033[92mAll health checks passed.\033[0m")

        except Exception as e:
            log_error("failed to check health", error=e)
            sys.exit(1)
