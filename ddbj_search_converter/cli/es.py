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

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.dblink.utils import load_blacklist, load_jga_blacklist, load_sra_blacklist
from ddbj_search_converter.es.bulk_delete import bulk_delete_by_ids
from ddbj_search_converter.es.bulk_insert import bulk_insert_from_dir, bulk_insert_jsonl
from ddbj_search_converter.es.index import create_index, delete_index, get_indexes_for_group, list_indexes
from ddbj_search_converter.es.monitoring import (
    check_health,
    format_bytes,
    get_cluster_health,
    get_index_stats,
    get_node_stats,
)
from ddbj_search_converter.id_patterns import ID_PATTERN_MAP
from ddbj_search_converter.logging.logger import log_debug, log_error, log_info, log_warn, run_logger

# === Create Index ===


def parse_create_index_args(args: list[str]) -> tuple[Config, str, bool]:
    parser = argparse.ArgumentParser(description="Create Elasticsearch indexes.")
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
            created = create_index(config, index, skip_existing=skip_existing)  # type: ignore[arg-type]
            if created:
                log_info("created indexes", indexes=created)
            else:
                log_info("No indexes created (all already exist)")
        except Exception as e:
            log_error("failed to create index", error=e)
            sys.exit(1)


# === Delete Index ===


def parse_delete_index_args(args: list[str]) -> tuple[Config, str, bool, bool]:
    parser = argparse.ArgumentParser(description="Delete Elasticsearch indexes.")
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
        indexes_to_delete = get_indexes_for_group(index)  # type: ignore[arg-type]
        log_info("indexes to delete", indexes=indexes_to_delete)

        if not force:
            confirm = input(f"Are you sure you want to delete {len(indexes_to_delete)} index(es)? [y/N]: ")
            if confirm.lower() != "y":
                log_info("operation cancelled")
                return

        try:
            deleted = delete_index(config, index, skip_missing=skip_missing)  # type: ignore[arg-type]
            if deleted:
                log_info("deleted indexes", indexes=deleted)
            else:
                log_info("No indexes deleted (none exist)")
        except Exception as e:
            log_error("failed to delete index", error=e)
            sys.exit(1)


# === Bulk Insert ===


def parse_bulk_insert_args(args: list[str]) -> tuple[Config, str, Path, list[Path], str, int]:
    parser = argparse.ArgumentParser(description="Bulk insert JSONL files into Elasticsearch.")
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
        "--pattern",
        default="*.jsonl",
        help="Glob pattern to match JSONL files when using --dir (default: *.jsonl)",
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

    jsonl_dir = Path(parsed.dir) if parsed.dir else Path()
    jsonl_files = [Path(f) for f in (parsed.files or [])]

    return config, parsed.index, jsonl_dir, jsonl_files, parsed.pattern, parsed.batch_size


def main_bulk_insert() -> None:
    config, index, jsonl_dir, jsonl_files, pattern, batch_size = parse_bulk_insert_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info("bulk inserting into elasticsearch", index=index, pattern=pattern)

        try:
            if jsonl_files:
                result = bulk_insert_jsonl(
                    config=config,
                    jsonl_files=jsonl_files,
                    index=index,  # type: ignore[arg-type]
                    batch_size=batch_size,
                )
            else:
                result = bulk_insert_from_dir(
                    config=config,
                    jsonl_dir=jsonl_dir,
                    index=index,  # type: ignore[arg-type]
                    pattern=pattern,
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


def parse_list_indexes_args(args: list[str]) -> Config:
    parser = argparse.ArgumentParser(description="List Elasticsearch indexes and their document counts.")

    parser.parse_args(args)
    config = get_config()

    return config


def main_list_indexes() -> None:
    config = parse_list_indexes_args(sys.argv[1:])
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        try:
            indexes = list_indexes(config)

            print("\nDDBJ Search Elasticsearch Indexes:")
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


def parse_health_check_args(args: list[str]) -> tuple[Config, bool]:
    parser = argparse.ArgumentParser(description="Check Elasticsearch cluster health.")
    parser.add_argument(
        "--verbose",
        "-v",
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
                    print(
                        f"  Disk: {format_bytes(node.disk_total_bytes - node.disk_free_bytes)} / "
                        f"{format_bytes(node.disk_total_bytes)} ({node.disk_used_percent:.1f}% used)"
                    )
                    print(
                        f"  Heap: {format_bytes(node.heap_used_bytes)} / "
                        f"{format_bytes(node.heap_max_bytes)} ({node.heap_used_percent:.1f}% used)"
                    )

                # Index stats
                print("\n=== Index Statistics ===")
                print("-" * 60)
                print(f"{'Index':<25} {'Docs':<12} {'Size':<12} {'Shards':<10}")
                print("-" * 60)

                index_stats = get_index_stats(config)
                for idx in sorted(index_stats, key=lambda x: x.name):
                    shards_str = f"{idx.primary_shards}p/{idx.replica_shards}r"
                    print(
                        f"{idx.name:<25} {idx.docs_count:<12} {format_bytes(idx.store_size_bytes):<12} {shards_str:<10}"
                    )

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


# === Delete Blacklist ===

# AccessionType -> ES インデックス名のマッピング
ACCESSION_TYPE_TO_INDEX: dict[str, str] = {
    "bioproject": "bioproject",
    "umbrella-bioproject": "bioproject",
    "biosample": "biosample",
    "sra-submission": "sra-submission",
    "sra-study": "sra-study",
    "sra-experiment": "sra-experiment",
    "sra-run": "sra-run",
    "sra-sample": "sra-sample",
    "sra-analysis": "sra-analysis",
    "jga-study": "jga-study",
    "jga-dataset": "jga-dataset",
    "jga-dac": "jga-dac",
    "jga-policy": "jga-policy",
}


def classify_accession(accession: str) -> str | None:
    """accession の ID パターンからインデックス名を判定する。"""
    for acc_type, pattern in ID_PATTERN_MAP.items():
        if acc_type in ACCESSION_TYPE_TO_INDEX and pattern.match(accession):
            return ACCESSION_TYPE_TO_INDEX[acc_type]
    return None


def classify_blacklist_by_index(
    blacklist: set[str],
) -> dict[str, set[str]]:
    """blacklist の各 accession を ID パターンからインデックスごとに分類する。"""
    result: dict[str, set[str]] = {}
    for accession in blacklist:
        index = classify_accession(accession)
        if index:
            result.setdefault(index, set()).add(accession)
        else:
            log_warn(f"cannot classify accession: {accession}")
    return result


def parse_delete_blacklist_args(args: list[str]) -> tuple[Config, str, bool, bool, int]:
    """Delete blacklist コマンドの引数をパースする。"""
    parser = argparse.ArgumentParser(description="Delete blacklisted documents from Elasticsearch indexes.")
    parser.add_argument(
        "--index",
        default="all",
        help="Index group (bioproject, biosample, sra, jga, all). Default: all",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete without confirmation",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for bulk delete (default: 1000)",
    )
    parsed = parser.parse_args(args)
    config = get_config()
    return config, parsed.index, parsed.force, parsed.dry_run, parsed.batch_size


def main_delete_blacklist() -> None:
    """Blacklist に含まれるドキュメントを ES から削除する。"""
    config, index_group, force, dry_run, batch_size = parse_delete_blacklist_args(sys.argv[1:])

    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        # blacklist を読み込む
        bp_blacklist, bs_blacklist = load_blacklist(config)
        sra_blacklist = load_sra_blacklist(config)
        jga_blacklist = load_jga_blacklist(config)

        # 全 blacklist を統合
        all_blacklist = bp_blacklist | bs_blacklist | sra_blacklist | jga_blacklist

        if not all_blacklist:
            log_info("No blacklist entries found")
            return

        log_info(
            "loaded blacklist entries",
            bioproject=len(bp_blacklist),
            biosample=len(bs_blacklist),
            sra=len(sra_blacklist),
            jga=len(jga_blacklist),
        )

        # ID パターンでインデックスごとに分類
        index_blacklist_map = classify_blacklist_by_index(all_blacklist)

        # 対象インデックスを取得
        target_indexes = get_indexes_for_group(index_group)  # type: ignore[arg-type]

        # サマリ表示
        total_to_delete = 0
        for idx in target_indexes:
            blacklist = index_blacklist_map.get(idx, set())
            if blacklist:
                total_to_delete += len(blacklist)
                log_info(f"index={idx}: {len(blacklist)} blacklisted accessions")

        if total_to_delete == 0:
            log_info("No blacklisted accessions to delete")
            return

        if dry_run:
            log_info(f"[DRY-RUN] Would delete {total_to_delete} documents total")
            return

        if not force:
            confirm = input(f"Delete {total_to_delete} blacklisted documents? [y/N]: ")
            if confirm.lower() != "y":
                log_info("operation cancelled")
                return

        # 削除実行
        total_success = 0
        total_not_found = 0
        total_errors = 0

        for idx in target_indexes:
            blacklist = index_blacklist_map.get(idx, set())
            if not blacklist:
                continue

            result = bulk_delete_by_ids(config, idx, blacklist, batch_size)
            log_info(
                f"deleted from {idx}",
                success=result.success_count,
                not_found=result.not_found_count,
                errors=result.error_count,
            )

            total_success += result.success_count
            total_not_found += result.not_found_count
            total_errors += result.error_count

            if result.errors:
                for err in result.errors[:5]:
                    log_error(f"delete error: {err}")

        log_info(
            "Blacklist deletion completed",
            total_success=total_success,
            total_not_found=total_not_found,
            total_errors=total_errors,
        )

        if total_errors > 0:
            sys.exit(1)
