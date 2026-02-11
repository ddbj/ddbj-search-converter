"""Elasticsearch snapshot CLI commands.

Usage:
    es_snapshot repo register --name backup --path /backup/es
    es_snapshot repo list
    es_snapshot repo delete --name backup
    es_snapshot create --repo backup [--snapshot my_snapshot] [--indexes bioproject,biosample]
    es_snapshot list --repo backup
    es_snapshot restore --repo backup --snapshot my_snapshot
    es_snapshot delete --repo backup --snapshot my_snapshot
    es_snapshot export-settings [--indexes bioproject,biosample] [--output settings.json]
"""

import argparse
import json
import sys
from pathlib import Path

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.es.snapshot import (
    create_snapshot,
    delete_repository,
    delete_snapshot,
    export_index_settings,
    list_repositories,
    list_snapshots,
    register_repository,
    restore_snapshot,
)
from ddbj_search_converter.logging.logger import log_debug, log_error, log_info, run_logger


def get_config_with_es_url(es_url: str | None) -> Config:
    """Get config, optionally overriding ES URL."""
    config = get_config()
    if es_url:
        config = Config(
            result_dir=config.result_dir,
            const_dir=config.const_dir,
            postgres_url=config.postgres_url,
            es_url=es_url,
        )
    return config


# === Subcommand: repo register ===


def cmd_repo_register(args: argparse.Namespace) -> None:
    """Register a snapshot repository."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())
        log_info("registering snapshot repository", name=args.name, path=args.path)

        try:
            result = register_repository(
                config=config,
                repo_name=args.name,
                repo_path=args.path,
                compress=not args.no_compress,
            )
            log_info("repository registered successfully", result=result)
        except Exception as e:
            log_error("failed to register repository", error=e)
            sys.exit(1)


# === Subcommand: repo list ===


def cmd_repo_list(args: argparse.Namespace) -> None:
    """List snapshot repositories."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        try:
            repos = list_repositories(config)

            if not repos:
                print("\nNo snapshot repositories found.")
                return

            print("\nSnapshot Repositories:")
            print("-" * 60)
            print(f"{'Name':<20} {'Type':<10} {'Location':<30}")
            print("-" * 60)

            for repo in repos:
                location = repo.get("settings", {}).get("location", "-")
                print(f"{repo['name']:<20} {repo['type']:<10} {location:<30}")

            print("-" * 60)

        except Exception as e:
            log_error("failed to list repositories", error=e)
            sys.exit(1)


# === Subcommand: repo delete ===


def cmd_repo_delete(args: argparse.Namespace) -> None:
    """Delete a snapshot repository."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        if not args.force:
            confirm = input(f"Are you sure you want to delete repository '{args.name}'? [y/N]: ")
            if confirm.lower() != "y":
                log_info("operation cancelled")
                return

        try:
            result = delete_repository(config=config, repo_name=args.name)
            log_info("repository deleted successfully", result=result)
        except Exception as e:
            log_error("failed to delete repository", error=e)
            sys.exit(1)


# === Subcommand: create ===


def cmd_create(args: argparse.Namespace) -> None:
    """Create a snapshot."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        indexes = args.indexes.split(",") if args.indexes else None
        metadata = {}
        if args.description:
            metadata["description"] = args.description

        log_info(
            "creating snapshot",
            repo=args.repo,
            snapshot=args.snapshot or "(auto)",
            indexes=indexes or "all",
        )

        try:
            result = create_snapshot(
                config=config,
                repo_name=args.repo,
                snapshot_name=args.snapshot,
                indexes=indexes,
                include_global_state=args.include_global_state,
                wait_for_completion=not args.no_wait,
                metadata=metadata or None,
            )

            if "snapshot" in result:
                snap_info = result["snapshot"]
                log_info(
                    "snapshot created successfully",
                    snapshot=snap_info.get("snapshot"),
                    state=snap_info.get("state"),
                    indices=snap_info.get("indices"),
                    duration_ms=snap_info.get("duration_in_millis"),
                )
            else:
                log_info("snapshot creation started", result=result)

        except Exception as e:
            log_error("failed to create snapshot", error=e)
            sys.exit(1)


# === Subcommand: list ===


def cmd_list(args: argparse.Namespace) -> None:
    """List snapshots in a repository."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        try:
            snapshots = list_snapshots(config=config, repo_name=args.repo)

            if not snapshots:
                print(f"\nNo snapshots found in repository '{args.repo}'.")
                return

            print(f"\nSnapshots in repository '{args.repo}':")
            print("-" * 80)
            print(f"{'Snapshot':<30} {'State':<12} {'Start Time':<25} {'Indices':<10}")
            print("-" * 80)

            for snap in snapshots:
                start_time = snap.get("start_time", "-")
                if start_time and start_time != "-":
                    start_time = start_time[:19].replace("T", " ")
                indices_count = len(snap.get("indices", []))
                print(f"{snap['snapshot']:<30} {snap['state']:<12} {start_time:<25} {indices_count:<10}")

            print("-" * 80)

            if args.verbose:
                print("\nDetailed Info:")
                for snap in snapshots:
                    print(f"\n  {snap['snapshot']}:")
                    print(f"    Indices: {', '.join(snap.get('indices', []))}")
                    if snap.get("metadata"):
                        print(f"    Metadata: {snap['metadata']}")
                    shards = snap.get("shards", {})
                    if shards:
                        print(
                            f"    Shards: total={shards.get('total', 0)}, "
                            f"successful={shards.get('successful', 0)}, "
                            f"failed={shards.get('failed', 0)}"
                        )

        except Exception as e:
            log_error("failed to list snapshots", error=e)
            sys.exit(1)


# === Subcommand: restore ===


def cmd_restore(args: argparse.Namespace) -> None:
    """Restore a snapshot."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        indexes = args.indexes.split(",") if args.indexes else None

        log_info(
            "restoring snapshot",
            repo=args.repo,
            snapshot=args.snapshot,
            indexes=indexes or "all from snapshot",
        )

        if not args.force:
            confirm = input(
                f"Are you sure you want to restore snapshot '{args.snapshot}'? "
                "This may overwrite existing indexes. [y/N]: "
            )
            if confirm.lower() != "y":
                log_info("operation cancelled")
                return

        try:
            result = restore_snapshot(
                config=config,
                repo_name=args.repo,
                snapshot_name=args.snapshot,
                indexes=indexes,
                rename_pattern=args.rename_pattern,
                rename_replacement=args.rename_replacement,
                include_global_state=args.include_global_state,
                wait_for_completion=not args.no_wait,
            )

            log_info("snapshot restored successfully", result=result)

        except Exception as e:
            log_error("failed to restore snapshot", error=e)
            sys.exit(1)


# === Subcommand: delete ===


def cmd_delete(args: argparse.Namespace) -> None:
    """Delete a snapshot."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        if not args.force:
            confirm = input(
                f"Are you sure you want to delete snapshot '{args.snapshot}' from repository '{args.repo}'? [y/N]: "
            )
            if confirm.lower() != "y":
                log_info("operation cancelled")
                return

        try:
            result = delete_snapshot(
                config=config,
                repo_name=args.repo,
                snapshot_name=args.snapshot,
            )
            log_info("snapshot deleted successfully", result=result)

        except Exception as e:
            log_error("failed to delete snapshot", error=e)
            sys.exit(1)


# === Subcommand: export-settings ===


def cmd_export_settings(args: argparse.Namespace) -> None:
    """Export index settings and mappings."""
    config = get_config_with_es_url(args.es_url)
    with run_logger(config=config):
        log_debug("config loaded", config=config.model_dump())

        indexes = args.indexes.split(",") if args.indexes else None

        log_info("exporting index settings", indexes=indexes or "all")

        try:
            result = export_index_settings(config=config, indexes=indexes)

            if args.output:
                output_path = Path(args.output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                log_info("settings exported to file", path=str(output_path))
            else:
                print(json.dumps(result, indent=2, ensure_ascii=False))

        except Exception as e:
            log_error("failed to export settings", error=e)
            sys.exit(1)


# === Main entry point ===


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Elasticsearch snapshot management commands.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--es-url",
        help="Elasticsearch URL (overrides env var)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # === repo subcommand ===
    repo_parser = subparsers.add_parser("repo", help="Repository management")
    repo_subparsers = repo_parser.add_subparsers(dest="repo_command", help="Repository commands")

    # repo register
    repo_register = repo_subparsers.add_parser("register", help="Register a repository")
    repo_register.add_argument("--name", required=True, help="Repository name")
    repo_register.add_argument("--path", required=True, help="Repository path (must match ES path.repo)")
    repo_register.add_argument("--no-compress", action="store_true", help="Disable compression")
    repo_register.add_argument("--es-url", help="Elasticsearch URL")
    repo_register.set_defaults(func=cmd_repo_register)

    # repo list
    repo_list = repo_subparsers.add_parser("list", help="List repositories")
    repo_list.add_argument("--es-url", help="Elasticsearch URL")
    repo_list.set_defaults(func=cmd_repo_list)

    # repo delete
    repo_del = repo_subparsers.add_parser("delete", help="Delete a repository")
    repo_del.add_argument("--name", required=True, help="Repository name")
    repo_del.add_argument("--force", action="store_true", help="Delete without confirmation")
    repo_del.add_argument("--es-url", help="Elasticsearch URL")
    repo_del.set_defaults(func=cmd_repo_delete)

    # === create subcommand ===
    create_parser = subparsers.add_parser("create", help="Create a snapshot")
    create_parser.add_argument("--repo", required=True, help="Repository name")
    create_parser.add_argument("--snapshot", help="Snapshot name (auto-generated if not provided)")
    create_parser.add_argument("--indexes", help="Comma-separated list of indexes")
    create_parser.add_argument("--description", help="Snapshot description")
    create_parser.add_argument("--include-global-state", action="store_true", help="Include cluster state")
    create_parser.add_argument("--no-wait", action="store_true", help="Don't wait for completion")
    create_parser.add_argument("--es-url", help="Elasticsearch URL")
    create_parser.set_defaults(func=cmd_create)

    # === list subcommand ===
    list_parser = subparsers.add_parser("list", help="List snapshots")
    list_parser.add_argument("--repo", required=True, help="Repository name")
    list_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")
    list_parser.add_argument("--es-url", help="Elasticsearch URL")
    list_parser.set_defaults(func=cmd_list)

    # === restore subcommand ===
    restore_parser = subparsers.add_parser("restore", help="Restore a snapshot")
    restore_parser.add_argument("--repo", required=True, help="Repository name")
    restore_parser.add_argument("--snapshot", required=True, help="Snapshot name")
    restore_parser.add_argument("--indexes", help="Comma-separated list of indexes to restore")
    restore_parser.add_argument("--rename-pattern", help="Regex pattern for renaming indexes")
    restore_parser.add_argument("--rename-replacement", help="Replacement string for renaming")
    restore_parser.add_argument("--include-global-state", action="store_true", help="Restore cluster state")
    restore_parser.add_argument("--no-wait", action="store_true", help="Don't wait for completion")
    restore_parser.add_argument("--force", action="store_true", help="Restore without confirmation")
    restore_parser.add_argument("--es-url", help="Elasticsearch URL")
    restore_parser.set_defaults(func=cmd_restore)

    # === delete subcommand ===
    delete_parser = subparsers.add_parser("delete", help="Delete a snapshot")
    delete_parser.add_argument("--repo", required=True, help="Repository name")
    delete_parser.add_argument("--snapshot", required=True, help="Snapshot name")
    delete_parser.add_argument("--force", action="store_true", help="Delete without confirmation")
    delete_parser.add_argument("--es-url", help="Elasticsearch URL")
    delete_parser.set_defaults(func=cmd_delete)

    # === export-settings subcommand ===
    export_parser = subparsers.add_parser("export-settings", help="Export index settings/mappings")
    export_parser.add_argument("--indexes", help="Comma-separated list of indexes")
    export_parser.add_argument("--output", "-o", help="Output file path (stdout if not specified)")
    export_parser.add_argument("--es-url", help="Elasticsearch URL")
    export_parser.set_defaults(func=cmd_export_settings)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "repo" and not getattr(args, "repo_command", None):
        repo_parser.print_help()
        sys.exit(1)

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
