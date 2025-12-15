"""
Sync command for blq CLI.

Handles syncing logs to a central location.
"""

from __future__ import annotations

import argparse
import shutil
import socket
import sys
from pathlib import Path

from blq.commands.core import (
    GLOBAL_LQ_DIR,
    LOGS_DIR,
    PROJECTS_DIR,
    LqConfig,
    ensure_initialized,
)


def get_sync_destination(destination: str | None = None) -> Path:
    """Get the sync destination directory.

    Args:
        destination: Explicit destination path, or None for default (~/.lq/projects/)

    Returns:
        Path to destination directory
    """
    if destination:
        return Path(destination).expanduser()
    return GLOBAL_LQ_DIR / PROJECTS_DIR


def get_sync_target_path(
    destination: Path,
    namespace: str,
    project: str,
    hostname: str,
) -> Path:
    """Build the full path for sync target using Hive-style partitioning.

    Structure: destination/hostname=Z/namespace=X/project=Y

    Hostname first optimizes for:
    - "What's on this machine" queries
    - Local development workflows
    - Date-based consolidation can be done separately

    Args:
        destination: Base destination directory
        namespace: Project namespace (includes provider, e.g., github__owner)
        project: Project name
        hostname: Machine hostname

    Returns:
        Full path to sync target directory
    """
    return destination / f"hostname={hostname}" / f"namespace={namespace}" / f"project={project}"


def cmd_sync(args: argparse.Namespace) -> None:
    """Sync project logs to a central location."""
    lq_dir = ensure_initialized()
    config = LqConfig.load(lq_dir)

    # Validate project info exists
    if not config.namespace or not config.project:
        print("Error: Project namespace/project not configured.", file=sys.stderr)
        print("Run 'blq init --namespace X --project Y' or set in .lq/config.yaml", file=sys.stderr)
        sys.exit(1)

    # Get sync parameters
    hostname = socket.gethostname()
    destination = get_sync_destination(args.destination)
    target_path = get_sync_target_path(destination, config.namespace, config.project, hostname)
    source_logs = (lq_dir / LOGS_DIR).resolve()

    # Dry run mode
    if args.dry_run:
        print("Dry run - would perform the following:")
        print(f"  Source: {source_logs}")
        print(f"  Target: {target_path}")
        if args.hard:
            print("  Mode: hard (copy files)")
        else:
            print("  Mode: soft (symlink)")
        return

    # Status mode - show current sync state
    if args.status:
        _show_sync_status(destination, config.namespace, config.project)
        return

    # Create destination directory structure
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if args.hard:
        # Hard sync: copy files
        _hard_sync(source_logs, target_path, args.verbose)
    else:
        # Soft sync: create symlink (default)
        _soft_sync(source_logs, target_path, args.force, args.verbose)


def _soft_sync(source: Path, target: Path, force: bool, verbose: bool) -> None:
    """Create a symlink from target to source.

    Args:
        source: Source directory (.lq/logs)
        target: Target path for symlink
        force: If True, remove existing symlink/directory
        verbose: If True, print detailed info
    """
    if target.exists() or target.is_symlink():
        if target.is_symlink():
            current_target = target.resolve()
            if current_target == source:
                print(f"Already synced: {target} -> {source}")
                return
            elif force:
                target.unlink()
                if verbose:
                    print(f"Removed existing symlink: {target}")
            else:
                print(f"Error: Target already exists: {target}", file=sys.stderr)
                print(f"  Current target: {current_target}", file=sys.stderr)
                print("Use --force to replace", file=sys.stderr)
                sys.exit(1)
        elif target.is_dir():
            if force:
                shutil.rmtree(target)
                if verbose:
                    print(f"Removed existing directory: {target}")
            else:
                print(f"Error: Target directory already exists: {target}", file=sys.stderr)
                print("Use --force to replace (will delete existing data!)", file=sys.stderr)
                sys.exit(1)
        else:
            print(
                f"Error: Target exists and is not a symlink or directory: {target}", file=sys.stderr
            )
            sys.exit(1)

    # Create symlink
    target.symlink_to(source)
    print(f"Synced (soft): {target} -> {source}")


def _hard_sync(source: Path, target: Path, verbose: bool) -> None:
    """Copy files from source to target (incremental).

    Args:
        source: Source directory (.lq/logs)
        target: Target directory
        verbose: If True, print detailed info
    """
    # TODO: Implement incremental copy
    # For now, just use a simple rsync-style copy
    print("Error: Hard sync not yet implemented.", file=sys.stderr)
    print("Use soft sync (default) for now.", file=sys.stderr)
    sys.exit(1)


def _show_sync_status(destination: Path, namespace: str | None, project: str | None) -> None:
    """Show current sync status.

    Hierarchy: hostname/namespace/project

    Args:
        destination: Base destination directory
        namespace: Filter to specific namespace (or None for all)
        project: Filter to specific project (or None for all)
    """
    if not destination.exists():
        print(f"No synced projects found at {destination}")
        return

    print(f"Synced projects in {destination}:\n")

    # Find all synced projects (hostname first hierarchy)
    found_any = False
    for host_dir in sorted(destination.glob("hostname=*")):
        host_name = host_dir.name.replace("hostname=", "")

        for ns_dir in sorted(host_dir.glob("namespace=*")):
            ns_name = ns_dir.name.replace("namespace=", "")
            if namespace and ns_name != namespace:
                continue

            for proj_dir in sorted(ns_dir.glob("project=*")):
                proj_name = proj_dir.name.replace("project=", "")
                if project and proj_name != project:
                    continue

                found_any = True

                if proj_dir.is_symlink():
                    target = proj_dir.resolve()
                    exists = target.exists()
                    status = "ok" if exists else "broken"
                    print(f"  {host_name}: {ns_name}/{proj_name}")
                    print(f"    Mode: symlink ({status})")
                    print(f"    Target: {target}")
                else:
                    # Count parquet files for hard sync
                    parquet_count = len(list(proj_dir.rglob("*.parquet")))
                    print(f"  {host_name}: {ns_name}/{proj_name}")
                    print(f"    Mode: copy ({parquet_count} files)")
                print()

    if not found_any:
        print("  No synced projects found.")
        if namespace or project:
            print(f"  (filtered by namespace={namespace}, project={project})")
