"""
Migration command for blq CLI.

Handles migration from parquet (v1) storage to BIRD (v2) storage.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime
from pathlib import Path

import duckdb

from blq.bird import BirdStore, InvocationRecord
from blq.commands.core import BlqConfig, LOGS_DIR


def _parse_timestamp(ts_str: str | None) -> datetime:
    """Parse an ISO timestamp string, returning now() if invalid."""
    if not ts_str:
        return datetime.now()
    try:
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return datetime.now()


def _migrate_parquet_to_bird(
    config: BlqConfig,
    dry_run: bool = False,
    verbose: bool = False,
) -> tuple[int, int]:
    """Migrate parquet data to BIRD storage.

    Args:
        config: BlqConfig instance
        dry_run: If True, don't actually write data
        verbose: If True, print detailed progress

    Returns:
        Tuple of (invocations_migrated, events_migrated)
    """
    lq_dir = config.lq_dir
    logs_dir = lq_dir / LOGS_DIR

    if not logs_dir.exists():
        print("No logs directory found, nothing to migrate.")
        return 0, 0

    # Find all parquet files
    parquet_files = list(logs_dir.rglob("*.parquet"))
    if not parquet_files:
        print("No parquet files found, nothing to migrate.")
        return 0, 0

    print(f"Found {len(parquet_files)} parquet file(s) to migrate")

    if dry_run:
        print("Dry run mode - no data will be written")

    # Read all data from parquet files
    conn = duckdb.connect(":memory:")

    # Build glob pattern for parquet files
    glob_pattern = str(logs_dir / "**" / "*.parquet")

    try:
        # Load all parquet data
        df = conn.execute(f"""
            SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)
            ORDER BY run_id, event_id
        """).fetchdf()
    except Exception as e:
        print(f"Error reading parquet files: {e}", file=sys.stderr)
        return 0, 0
    finally:
        conn.close()

    if df.empty:
        print("No data found in parquet files.")
        return 0, 0

    # Group by run_id to get unique runs
    runs = df.groupby("run_id").first().reset_index()
    print(f"Found {len(runs)} run(s) to migrate")

    if dry_run:
        # Just count what would be migrated
        total_events = len(df[df["severity"].notna()])
        print(f"Would migrate {len(runs)} invocations and {total_events} events")
        return len(runs), total_events

    # Initialize BIRD store
    # Create blobs directory if needed
    (lq_dir / "blobs" / "content").mkdir(parents=True, exist_ok=True)

    # Ensure BIRD schema exists
    store = BirdStore.open(lq_dir)

    invocations_migrated = 0
    events_migrated = 0

    try:
        for _, run in runs.iterrows():
            run_id = run["run_id"]
            source_name = run.get("source_name") or "unknown"
            source_type = run.get("source_type") or "run"

            if verbose:
                print(f"  Migrating run {run_id}: {source_name}")

            # Create session if needed
            session_id = source_name if source_type == "run" else f"{source_type}-migrated"
            client_id = f"blq-{source_type}"

            store.ensure_session(
                session_id=session_id,
                client_id=client_id,
                invoker="blq-migrate",
                invoker_type="import",
                cwd=run.get("cwd"),
            )

            # Parse timestamps
            started_at = _parse_timestamp(run.get("started_at"))
            completed_at = _parse_timestamp(run.get("completed_at"))
            duration_ms = None
            if started_at and completed_at:
                duration_ms = int((completed_at - started_at).total_seconds() * 1000)

            # Parse environment and CI from MAP columns
            environment = None
            ci = None
            if run.get("environment") is not None:
                try:
                    env_val = run["environment"]
                    if isinstance(env_val, dict):
                        environment = env_val
                except Exception:
                    pass
            if run.get("ci") is not None:
                try:
                    ci_val = run["ci"]
                    if isinstance(ci_val, dict):
                        ci = ci_val
                except Exception:
                    pass

            # Create invocation record
            inv = InvocationRecord(
                id=str(uuid.uuid4()),
                session_id=session_id,
                cmd=run.get("command") or "",
                cwd=run.get("cwd") or "",
                exit_code=int(run.get("exit_code") or 0),
                client_id=client_id,
                timestamp=started_at,
                duration_ms=duration_ms,
                executable=run.get("executable_path"),
                format_hint=None,  # Not stored in v1
                hostname=run.get("hostname"),
                username=None,  # Not stored in v1
                source_name=source_name,
                source_type=source_type,
                environment=environment,
                platform=run.get("platform"),
                arch=run.get("arch"),
                git_commit=run.get("git_commit"),
                git_branch=run.get("git_branch"),
                git_dirty=run.get("git_dirty"),
                ci=ci,
            )

            store.write_invocation(inv)
            invocations_migrated += 1

            # Get events for this run
            run_events = df[
                (df["run_id"] == run_id) & (df["severity"].notna())
            ]

            if len(run_events) > 0:
                events = []
                for _, event in run_events.iterrows():
                    events.append({
                        "event_id": event.get("event_id"),
                        "severity": event.get("severity"),
                        "file_path": event.get("file_path"),
                        "line_number": event.get("line_number"),
                        "column_number": event.get("column_number"),
                        "message": event.get("message"),
                        "error_code": event.get("error_code"),
                        "tool_name": event.get("tool_name"),
                        "category": event.get("category"),
                        "fingerprint": event.get("fingerprint"),
                        "log_line_start": event.get("log_line_start"),
                        "log_line_end": event.get("log_line_end"),
                    })

                store.write_events(
                    inv.id,
                    events,
                    client_id=client_id,
                    hostname=run.get("hostname"),
                )
                events_migrated += len(events)

                if verbose:
                    print(f"    Migrated {len(events)} event(s)")

    finally:
        store.close()

    return invocations_migrated, events_migrated


def cmd_migrate(args: argparse.Namespace) -> None:
    """Migrate data between storage formats."""
    config = BlqConfig.ensure()

    to_bird = getattr(args, "to_bird", False)
    dry_run = getattr(args, "dry_run", False)
    verbose = getattr(args, "verbose", False)
    keep_parquet = getattr(args, "keep_parquet", False)

    if not to_bird:
        print("Usage: blq migrate --to-bird")
        print("  Migrates parquet data to BIRD storage format")
        sys.exit(1)

    if config.storage_mode == "bird":
        print("Already using BIRD storage mode.")
        print("Use --force to re-import parquet data if needed.")
        if not getattr(args, "force", False):
            return

    print("Migrating to BIRD storage format...")
    print()

    # Run migration
    invocations, events = _migrate_parquet_to_bird(
        config,
        dry_run=dry_run,
        verbose=verbose,
    )

    if dry_run:
        print()
        print("Dry run complete. Use --execute to perform migration.")
        return

    print()
    print(f"Migrated {invocations} invocation(s) and {events} event(s)")

    # Update config to use BIRD mode
    if invocations > 0 or events > 0:
        config.storage_mode = "bird"
        config.save()
        print("Updated config.yaml to use BIRD storage mode")

    if not keep_parquet:
        print()
        print("Parquet files preserved in .lq/logs/")
        print("Run 'rm -rf .lq/logs/' to remove after verifying migration")
    else:
        print()
        print("Parquet files preserved (--keep-parquet)")


def register_migrate_command(subparsers) -> None:
    """Register the migrate command with the argument parser."""
    p_migrate = subparsers.add_parser(
        "migrate",
        help="Migrate data between storage formats",
    )
    p_migrate.add_argument(
        "--to-bird",
        action="store_true",
        help="Migrate parquet data to BIRD storage format",
    )
    p_migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    p_migrate.add_argument(
        "--execute",
        action="store_true",
        dest="execute",
        help="Actually perform the migration (opposite of --dry-run)",
    )
    p_migrate.add_argument(
        "--keep-parquet",
        action="store_true",
        help="Keep parquet files after migration (default: preserve)",
    )
    p_migrate.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force migration even if already using BIRD mode",
    )
    p_migrate.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed progress",
    )
    p_migrate.set_defaults(func=cmd_migrate)
