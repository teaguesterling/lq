"""
Management commands for lq CLI.

Handles status, errors, warnings, summary, history, and prune operations.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from lq.commands.core import (
    LOGS_DIR,
    ensure_initialized,
    get_store_for_args,
)


def cmd_status(args: argparse.Namespace) -> None:
    """Show status of all sources."""
    try:
        store = get_store_for_args(args)
        conn = store.connection

        if args.verbose:
            result = conn.execute("FROM lq_status_verbose()").fetchdf()
        else:
            result = conn.execute("FROM lq_status()").fetchdf()
        print(result.to_string(index=False))
    except duckdb.Error:
        # Fallback if macros aren't working
        store = get_store_for_args(args)
        result = store.events().limit(10).df()
        print(result.to_string(index=False))


def cmd_errors(args: argparse.Namespace) -> None:
    """Show recent errors."""
    try:
        store = get_store_for_args(args)
        query = store.errors()

        # Filter by source if specified
        if args.source:
            query = query.filter(source_name=args.source)

        # Order by run_id desc, event_id
        query = query.order_by("run_id", desc=True).limit(args.limit)

        # Select columns based on compact mode
        if args.compact:
            query = query.select("run_id", "event_id", "file_path", "line_number", "message")

        result = query.df()

        if args.json:
            print(result.to_json(orient="records"))
        else:
            print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_warnings(args: argparse.Namespace) -> None:
    """Show recent warnings."""
    try:
        store = get_store_for_args(args)
        result = (
            store.warnings()
            .order_by("run_id", desc=True)
            .limit(args.limit)
            .df()
        )
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)


def cmd_summary(args: argparse.Namespace) -> None:
    """Show aggregate summary."""
    try:
        store = get_store_for_args(args)
        conn = store.connection

        if args.latest:
            result = conn.execute("FROM lq_summary_latest()").fetchdf()
        else:
            result = conn.execute("FROM lq_summary()").fetchdf()
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)


def cmd_history(args: argparse.Namespace) -> None:
    """Show run history."""
    try:
        store = get_store_for_args(args)
        result = store.runs().head(args.limit)
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)


def cmd_prune(args: argparse.Namespace) -> None:
    """Remove old log files."""
    lq_dir = ensure_initialized()
    logs_dir = lq_dir / LOGS_DIR

    cutoff = datetime.now() - timedelta(days=args.older_than)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    removed = 0
    for date_dir in logs_dir.glob("date=*"):
        date_str = date_dir.name.replace("date=", "")
        if date_str < cutoff_str:
            if args.dry_run:
                print(f"Would remove: {date_dir}")
            else:
                shutil.rmtree(date_dir)
                print(f"Removed: {date_dir}")
            removed += 1

    if removed == 0:
        print(f"No logs older than {args.older_than} days")
    elif args.dry_run:
        print(f"\nDry run: would remove {removed} date partitions")
