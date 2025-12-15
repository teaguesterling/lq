"""
Event commands for blq CLI.

Handles viewing event details and context.
"""

from __future__ import annotations

import argparse
import json
import sys

import duckdb

from blq.commands.core import (
    RAW_DIR,
    EventRef,
    ensure_initialized,
)
from blq.query import LogStore


def cmd_event(args: argparse.Namespace) -> None:
    """Show event details by reference."""
    lq_dir = ensure_initialized()

    try:
        ref = EventRef.parse(args.ref)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        store = LogStore(lq_dir)
        event = store.event(ref.run_id, ref.event_id)

        if event is None:
            print(f"Event {args.ref} not found", file=sys.stderr)
            sys.exit(1)

        if args.json:
            print(json.dumps(event, indent=2, default=str))
        else:
            # Pretty print event details
            print(f"Event: {args.ref}")
            print(f"  Source: {event.get('source_name', '?')}")
            print(f"  Severity: {event.get('severity', '?')}")
            print(f"  File: {event.get('file_path', '?')}:{event.get('line_number', '?')}")
            print(f"  Message: {event.get('message', '?')}")
            if event.get("error_fingerprint"):
                print(f"  Fingerprint: {event.get('error_fingerprint')}")
            if event.get("log_line_start"):
                print(f"  Log lines: {event.get('log_line_start')}-{event.get('log_line_end')}")

    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_context(args: argparse.Namespace) -> None:
    """Show context lines around an event."""
    lq_dir = ensure_initialized()

    try:
        ref = EventRef.parse(args.ref)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        store = LogStore(lq_dir)
        event = store.event(ref.run_id, ref.event_id)

        if event is None:
            print(f"Event {args.ref} not found", file=sys.stderr)
            sys.exit(1)

        log_line_start = event.get("log_line_start")
        log_line_end = event.get("log_line_end")
        source_name = event.get("source_name")
        message = event.get("message")

        if log_line_start is None:
            # For structured formats, show message instead
            print(f"Event {args.ref} (from structured format, no log line context)")
            print(f"  Source: {source_name}")
            print(f"  Message: {message}")
            return

        # Read raw log file
        raw_file = lq_dir / RAW_DIR / f"{ref.run_id:03d}.log"
        if not raw_file.exists():
            print(f"Raw log not found: {raw_file}", file=sys.stderr)
            print("Hint: Use --keep-raw or --json/--markdown to save raw logs", file=sys.stderr)
            sys.exit(1)

        lines = raw_file.read_text().splitlines()
        context = args.lines

        start = max(0, log_line_start - context - 1)  # 1-indexed to 0-indexed
        end = min(len(lines), log_line_end + context)

        print(f"Context for event {args.ref} (lines {start + 1}-{end}):")
        print("-" * 60)
        for i in range(start, end):
            line_num = i + 1
            prefix = ">>> " if log_line_start <= line_num <= log_line_end else "    "
            print(f"{prefix}{line_num:4d} | {lines[i]}")
        print("-" * 60)

    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
