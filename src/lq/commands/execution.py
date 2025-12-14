"""
Execution commands for lq CLI.

Handles running commands, importing logs, and capturing stdin.
"""

from __future__ import annotations

import argparse
import os
import platform
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from lq.commands.core import (
    EventSummary,
    LqConfig,
    RAW_DIR,
    RunResult,
    capture_ci_info,
    capture_environment,
    capture_git_info,
    ensure_initialized,
    find_executable,
    get_next_run_id,
    load_commands,
    parse_log_content,
    write_run_parquet,
)


def cmd_run(args: argparse.Namespace) -> None:
    """Run a command and capture its output."""
    lq_dir = ensure_initialized()

    # Load config for default environment capture
    config = LqConfig.load(lq_dir)

    # Check if first argument is a registered command name
    registered_commands = load_commands(lq_dir)
    first_arg = args.command[0]

    # Build list of env vars to capture (config defaults + command-specific)
    capture_env_vars = config.capture_env.copy()

    # Default capture setting (can be overridden by command config)
    should_capture = True

    if first_arg in registered_commands and len(args.command) == 1:
        # Use registered command
        reg_cmd = registered_commands[first_arg]
        command = reg_cmd.cmd
        source_name = args.name or first_arg
        format_hint = args.format if args.format != "auto" else reg_cmd.format
        should_capture = reg_cmd.capture
        # Add command-specific env vars
        for var in reg_cmd.capture_env:
            if var not in capture_env_vars:
                capture_env_vars.append(var)
    else:
        # Use literal command
        command = " ".join(args.command)
        source_name = args.name or first_arg
        format_hint = args.format

    # Runtime flag overrides command config
    if args.capture is not None:
        should_capture = args.capture

    run_id = get_next_run_id(lq_dir)
    started_at = datetime.now()

    # Capture execution context
    cwd = os.getcwd()
    executable_path = find_executable(command)
    environment = capture_environment(capture_env_vars)
    hostname = socket.gethostname()
    platform_name = platform.system()
    arch = platform.machine()
    git_info = capture_git_info()
    ci_info = capture_ci_info()

    # Determine output mode
    structured_output = args.json or args.markdown
    quiet = args.quiet or structured_output

    if not quiet:
        print(f"[lq] Running: {command}", file=sys.stderr)
        print(f"[lq] Run ID: {run_id}", file=sys.stderr)

    # Run command, capturing output
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    output_lines = []
    for line in process.stdout:
        if not quiet:
            sys.stdout.write(line)
            sys.stdout.flush()
        output_lines.append(line)

    exit_code = process.wait()
    completed_at = datetime.now()
    output = "".join(output_lines)
    duration_sec = (completed_at - started_at).total_seconds()

    # No-capture mode: just run and exit with the command's exit code
    if not should_capture:
        if not quiet:
            print(f"\n[lq] Completed in {duration_sec:.1f}s (exit code {exit_code})",
                  file=sys.stderr)
        sys.exit(exit_code)

    # Always save raw output when using structured output (needed for context)
    if args.keep_raw or structured_output:
        raw_file = lq_dir / RAW_DIR / f"{run_id:03d}.log"
        raw_file.write_text(output)

    # Parse output
    events = parse_log_content(output, format_hint)

    # Write parquet
    run_meta = {
        "run_id": run_id,
        "source_name": source_name,
        "source_type": "run",
        "command": command,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "exit_code": exit_code,
        "cwd": cwd,
        "executable_path": executable_path,
        "environment": environment or None,  # dict -> MAP(VARCHAR, VARCHAR)
        "hostname": hostname,
        "platform": platform_name,
        "arch": arch,
        "git_commit": git_info.commit,
        "git_branch": git_info.branch,
        "git_dirty": git_info.dirty,
        "ci": ci_info,  # dict -> MAP(VARCHAR, VARCHAR), None if not in CI
    }

    filepath = write_run_parquet(events, run_meta, lq_dir)

    # Build structured result
    error_events = [e for e in events if e.get("severity") == "error"]
    warning_events = [e for e in events if e.get("severity") == "warning"]

    def make_event_summary(e: dict) -> EventSummary:
        return EventSummary(
            ref=f"{run_id}:{e.get('event_id', 0)}",
            severity=e.get("severity"),
            file_path=e.get("file_path"),
            line_number=e.get("line_number"),
            column_number=e.get("column_number"),
            message=e.get("message"),
            error_code=e.get("error_code"),
            fingerprint=e.get("error_fingerprint"),
            test_name=e.get("test_name"),
            log_line_start=e.get("log_line_start"),
            log_line_end=e.get("log_line_end"),
        )

    # Determine status
    if error_events:
        status = "FAIL"
    elif warning_events:
        status = "WARN"
    elif exit_code != 0:
        status = "FAIL"
    else:
        status = "OK"

    result = RunResult(
        run_id=run_id,
        command=command,
        status=status,
        exit_code=exit_code,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        duration_sec=duration_sec,
        summary={
            "total_events": len(events),
            "errors": len(error_events),
            "warnings": len(warning_events),
        },
        errors=[make_event_summary(e) for e in error_events[: args.error_limit]],
        warnings=[make_event_summary(e) for e in warning_events[: args.error_limit]],
        parquet_path=str(filepath),
    )

    # Output based on format
    if args.json:
        print(result.to_json(include_warnings=args.include_warnings))
    elif args.markdown:
        print(result.to_markdown(include_warnings=args.include_warnings))
    else:
        # Traditional output
        print(
            f"\n[lq] Captured {len(events)} events "
            f"({len(error_events)} errors, {len(warning_events)} warnings)",
            file=sys.stderr,
        )
        print(f"[lq] Saved to {filepath}", file=sys.stderr)

    sys.exit(exit_code)


def cmd_import(args: argparse.Namespace) -> None:
    """Import an existing log file."""
    lq_dir = ensure_initialized()

    filepath = Path(args.file)
    if not filepath.exists():
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    source_name = args.name or filepath.stem
    run_id = get_next_run_id(lq_dir)
    now = datetime.now().isoformat()

    content = filepath.read_text()
    events = parse_log_content(content, args.format)

    run_meta = {
        "run_id": run_id,
        "source_name": source_name,
        "source_type": "import",
        "command": f"import {filepath}",
        "started_at": now,
        "completed_at": now,
        "exit_code": 0,
    }

    outpath = write_run_parquet(events, run_meta, lq_dir)

    errors = sum(1 for e in events if e.get("severity") == "error")
    warnings = sum(1 for e in events if e.get("severity") == "warning")
    print(f"Imported {len(events)} events ({errors} errors, {warnings} warnings)")
    print(f"Saved to {outpath}")


def cmd_capture(args: argparse.Namespace) -> None:
    """Capture from stdin."""
    lq_dir = ensure_initialized()

    source_name = args.name or "stdin"
    run_id = get_next_run_id(lq_dir)
    started_at = datetime.now().isoformat()

    content = sys.stdin.read()
    completed_at = datetime.now().isoformat()

    events = parse_log_content(content, args.format)

    run_meta = {
        "run_id": run_id,
        "source_name": source_name,
        "source_type": "capture",
        "command": "stdin",
        "started_at": started_at,
        "completed_at": completed_at,
        "exit_code": 0,
    }

    outpath = write_run_parquet(events, run_meta, lq_dir)

    errors = sum(1 for e in events if e.get("severity") == "error")
    warnings = sum(1 for e in events if e.get("severity") == "warning")
    print(f"Captured {len(events)} events ({errors} errors, {warnings} warnings)", file=sys.stderr)
    print(f"Saved to {outpath}", file=sys.stderr)
