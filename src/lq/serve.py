"""
MCP server for lq.

Provides tools, resources, and prompts for AI agent integration.

Usage:
    lq serve                    # stdio transport (for Claude Desktop)
    lq serve --transport sse    # SSE transport (for HTTP clients)
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
from fastmcp import FastMCP

from lq.query import LogStore


def _to_json_safe(value: Any) -> Any:
    """Convert pandas NA/NaT values to None for JSON serialization."""
    if pd.isna(value):
        return None
    return value

# Create the MCP server
mcp = FastMCP(
    "lq",
    instructions=(
        "Log Query - capture and query build/test logs. "
        "Use tools to run builds, query errors, and analyze results."
    ),
)


def _get_store() -> LogStore:
    """Get LogStore for current directory."""
    return LogStore.open()


def _format_ref(run_id: int, event_id: int) -> str:
    """Format event reference."""
    return f"{run_id}:{event_id}"


def _parse_ref(ref: str) -> tuple[int, int]:
    """Parse event reference into (run_id, event_id)."""
    parts = ref.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid ref format: {ref}")
    return int(parts[0]), int(parts[1])


# ============================================================================
# Implementation Functions
# (Separated from decorators so they can be called from resources/prompts)
# ============================================================================


def _run_impl(command: str, args: list[str] | None = None, timeout: int = 300) -> dict[str, Any]:
    """Implementation of run command."""
    # Build command for lq run
    cmd_parts = ["lq", "run", "--json", "--quiet"]
    cmd_parts.append(command)
    if args:
        cmd_parts.extend(args)

    try:
        result = subprocess.run(
            cmd_parts,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        # Parse JSON output
        if result.stdout.strip():
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                pass

        # Fallback: construct basic result
        return {
            "run_id": None,
            "status": "FAIL" if result.returncode != 0 else "OK",
            "exit_code": result.returncode,
            "error_count": 0,
            "warning_count": 0,
            "errors": [],
            "output": result.stdout[:1000] if result.stdout else None,
            "stderr": result.stderr[:1000] if result.stderr else None,
        }
    except subprocess.TimeoutExpired:
        return {
            "run_id": None,
            "status": "FAIL",
            "exit_code": -1,
            "error": f"Command timed out after {timeout} seconds",
            "error_count": 0,
            "warning_count": 0,
            "errors": [],
        }
    except Exception as e:
        return {
            "run_id": None,
            "status": "FAIL",
            "exit_code": -1,
            "error": str(e),
            "error_count": 0,
            "warning_count": 0,
            "errors": [],
        }


def _query_impl(sql: str, limit: int = 100) -> dict[str, Any]:
    """Implementation of query command."""
    try:
        store = _get_store()
        conn = store.connection

        # Add LIMIT if not present (basic safety)
        sql_upper = sql.upper()
        if "LIMIT" not in sql_upper:
            sql = f"SELECT * FROM ({sql}) LIMIT {limit}"

        result = conn.sql(sql)
        columns = result.columns
        rows = result.fetchall()

        return {
            "columns": columns,
            "rows": [list(row) for row in rows],
            "row_count": len(rows),
        }
    except FileNotFoundError:
        return {"columns": [], "rows": [], "row_count": 0, "error": "No lq repository found"}
    except Exception as e:
        return {"columns": [], "rows": [], "row_count": 0, "error": str(e)}


def _errors_impl(
    limit: int = 20,
    run_id: int | None = None,
    source: str | None = None,
    file_pattern: str | None = None,
) -> dict[str, Any]:
    """Implementation of errors command."""
    try:
        store = _get_store()
        if not store.has_data():
            return {"errors": [], "total_count": 0}

        query = store.errors()

        if run_id is not None:
            query = query.filter(run_id=run_id)
        if source:
            query = query.filter(source_name=source)
        if file_pattern:
            query = query.filter(file_path=file_pattern)

        total_count = query.count()
        query = query.order_by("run_id", desc=True).limit(limit)
        df = query.df()

        error_list = []
        for _, row in df.iterrows():
            error_list.append(
                {
                    "ref": _format_ref(int(row.get("run_id", 0)), int(row.get("event_id", 0))),
                    "file_path": row.get("file_path"),
                    "line_number": int(row["line_number"])
                    if row.get("line_number") is not None
                    else None,
                    "column_number": int(row["column_number"])
                    if row.get("column_number") is not None
                    else None,
                    "message": row.get("message"),
                    "tool_name": row.get("tool_name"),
                    "category": row.get("category"),
                }
            )

        return {"errors": error_list, "total_count": total_count}
    except FileNotFoundError:
        return {"errors": [], "total_count": 0}


def _warnings_impl(
    limit: int = 20,
    run_id: int | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """Implementation of warnings command."""
    try:
        store = _get_store()
        if not store.has_data():
            return {"warnings": [], "total_count": 0}

        query = store.warnings()

        if run_id is not None:
            query = query.filter(run_id=run_id)
        if source:
            query = query.filter(source_name=source)

        total_count = query.count()
        query = query.order_by("run_id", desc=True).limit(limit)
        df = query.df()

        warning_list = []
        for _, row in df.iterrows():
            warning_list.append(
                {
                    "ref": _format_ref(int(row.get("run_id", 0)), int(row.get("event_id", 0))),
                    "file_path": row.get("file_path"),
                    "line_number": int(row["line_number"])
                    if row.get("line_number") is not None
                    else None,
                    "column_number": int(row["column_number"])
                    if row.get("column_number") is not None
                    else None,
                    "message": row.get("message"),
                    "tool_name": row.get("tool_name"),
                    "category": row.get("category"),
                }
            )

        return {"warnings": warning_list, "total_count": total_count}
    except FileNotFoundError:
        return {"warnings": [], "total_count": 0}


def _event_impl(ref: str) -> dict[str, Any] | None:
    """Implementation of event command."""
    try:
        run_id, event_id = _parse_ref(ref)
        store = _get_store()
        event_data = store.event(run_id, event_id)

        if event_data is None:
            return None

        # Environment is now stored as MAP, convert to dict if needed
        environment = event_data.get("environment")
        if environment is not None and not isinstance(environment, dict):
            # Handle legacy JSON format
            try:
                environment = json.loads(environment)
            except (json.JSONDecodeError, TypeError):
                environment = None

        return {
            "ref": ref,
            "run_id": run_id,
            "event_id": event_id,
            "severity": event_data.get("severity"),
            "file_path": event_data.get("file_path"),
            "line_number": event_data.get("line_number"),
            "column_number": event_data.get("column_number"),
            "message": event_data.get("message"),
            "tool_name": event_data.get("tool_name"),
            "category": event_data.get("category"),
            "error_fingerprint": event_data.get("error_fingerprint"),
            "raw_text": event_data.get("raw_text"),
            "log_line_start": event_data.get("log_line_start"),
            "log_line_end": event_data.get("log_line_end"),
            # Execution context
            "cwd": event_data.get("cwd"),
            "executable_path": event_data.get("executable_path"),
            "environment": environment,
            # System context
            "hostname": event_data.get("hostname"),
            "platform": event_data.get("platform"),
            "arch": event_data.get("arch"),
            # Git context
            "git_commit": event_data.get("git_commit"),
            "git_branch": event_data.get("git_branch"),
            "git_dirty": event_data.get("git_dirty"),
            # CI context
            "ci": event_data.get("ci"),
        }
    except (ValueError, FileNotFoundError):
        return None


def _context_impl(ref: str, lines: int = 5) -> dict[str, Any]:
    """Implementation of context command."""
    try:
        run_id, event_id = _parse_ref(ref)
        store = _get_store()
        event_data = store.event(run_id, event_id)

        if event_data is None:
            return {"ref": ref, "context_lines": [], "error": "Event not found"}

        # Get context from nearby events in the same run
        context_lines = []

        log_line_start = event_data.get("log_line_start")
        log_line_end = event_data.get("log_line_end")

        if log_line_start is not None:
            # Get events near this log line
            nearby = (
                store.run(run_id)
                .filter(f"log_line_start >= {log_line_start - lines}")
                .filter(f"log_line_end <= {log_line_end + lines}" if log_line_end else "TRUE")
                .order_by("log_line_start")
                .limit(lines * 2 + 1)
                .df()
            )

            for _, row in nearby.iterrows():
                is_event = row.get("run_id") == run_id and row.get("event_id") == event_id
                context_lines.append(
                    {
                        "line": row.get("log_line_start"),
                        "text": row.get("raw_text") or row.get("message", ""),
                        "is_event": is_event,
                    }
                )
        else:
            # No line info, just return the event itself
            context_lines.append(
                {
                    "line": None,
                    "text": event_data.get("raw_text") or event_data.get("message", ""),
                    "is_event": True,
                }
            )

        return {"ref": ref, "context_lines": context_lines}
    except (ValueError, FileNotFoundError):
        return {"ref": ref, "context_lines": [], "error": "Event not found"}


def _status_impl() -> dict[str, Any]:
    """Implementation of status command."""
    try:
        store = _get_store()
        if not store.has_data():
            return {"sources": []}

        # Get status for each source
        runs_df = store.runs()
        sources = []

        for _, row in runs_df.iterrows():
            error_count = store.run(row["run_id"]).filter(severity="error").count()
            warning_count = store.run(row["run_id"]).filter(severity="warning").count()

            if error_count > 0:
                status_str = "FAIL"
            elif warning_count > 0:
                status_str = "WARN"
            else:
                status_str = "OK"

            sources.append(
                {
                    "name": row.get("source_name", "unknown"),
                    "status": status_str,
                    "error_count": error_count,
                    "warning_count": warning_count,
                    "last_run": str(row.get("started_at", "")),
                    "run_id": int(row["run_id"]),
                }
            )

        return {"sources": sources}
    except FileNotFoundError:
        return {"sources": []}


def _history_impl(limit: int = 20, source: str | None = None) -> dict[str, Any]:
    """Implementation of history command."""
    try:
        store = _get_store()
        if not store.has_data():
            return {"runs": []}

        runs_df = store.runs()

        if source:
            runs_df = runs_df[runs_df["source_name"] == source]

        runs_df = runs_df.head(limit)
        runs = []

        for _, row in runs_df.iterrows():
            error_count = store.run(row["run_id"]).filter(severity="error").count()
            warning_count = store.run(row["run_id"]).filter(severity="warning").count()

            if error_count > 0:
                status_str = "FAIL"
            elif warning_count > 0:
                status_str = "WARN"
            else:
                status_str = "OK"

            runs.append(
                {
                    "run_id": int(row["run_id"]),
                    "source_name": _to_json_safe(row.get("source_name")) or "unknown",
                    "status": status_str,
                    "error_count": error_count,
                    "warning_count": warning_count,
                    "started_at": str(row.get("started_at", "")),
                    "exit_code": int(row["exit_code"])
                    if not pd.isna(row.get("exit_code"))
                    else None,
                    "command": _to_json_safe(row.get("command")),
                    "cwd": _to_json_safe(row.get("cwd")),
                    "executable_path": _to_json_safe(row.get("executable_path")),
                    "hostname": _to_json_safe(row.get("hostname")),
                    "platform": _to_json_safe(row.get("platform")),
                    "arch": _to_json_safe(row.get("arch")),
                    "git_commit": _to_json_safe(row.get("git_commit")),
                    "git_branch": _to_json_safe(row.get("git_branch")),
                    "git_dirty": _to_json_safe(row.get("git_dirty")),
                    "ci": _to_json_safe(row.get("ci")),
                }
            )
        return {"runs": runs}
    except FileNotFoundError:
        return {"runs": []}


def _diff_impl(run1: int, run2: int) -> dict[str, Any]:
    """Implementation of diff command."""
    try:
        store = _get_store()

        # Get errors from each run
        errors1 = store.run(run1).filter(severity="error").df()
        errors2 = store.run(run2).filter(severity="error").df()

        # Use fingerprints for comparison if available, else use file+line+message
        def get_error_key(row):
            fp = row.get("error_fingerprint")
            if fp:
                return fp
            return f"{row.get('file_path')}:{row.get('line_number')}:{row.get('message', '')[:50]}"

        keys1 = set(get_error_key(row) for _, row in errors1.iterrows())
        keys2 = set(get_error_key(row) for _, row in errors2.iterrows())

        fixed_keys = keys1 - keys2
        new_keys = keys2 - keys1
        unchanged_keys = keys1 & keys2

        # Build fixed and new error lists
        fixed = []
        for _, row in errors1.iterrows():
            if get_error_key(row) in fixed_keys:
                fixed.append(
                    {
                        "file_path": row.get("file_path"),
                        "message": row.get("message"),
                    }
                )

        new_errors = []
        for _, row in errors2.iterrows():
            if get_error_key(row) in new_keys:
                new_errors.append(
                    {
                        "ref": _format_ref(run2, int(row.get("event_id", 0))),
                        "file_path": row.get("file_path"),
                        "line_number": row.get("line_number"),
                        "message": row.get("message"),
                    }
                )

        return {
            "summary": {
                "run1_errors": len(errors1),
                "run2_errors": len(errors2),
                "fixed": len(fixed_keys),
                "new": len(new_keys),
                "unchanged": len(unchanged_keys),
            },
            "fixed": fixed,
            "new": new_errors,
        }
    except FileNotFoundError:
        return {
            "summary": {"run1_errors": 0, "run2_errors": 0, "fixed": 0, "new": 0, "unchanged": 0},
            "fixed": [],
            "new": [],
            "error": "No lq repository found",
        }


def _register_command_impl(
    name: str,
    cmd: str,
    description: str = "",
    timeout: int = 300,
    capture: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """Implementation of register_command."""
    try:
        from lq.cli import RegisteredCommand, load_commands, save_commands
        lq_dir = Path(".lq")

        if not lq_dir.exists():
            return {"success": False, "error": "No lq repository found. Run 'lq init' first."}

        commands = load_commands(lq_dir)

        if name in commands and not force:
            return {
                "success": False,
                "error": f"Command '{name}' already exists. Use force=true to overwrite.",
            }

        commands[name] = RegisteredCommand(
            name=name,
            cmd=cmd,
            description=description,
            timeout=timeout,
            capture=capture,
        )
        save_commands(lq_dir, commands)

        return {
            "success": True,
            "message": f"Registered command '{name}': {cmd}",
            "command": {
                "name": name,
                "cmd": cmd,
                "description": description,
                "timeout": timeout,
                "capture": capture,
            },
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def _unregister_command_impl(name: str) -> dict[str, Any]:
    """Implementation of unregister_command."""
    try:
        from lq.cli import load_commands, save_commands
        lq_dir = Path(".lq")

        if not lq_dir.exists():
            return {"success": False, "error": "No lq repository found."}

        commands = load_commands(lq_dir)

        if name not in commands:
            return {"success": False, "error": f"Command '{name}' not found."}

        del commands[name]
        save_commands(lq_dir, commands)

        return {"success": True, "message": f"Unregistered command '{name}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _list_commands_impl() -> dict[str, Any]:
    """Implementation of list_commands."""
    try:
        from lq.cli import load_commands
        lq_dir = Path(".lq")

        if not lq_dir.exists():
            return {"commands": []}

        commands = load_commands(lq_dir)

        return {
            "commands": [
                {
                    "name": name,
                    "cmd": cmd.cmd,
                    "description": cmd.description,
                    "timeout": cmd.timeout,
                    "capture": cmd.capture,
                }
                for name, cmd in commands.items()
            ]
        }
    except Exception as e:
        return {"commands": [], "error": str(e)}


# ============================================================================
# Tools (thin wrappers around implementations)
# ============================================================================


@mcp.tool()
def run(command: str, args: list[str] | None = None, timeout: int = 300) -> dict[str, Any]:
    """Run a command and capture its output.

    Args:
        command: Command to run (registered name or shell command)
        args: Additional arguments
        timeout: Timeout in seconds (default: 300)

    Returns:
        Run result with status, errors, and warnings
    """
    return _run_impl(command, args, timeout)


@mcp.tool()
def query(sql: str, limit: int = 100) -> dict[str, Any]:
    """Query stored log events with SQL.

    Args:
        sql: SQL query against lq_events view
        limit: Max rows to return (default: 100)

    Returns:
        Query results with columns, rows, and row_count
    """
    return _query_impl(sql, limit)


@mcp.tool()
def errors(
    limit: int = 20,
    run_id: int | None = None,
    source: str | None = None,
    file_pattern: str | None = None,
) -> dict[str, Any]:
    """Get recent errors.

    Args:
        limit: Max errors to return (default: 20)
        run_id: Filter to specific run
        source: Filter to specific source name
        file_pattern: Filter by file path pattern (SQL LIKE)

    Returns:
        Errors list with total count
    """
    return _errors_impl(limit, run_id, source, file_pattern)


@mcp.tool()
def warnings(
    limit: int = 20,
    run_id: int | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """Get recent warnings.

    Args:
        limit: Max warnings to return (default: 20)
        run_id: Filter to specific run
        source: Filter to specific source name

    Returns:
        Warnings list with total count
    """
    return _warnings_impl(limit, run_id, source)


@mcp.tool()
def event(ref: str) -> dict[str, Any] | None:
    """Get details for a specific event by reference.

    Args:
        ref: Event reference (e.g., "1:3")

    Returns:
        Event details or None if not found
    """
    return _event_impl(ref)


@mcp.tool()
def context(ref: str, lines: int = 5) -> dict[str, Any]:
    """Get log context around a specific event.

    Args:
        ref: Event reference (e.g., "1:3")
        lines: Lines of context before/after (default: 5)

    Returns:
        Context lines around the event
    """
    return _context_impl(ref, lines)


@mcp.tool()
def status() -> dict[str, Any]:
    """Get current status summary of all sources.

    Returns:
        Status summary with sources list
    """
    return _status_impl()


@mcp.tool()
def history(limit: int = 20, source: str | None = None) -> dict[str, Any]:
    """Get run history.

    Args:
        limit: Max runs to return (default: 20)
        source: Filter to specific source name

    Returns:
        Run history list
    """
    return _history_impl(limit, source)


@mcp.tool()
def diff(run1: int, run2: int) -> dict[str, Any]:
    """Compare errors between two runs.

    Args:
        run1: First run ID (baseline)
        run2: Second run ID (comparison)

    Returns:
        Diff summary with fixed and new errors
    """
    return _diff_impl(run1, run2)


@mcp.tool()
def register_command(
    name: str,
    cmd: str,
    description: str = "",
    timeout: int = 300,
    capture: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """Register a new command.

    Args:
        name: Command name (e.g., 'build', 'test')
        cmd: Command to run
        description: Command description
        timeout: Timeout in seconds (default: 300)
        capture: Whether to capture and parse logs (default: true)
        force: Overwrite existing command if it exists

    Returns:
        Success status and registered command details
    """
    return _register_command_impl(name, cmd, description, timeout, capture, force)


@mcp.tool()
def unregister_command(name: str) -> dict[str, Any]:
    """Remove a registered command.

    Args:
        name: Command name to remove

    Returns:
        Success status
    """
    return _unregister_command_impl(name)


@mcp.tool()
def list_commands() -> dict[str, Any]:
    """List all registered commands.

    Returns:
        List of registered commands with their configuration
    """
    return _list_commands_impl()


# ============================================================================
# Resources
# ============================================================================


@mcp.resource("lq://status")
def resource_status() -> str:
    """Current status of all sources."""
    result = _status_impl()
    return json.dumps(result, indent=2, default=str)


@mcp.resource("lq://runs")
def resource_runs() -> str:
    """List of all runs."""
    result = _history_impl(limit=100)
    return json.dumps(result, indent=2, default=str)


@mcp.resource("lq://events")
def resource_events() -> str:
    """All stored events."""
    result = _errors_impl(limit=100)
    return json.dumps(result, indent=2, default=str)


@mcp.resource("lq://event/{ref}")
def resource_event(ref: str) -> str:
    """Single event details."""
    result = _event_impl(ref)
    return json.dumps(result, indent=2, default=str)


@mcp.resource("lq://commands")
def resource_commands() -> str:
    """Registered commands."""
    try:
        from lq.cli import load_commands

        lq_dir = Path(".lq")
        if lq_dir.exists():
            commands = load_commands(lq_dir)
            return json.dumps({"commands": commands}, indent=2, default=str)
    except Exception:
        pass
    return json.dumps({"commands": []}, indent=2)


# ============================================================================
# Prompts
# ============================================================================


@mcp.prompt(name="fix-errors")
def fix_errors(run_id: int | None = None, file_pattern: str | None = None) -> str:
    """Guide through fixing build errors systematically."""
    # Get current errors
    error_result = _errors_impl(limit=20, run_id=run_id, file_pattern=file_pattern)
    status_result = _status_impl()

    # Build status table
    status_lines = [
        "| Source | Status | Errors | Warnings |",
        "|--------|--------|--------|----------|",
    ]
    for src in status_result.get("sources", []):
        status_lines.append(
            f"| {src['name']} | {src['status']} | {src['error_count']} | {src['warning_count']} |"
        )
    status_table = "\n".join(status_lines)

    # Build error list
    error_lines = []
    for i, err in enumerate(error_result.get("errors", []), 1):
        loc = f"{err.get('file_path', '?')}:{err.get('line_number', '?')}"
        if err.get("column_number"):
            loc += f":{err['column_number']}"
        error_lines.append(
            f"{i}. **ref: {err['ref']}** `{loc}`\n   ```\n   {err.get('message', '')}\n   ```"
        )
    error_list = "\n\n".join(error_lines) if error_lines else "No errors found."

    return f"""You are helping fix build errors in a software project.

## Current Status

{status_table}

## Errors to Fix

{error_list}

## Instructions

1. Read each error and understand the root cause
2. Use `event(ref="...")` for full context if the message is unclear
3. Use `context(ref="...")` to see surrounding log lines
4. Fix errors in dependency order:
   - Missing includes/declarations first
   - Then type errors
   - Then syntax errors
5. After fixing, run `run(command="...")` to verify
6. Repeat until build passes

Focus on fixing the root cause, not just suppressing warnings."""


@mcp.prompt(name="analyze-regression")
def analyze_regression(good_run: int | None = None, bad_run: int | None = None) -> str:
    """Help identify why a build started failing between two runs."""
    # Get run history to find good/bad runs if not specified
    hist = _history_impl(limit=10)
    runs = hist.get("runs", [])

    if not runs:
        return 'No runs found. Run a build first with `run(command="...")`.'

    if bad_run is None:
        bad_run = runs[0]["run_id"] if runs else 1
    if good_run is None:
        # Find last passing run
        for r in runs[1:]:
            if r["status"] == "OK":
                good_run = r["run_id"]
                break
        if good_run is None:
            good_run = bad_run - 1 if bad_run > 1 else 1

    # Get diff
    diff_result = _diff_impl(good_run, bad_run)
    summary = diff_result.get("summary", {})

    # Build new errors list
    new_error_lines = []
    for err in diff_result.get("new", []):
        loc = f"{err.get('file_path', '?')}:{err.get('line_number', '?')}"
        new_error_lines.append(f"- **ref: {err['ref']}** `{loc}`\n  {err.get('message', '')}")
    new_errors = "\n".join(new_error_lines) if new_error_lines else "None"

    return f"""You are analyzing why a build started failing.

## Run Comparison

| Metric | Run {good_run} (good) | Run {bad_run} (bad) | Delta |
|--------|--------------|-------------|-------|
| Errors | {summary.get("run1_errors", 0)} | {summary.get("run2_errors", 0)} | \
+{summary.get("new", 0)} |

## New Errors (not in Run {good_run})

{new_errors}

## Instructions

1. Review the new errors that appeared
2. Look for patterns (same file, same error type)
3. Use `event(ref="...")` for full error context
4. Identify the root cause
5. Suggest the minimal fix to restore the build"""


@mcp.prompt(name="summarize-run")
def summarize_run(run_id: int | None = None, format: str = "brief") -> str:
    """Generate a concise summary of a build/test run."""
    hist = _history_impl(limit=1)
    runs = hist.get("runs", [])

    if not runs:
        return 'No runs found. Run a build first with `run(command="...")`.'

    if run_id is None:
        run_id = runs[0]["run_id"]

    # Get run info
    run_info = None
    for r in runs:
        if r["run_id"] == run_id:
            run_info = r
            break

    if not run_info:
        run_info = runs[0]

    error_result = _errors_impl(limit=10, run_id=run_id)

    # Build error details
    error_lines = []
    for err in error_result.get("errors", []):
        loc = f"{err.get('file_path', '?')}:{err.get('line_number', '?')}"
        error_lines.append(f"- `{loc}` - {err.get('message', '')[:80]}")
    error_details = "\n".join(error_lines) if error_lines else "No errors"

    return f"""Summarize this build/test run.

## Run Details

- **Run ID:** {run_info["run_id"]}
- **Status:** {run_info["status"]}
- **Errors:** {run_info.get("error_count", 0)}
- **Warnings:** {run_info.get("warning_count", 0)}

## Error Details

{error_details}

## Instructions

Generate a summary suitable for a GitHub PR comment:
- Lead with pass/fail status
- List the key errors (not all warnings)
- Suggest what might have caused the failure
- Keep it concise"""


@mcp.prompt(name="investigate-flaky")
def investigate_flaky(test_pattern: str | None = None, lookback: int = 10) -> str:
    """Help investigate intermittently failing tests."""
    hist = _history_impl(limit=lookback)
    runs = hist.get("runs", [])

    if not runs:
        return 'No runs found. Run tests first with `run(command="...")`.'

    # Build history table
    history_lines = ["| Run | Status | Errors |", "|-----|--------|--------|"]
    for r in runs:
        history_lines.append(f"| {r['run_id']} | {r['status']} | {r.get('error_count', 0)} |")
    history_table = "\n".join(history_lines)

    return f"""You are investigating flaky (intermittently failing) tests.

## Test History (last {lookback} runs)

{history_table}

## Instructions

1. Look for patterns in failures
2. Use `errors(run_id=N)` to see errors for specific runs
3. Use `event(ref="...")` for detailed failure output
4. Look for:
   - Race conditions (concurrent, parallel, thread)
   - Timing issues (timeout, sleep, wait)
   - Resource contention (connection, file, lock)
5. Suggest fixes to make tests more deterministic"""


# ============================================================================
# Entry point
# ============================================================================


def serve(transport: str = "stdio", port: int = 8080) -> None:
    """Start the MCP server.

    Args:
        transport: Transport type ("stdio" or "sse")
        port: Port for SSE transport
    """
    if transport == "stdio":
        mcp.run()
    elif transport == "sse":
        mcp.run(transport="sse", port=port)
    else:
        raise ValueError(f"Unknown transport: {transport}")


if __name__ == "__main__":
    serve()
