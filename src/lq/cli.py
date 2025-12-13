"""
lq CLI - Log Query command-line interface.

Usage:
    lq init                          Initialize .lq directory
    lq run <command>                 Run command and capture output
    lq import <file> [--name NAME]   Import existing log file
    lq capture [--name NAME]         Capture from stdin
    lq status                        Show status of all sources
    lq errors [--source S] [-n N]    Show recent errors
    lq warnings [--source S] [-n N]  Show recent warnings
    lq summary                       Aggregate summary
    lq sql <query>                   Run arbitrary SQL
    lq shell                         Interactive SQL shell
    lq history [-n N]                Show run history
    lq prune [--older-than DAYS]     Remove old logs
    lq event <ref>                   Show event details by reference (e.g., 5:3)
    lq query [options] [file...]     Query log files or stored events (alias: q)
    lq filter [expr...] [file...]    Filter with simple syntax (alias: f)

Query examples:
    lq q build.log                           # all events from file
    lq q -s file_path,message build.log      # select columns
    lq q -f "severity='error'" build.log     # filter with SQL WHERE
    lq q -f "severity='error'"               # query stored events

Filter examples:
    lq f severity=error build.log            # filter by exact match
    lq f severity=error,warning build.log    # OR within field
    lq f file_path~main build.log            # contains (LIKE)
    lq f severity!=info build.log            # not equal
    lq f -v severity=error build.log         # invert (grep -v style)
    lq f -c severity=error build.log         # count matches only
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from lq.query import LogQuery, LogStore


# ============================================================================
# Result Types
# ============================================================================

@dataclass
class EventRef:
    """Reference to a specific event within a run."""
    run_id: int
    event_id: int

    def __str__(self) -> str:
        return f"{self.run_id}:{self.event_id}"

    @classmethod
    def parse(cls, ref: str) -> "EventRef":
        """Parse a reference string like '5:3' into an EventRef."""
        parts = ref.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid event reference: {ref}. Expected format: run_id:event_id")
        return cls(run_id=int(parts[0]), event_id=int(parts[1]))


@dataclass
class EventSummary:
    """Summary of a parsed event for structured output."""
    ref: str
    severity: str | None
    file_path: str | None
    line_number: int | None
    column_number: int | None
    message: str | None
    error_code: str | None = None
    fingerprint: str | None = None
    # For test results
    test_name: str | None = None
    # For log line context (when available)
    log_line_start: int | None = None
    log_line_end: int | None = None

    def location(self) -> str:
        """Format as file:line:col string."""
        if not self.file_path:
            return "?"
        loc = self.file_path
        if self.line_number is not None:
            loc += f":{self.line_number}"
            if self.column_number and self.column_number > 0:
                loc += f":{self.column_number}"
        return loc


@dataclass
class RunResult:
    """Structured result from running a command."""
    run_id: int
    command: str
    status: str  # "OK", "FAIL", "WARN"
    exit_code: int
    started_at: str
    completed_at: str
    duration_sec: float
    summary: dict[str, int] = field(default_factory=dict)
    errors: list[EventSummary] = field(default_factory=list)
    warnings: list[EventSummary] = field(default_factory=list)
    parquet_path: str | None = None

    def to_json(self, include_warnings: bool = False) -> str:
        """Convert to JSON string."""
        data = {
            "run_id": self.run_id,
            "command": self.command,
            "status": self.status,
            "exit_code": self.exit_code,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_sec": round(self.duration_sec, 3),
            "summary": self.summary,
            "errors": [asdict(e) for e in self.errors],
        }
        if include_warnings:
            data["warnings"] = [asdict(w) for w in self.warnings]
        return json.dumps(data, indent=2)

    def to_markdown(self, include_warnings: bool = False) -> str:
        """Convert to markdown summary."""
        badge = {"OK": "✓", "FAIL": "✗", "WARN": "⚠"}.get(self.status, "?")
        lines = [
            f"## {badge} Build Result: {self.status}",
            "",
            f"**Command:** `{self.command}`",
            f"**Duration:** {self.duration_sec:.1f}s | **Exit code:** {self.exit_code} | **Run ID:** {self.run_id}",
            "",
        ]

        if self.errors:
            lines.append(f"### Errors ({len(self.errors)})")
            lines.append("")
            for e in self.errors[:20]:  # Limit to 20 in markdown
                msg = (e.message or "")[:100]
                lines.append(f"- `{e.location()}` [{e.ref}] - {msg}")
            if len(self.errors) > 20:
                lines.append(f"- ... and {len(self.errors) - 20} more errors")
            lines.append("")

        if include_warnings and self.warnings:
            lines.append(f"### Warnings ({len(self.warnings)})")
            lines.append("")
            for w in self.warnings[:10]:
                msg = (w.message or "")[:100]
                lines.append(f"- `{w.location()}` [{w.ref}] - {msg}")
            if len(self.warnings) > 10:
                lines.append(f"- ... and {len(self.warnings) - 10} more warnings")
            lines.append("")

        if not self.errors and not (include_warnings and self.warnings):
            lines.append("No errors or warnings detected.")
            lines.append("")

        return "\n".join(lines)

# ============================================================================
# Configuration
# ============================================================================

LQ_DIR = ".lq"
LOGS_DIR = "logs"
RAW_DIR = "raw"
SCHEMA_FILE = "schema.sql"
COMMANDS_FILE = "commands.yaml"


# ============================================================================
# Command Registry
# ============================================================================

@dataclass
class RegisteredCommand:
    """A registered command in the commands.yaml file."""
    name: str
    cmd: str
    description: str = ""
    timeout: int = 300
    format: str = "auto"

    def to_dict(self) -> dict[str, Any]:
        return {
            "cmd": self.cmd,
            "description": self.description,
            "timeout": self.timeout,
            "format": self.format,
        }


def load_commands(lq_dir: Path) -> dict[str, RegisteredCommand]:
    """Load registered commands from commands.yaml."""
    commands_path = lq_dir / COMMANDS_FILE
    if not commands_path.exists():
        return {}

    try:
        import yaml
        with open(commands_path) as f:
            data = yaml.safe_load(f) or {}
    except ImportError:
        # Fallback to basic YAML parsing if PyYAML not installed
        data = _parse_simple_yaml(commands_path.read_text())

    commands = {}
    for name, config in data.get("commands", {}).items():
        if isinstance(config, str):
            # Simple format: name: "command"
            commands[name] = RegisteredCommand(name=name, cmd=config)
        else:
            # Full format with options
            commands[name] = RegisteredCommand(
                name=name,
                cmd=config.get("cmd", ""),
                description=config.get("description", ""),
                timeout=config.get("timeout", 300),
                format=config.get("format", "auto"),
            )
    return commands


def save_commands(lq_dir: Path, commands: dict[str, RegisteredCommand]) -> None:
    """Save registered commands to commands.yaml."""
    commands_path = lq_dir / COMMANDS_FILE

    try:
        import yaml
        data = {"commands": {name: cmd.to_dict() for name, cmd in commands.items()}}
        with open(commands_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    except ImportError:
        # Fallback to simple YAML writing
        lines = ["commands:"]
        for name, cmd in commands.items():
            lines.append(f"  {name}:")
            lines.append(f'    cmd: "{cmd.cmd}"')
            if cmd.description:
                lines.append(f'    description: "{cmd.description}"')
            if cmd.timeout != 300:
                lines.append(f"    timeout: {cmd.timeout}")
            if cmd.format != "auto":
                lines.append(f'    format: "{cmd.format}"')
        commands_path.write_text("\n".join(lines) + "\n")


def _parse_simple_yaml(content: str) -> dict:
    """Basic YAML parser for commands.yaml (fallback if PyYAML not installed)."""
    result: dict = {"commands": {}}
    current_cmd = None
    current_data: dict = {}

    for line in content.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip())

        if stripped == "commands:":
            continue
        elif indent == 2 and stripped.endswith(":"):
            # New command
            if current_cmd:
                result["commands"][current_cmd] = current_data
            current_cmd = stripped[:-1]
            current_data = {}
        elif indent == 4 and ":" in stripped:
            # Command property
            key, value = stripped.split(":", 1)
            value = value.strip().strip('"').strip("'")
            if key == "timeout":
                value = int(value)
            current_data[key] = value

    if current_cmd:
        result["commands"][current_cmd] = current_data

    return result


# ============================================================================
# Database Connection
# ============================================================================

def get_lq_dir() -> Path | None:
    """Find .lq directory in current or parent directories.

    Returns None if no .lq directory is found.
    """
    cwd = Path.cwd()
    for p in [cwd, *list(cwd.parents)]:
        lq_path = p / LQ_DIR
        if lq_path.exists():
            return lq_path
    return None


def ensure_initialized() -> Path:
    """Ensure .lq directory exists."""
    lq_dir = get_lq_dir()
    if lq_dir is None or not lq_dir.exists():
        print("Error: .lq not initialized. Run 'lq init' first.", file=sys.stderr)
        sys.exit(1)
    return lq_dir


class ConnectionFactory:
    """Factory for creating properly initialized DuckDB connections.

    Handles:
    - duck_hunt extension loading/installation
    - Schema loading for stored data queries
    - Future: persistent database support
    """

    _duck_hunt_available: bool | None = None

    @classmethod
    def check_duck_hunt(cls, conn: duckdb.DuckDBPyConnection) -> bool:
        """Check if duck_hunt is available (cached)."""
        if cls._duck_hunt_available is None:
            try:
                conn.execute("LOAD duck_hunt")
                cls._duck_hunt_available = True
            except duckdb.Error:
                cls._duck_hunt_available = False
        return cls._duck_hunt_available

    @classmethod
    def install_duck_hunt(cls, conn: duckdb.DuckDBPyConnection) -> bool:
        """Install duck_hunt extension from community repo.

        Returns True if successful, False otherwise.
        """
        try:
            conn.execute("INSTALL duck_hunt FROM community")
            conn.execute("LOAD duck_hunt")
            cls._duck_hunt_available = True
            return True
        except duckdb.Error:
            cls._duck_hunt_available = False
            return False

    @classmethod
    def create(
        cls,
        lq_dir: Path | None = None,
        load_schema: bool = True,
        require_duck_hunt: bool = False,
        install_duck_hunt: bool = False,
    ) -> duckdb.DuckDBPyConnection:
        """Create a properly initialized DuckDB connection.

        Args:
            lq_dir: Path to .lq directory (for schema loading)
            load_schema: Whether to load the schema (for stored data queries)
            require_duck_hunt: If True, raise error if duck_hunt unavailable
            install_duck_hunt: If True, attempt to install duck_hunt if missing

        Returns:
            Initialized DuckDB connection

        Raises:
            duckdb.Error: If require_duck_hunt=True and duck_hunt unavailable
        """
        conn = duckdb.connect(":memory:")

        # Handle duck_hunt loading
        duck_hunt_loaded = False
        try:
            conn.execute("LOAD duck_hunt")
            duck_hunt_loaded = True
            cls._duck_hunt_available = True
        except duckdb.Error:
            if install_duck_hunt:
                duck_hunt_loaded = cls.install_duck_hunt(conn)

            if require_duck_hunt and not duck_hunt_loaded:
                raise duckdb.Error(
                    "duck_hunt extension required but not available. "
                    "Run 'lq init' to install required extensions."
                )

        # Load schema if requested and lq_dir provided
        if load_schema and lq_dir is not None:
            cls._load_schema(conn, lq_dir)

        return conn

    @classmethod
    def _load_schema(cls, conn: duckdb.DuckDBPyConnection, lq_dir: Path) -> None:
        """Load schema into connection."""
        # Set up absolute path for lq_base_path before loading schema
        logs_path = (lq_dir / LOGS_DIR).resolve()
        conn.execute(f"CREATE OR REPLACE MACRO lq_base_path() AS '{logs_path}'")

        # Load schema (which will use our lq_base_path)
        schema_path = lq_dir / SCHEMA_FILE
        if schema_path.exists():
            schema_sql = schema_path.read_text()
            # Execute each statement separately
            for stmt in schema_sql.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                # Skip the lq_base_path definition since we already set it with absolute path
                if "lq_base_path()" in stmt and "CREATE" in stmt.upper() and "MACRO" in stmt.upper():
                    continue
                # Skip pure comment blocks
                lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
                if not lines:
                    continue
                try:
                    conn.execute(stmt)
                except duckdb.Error as e:
                    # Skip errors from missing functions (status_badge if duck_hunt not loaded)
                    if "status_badge" not in str(e):
                        pass  # Ignore other schema errors for now


def get_connection(lq_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection with schema loaded.

    This is a convenience wrapper around ConnectionFactory.create() for
    backward compatibility.
    """
    if lq_dir is None:
        lq_dir = ensure_initialized()
    return ConnectionFactory.create(lq_dir=lq_dir, load_schema=True)


def get_next_run_id(lq_dir: Path) -> int:
    """Get next run ID by scanning existing files."""
    logs_dir = lq_dir / LOGS_DIR
    if not logs_dir.exists():
        return 1

    max_id = 0
    for f in logs_dir.rglob("*.parquet"):
        try:
            run_id = int(f.stem.split("_")[0])
            max_id = max(max_id, run_id)
        except (ValueError, IndexError):
            pass
    return max_id + 1


# ============================================================================
# Parquet Writing
# ============================================================================

def write_run_parquet(
    events: list[dict[str, Any]],
    run_meta: dict[str, Any],
    lq_dir: Path,
) -> Path:
    """Write events to a Hive-partitioned parquet file."""
    # Determine partition path
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H%M%S")
    source_type = run_meta.get("source_type", "run")
    run_id = run_meta["run_id"]
    name = run_meta.get("source_name", "unknown")
    # Sanitize name for filename
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)[:50]

    partition_dir = lq_dir / LOGS_DIR / f"date={date_str}" / f"source={source_type}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{run_id:03d}_{safe_name}_{time_str}.parquet"
    filepath = partition_dir / filename

    # Add run metadata to each event
    enriched_events = []
    for event in events or [{}]:
        enriched = {
            "run_id": run_id,
            "source_name": run_meta.get("source_name"),
            "source_type": source_type,
            "command": run_meta.get("command"),
            "started_at": run_meta.get("started_at"),
            "completed_at": run_meta.get("completed_at"),
            "exit_code": run_meta.get("exit_code"),
            **event,
        }
        enriched_events.append(enriched)

    # Write using DuckDB
    conn = duckdb.connect(":memory:")
    # Register the list as a table-like object
    df = pd.DataFrame(enriched_events)
    conn.register("events_df", df)
    conn.execute("CREATE TABLE events AS SELECT * FROM events_df")
    conn.execute(f"COPY events TO '{filepath}' (FORMAT PARQUET)")
    conn.close()

    return filepath


# ============================================================================
# Log Parsing
# ============================================================================

def parse_log_content(content: str, format_hint: str = "auto") -> list[dict[str, Any]]:
    """Parse log content, using duck_hunt if available."""
    conn = duckdb.connect(":memory:")

    # Try to load duck_hunt
    try:
        conn.execute("LOAD duck_hunt")
        has_duck_hunt = True
    except duckdb.Error:
        has_duck_hunt = False

    if has_duck_hunt:
        # Use duck_hunt's parse_duck_hunt_log function
        try:
            # Register content as a parameter and call parse_duck_hunt_log
            result = conn.execute(
                "SELECT * FROM parse_duck_hunt_log($1, $2)",
                [content, format_hint]
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            events = [dict(zip(columns, row)) for row in result]
            conn.close()
            return events
        except duckdb.Error as e:
            # Fall back to basic parsing if duck_hunt parsing fails
            pass

    conn.close()

    # Fallback: basic line-based parsing
    events = []
    for i, line in enumerate(content.splitlines(), 1):
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # Simple error/warning detection
        severity = None
        file_path = None
        line_number = None
        column_number = None
        message = line_stripped

        # Try to parse gcc/clang style: file:line:col: severity: message
        match = re.match(r'^([^:]+):(\d+):(?:(\d+):)?\s*(error|warning|note):\s*(.+)$', line_stripped, re.IGNORECASE)
        if match:
            file_path = match.group(1)
            line_number = int(match.group(2))
            column_number = int(match.group(3)) if match.group(3) else None
            severity = match.group(4).lower()
            message = match.group(5)
        elif ": error:" in line_stripped.lower() or line_stripped.lower().startswith("error:"):
            severity = "error"
        elif ": warning:" in line_stripped.lower() or line_stripped.lower().startswith("warning:"):
            severity = "warning"

        if severity:
            events.append({
                "event_id": len(events) + 1,
                "log_line_start": i,
                "log_line_end": i,
                "file_path": file_path,
                "line_number": line_number,
                "column_number": column_number,
                "severity": severity,
                "message": message,
                "tool_name": "lq_basic",
            })

    return events


# ============================================================================
# Commands
# ============================================================================

def cmd_init(args: argparse.Namespace) -> None:
    """Initialize .lq directory and install required extensions."""
    lq_dir = Path.cwd() / LQ_DIR

    if lq_dir.exists():
        print(f".lq already exists at {lq_dir}")
        # Still try to install extensions if they're missing
        _install_extensions()
        return

    # Create directories
    (lq_dir / LOGS_DIR).mkdir(parents=True)
    (lq_dir / RAW_DIR).mkdir(parents=True)

    # Copy schema file from package
    try:
        schema_content = resources.files("lq").joinpath("schema.sql").read_text()
        (lq_dir / SCHEMA_FILE).write_text(schema_content)
    except Exception as e:
        print(f"Warning: Could not copy schema.sql: {e}", file=sys.stderr)

    print(f"Initialized .lq at {lq_dir}")
    print("  logs/      - Hive-partitioned parquet files")
    print("  raw/       - Raw log files (optional)")
    print("  schema.sql - SQL schema and macros")

    # Install required extensions
    _install_extensions()


def _install_extensions() -> None:
    """Install required DuckDB extensions."""
    conn = duckdb.connect(":memory:")

    # Check if duck_hunt is already available
    try:
        conn.execute("LOAD duck_hunt")
        print("  duck_hunt  - Already installed")
        return
    except duckdb.Error:
        pass

    # Try to install duck_hunt
    print("  duck_hunt  - Installing from community repo...")
    if ConnectionFactory.install_duck_hunt(conn):
        print("  duck_hunt  - Installed successfully")
    else:
        print("  duck_hunt  - Installation failed (some features unavailable)", file=sys.stderr)
        print("             Run manually: INSTALL duck_hunt FROM community", file=sys.stderr)


def cmd_run(args: argparse.Namespace) -> None:
    """Run a command and capture its output."""
    lq_dir = ensure_initialized()

    # Check if first argument is a registered command name
    registered_commands = load_commands(lq_dir)
    first_arg = args.command[0]

    if first_arg in registered_commands and len(args.command) == 1:
        # Use registered command
        reg_cmd = registered_commands[first_arg]
        command = reg_cmd.cmd
        source_name = args.name or first_arg
        format_hint = args.format if args.format != "auto" else reg_cmd.format
        timeout = reg_cmd.timeout
    else:
        # Use literal command
        command = " ".join(args.command)
        source_name = args.name or first_arg
        format_hint = args.format
        timeout = None  # No timeout for ad-hoc commands

    run_id = get_next_run_id(lq_dir)
    started_at = datetime.now()

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
    }

    filepath = write_run_parquet(events, run_meta, lq_dir)

    # Build structured result
    error_events = [e for e in events if e.get("severity") == "error"]
    warning_events = [e for e in events if e.get("severity") == "warning"]

    def make_event_summary(e: dict[str, Any]) -> EventSummary:
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
        errors=[make_event_summary(e) for e in error_events[:args.error_limit]],
        warnings=[make_event_summary(e) for e in warning_events[:args.error_limit]],
        parquet_path=str(filepath),
    )

    # Output based on format
    if args.json:
        print(result.to_json(include_warnings=args.include_warnings))
    elif args.markdown:
        print(result.to_markdown(include_warnings=args.include_warnings))
    else:
        # Traditional output
        print(f"\n[lq] Captured {len(events)} events ({len(error_events)} errors, {len(warning_events)} warnings)", file=sys.stderr)
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


def cmd_status(args: argparse.Namespace) -> None:
    """Show status of all sources."""
    lq_dir = ensure_initialized()

    try:
        store = LogStore(lq_dir)
        conn = store.connection

        if args.verbose:
            result = conn.execute("FROM lq_status_verbose()").fetchdf()
        else:
            result = conn.execute("FROM lq_status()").fetchdf()
        print(result.to_string(index=False))
    except duckdb.Error:
        # Fallback if macros aren't working
        store = LogStore(lq_dir)
        result = store.events().limit(10).df()
        print(result.to_string(index=False))


def cmd_errors(args: argparse.Namespace) -> None:
    """Show recent errors."""
    lq_dir = ensure_initialized()

    try:
        store = LogStore(lq_dir)
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
    lq_dir = ensure_initialized()

    try:
        store = LogStore(lq_dir)
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
    lq_dir = ensure_initialized()

    try:
        store = LogStore(lq_dir)
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
    lq_dir = ensure_initialized()

    try:
        store = LogStore(lq_dir)
        result = store.runs().head(args.limit)
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)


def cmd_sql(args: argparse.Namespace) -> None:
    """Run arbitrary SQL."""
    lq_dir = ensure_initialized()

    sql = " ".join(args.query)
    try:
        store = LogStore(lq_dir)
        result = store.connection.execute(sql).fetchdf()
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_shell(args: argparse.Namespace) -> None:
    """Start interactive DuckDB shell."""
    lq_dir = ensure_initialized()

    # Create init file
    init_sql = f"""
.prompt 'lq> '
LOAD duck_hunt;
"""
    schema_path = lq_dir / SCHEMA_FILE
    if schema_path.exists():
        init_sql += f".read '{schema_path}'\n"

    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(init_sql)
        init_file = f.name

    try:
        subprocess.run(["duckdb", "-init", init_file])
    finally:
        Path(init_file).unlink()


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
            if event.get('error_fingerprint'):
                print(f"  Fingerprint: {event.get('error_fingerprint')}")
            if event.get('log_line_start'):
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

        log_line_start = event.get('log_line_start')
        log_line_end = event.get('log_line_end')
        source_name = event.get('source_name')
        message = event.get('message')

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


def cmd_commands(args: argparse.Namespace) -> None:
    """List registered commands."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    if not commands:
        print("No commands registered.")
        print("Use 'lq register <name> <command>' to register a command.")
        return

    if args.json:
        data = {name: cmd.to_dict() for name, cmd in commands.items()}
        print(json.dumps(data, indent=2))
    else:
        print(f"{'Name':<15} {'Command':<40} Description")
        print("-" * 70)
        for name, cmd in commands.items():
            cmd_display = cmd.cmd[:37] + "..." if len(cmd.cmd) > 40 else cmd.cmd
            print(f"{name:<15} {cmd_display:<40} {cmd.description}")


def cmd_register(args: argparse.Namespace) -> None:
    """Register a new command."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    name = args.name
    cmd_str = " ".join(args.cmd)

    if name in commands and not args.force:
        print(f"Command '{name}' already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)

    commands[name] = RegisteredCommand(
        name=name,
        cmd=cmd_str,
        description=args.description or "",
        timeout=args.timeout,
        format=args.format,
    )

    save_commands(lq_dir, commands)
    print(f"Registered command '{name}': {cmd_str}")


def cmd_unregister(args: argparse.Namespace) -> None:
    """Remove a registered command."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    if args.name not in commands:
        print(f"Command '{args.name}' not found.", file=sys.stderr)
        sys.exit(1)

    del commands[args.name]
    save_commands(lq_dir, commands)
    print(f"Unregistered command '{args.name}'")


# ============================================================================
# Query and Filter Commands
# ============================================================================

def format_query_output(
    df: pd.DataFrame,
    output_format: str = "table",
    limit: int | None = None,
) -> str:
    """Format query results for output.

    Args:
        df: DataFrame with query results
        output_format: One of 'table', 'json', 'csv', 'markdown'
        limit: Max rows to output (None for all)

    Returns:
        Formatted string output
    """
    if limit is not None and limit > 0:
        df = df.head(limit)

    if output_format == "json":
        return df.to_json(orient="records", indent=2)
    elif output_format == "csv":
        return df.to_csv(index=False)
    elif output_format == "markdown":
        return df.to_markdown(index=False)
    else:  # table
        return df.to_string(index=False)


def query_source(
    source: str | Path | None,
    select: str | None = None,
    where: str | None = None,
    order: str | None = None,
    lq_dir: Path | None = None,
    log_format: str = "auto",
) -> pd.DataFrame:
    """Query a log file directly or the stored lq_events.

    Uses the LogQuery API for cleaner query building.

    Args:
        source: Path to log file(s) or None to query stored data
        select: Columns to select (comma-separated) or None for all
        where: SQL WHERE clause (without WHERE keyword)
        order: SQL ORDER BY clause (without ORDER BY keyword)
        lq_dir: Path to .lq directory (for stored data queries)
        log_format: Log format hint for duck_hunt (default: auto)

    Returns:
        DataFrame with query results
    """
    if source:
        # Query file(s) directly using duck_hunt
        source_path = Path(source)
        if not source_path.exists() and "*" not in str(source_path):
            raise FileNotFoundError(f"File not found: {source}")

        try:
            query = LogQuery.from_file(source_path, format=log_format)
        except duckdb.Error:
            print("Error: duck_hunt extension required for querying files directly.", file=sys.stderr)
            print("Run 'lq init' to install required extensions.", file=sys.stderr)
            print(f"Or import the file first: lq import {source}", file=sys.stderr)
            raise
    else:
        # Query stored data
        if lq_dir is None:
            lq_dir = ensure_initialized()
        store = LogStore(lq_dir)
        query = store.events()

    # Apply query modifiers
    if where:
        query = query.filter(where)
    if order:
        query = query.order_by(*[col.strip() for col in order.split(",")])
    if select:
        query = query.select(*[col.strip() for col in select.split(",")])

    return query.df()


def parse_filter_expression(expr: str, ignore_case: bool = False) -> str:
    """Parse a simple filter expression into SQL WHERE clause.

    Supports:
        key=value      -> key = 'value'
        key=v1,v2      -> key IN ('v1', 'v2')
        key~pattern    -> key ILIKE '%pattern%'
        key!=value     -> key != 'value'

    Args:
        expr: Filter expression like "severity=error" or "file_path~main"
        ignore_case: If True, use ILIKE for = comparisons too

    Returns:
        SQL WHERE clause fragment
    """
    # Handle ~ (LIKE/contains)
    if "~" in expr:
        key, value = expr.split("~", 1)
        return f"{key.strip()} ILIKE '%{value.strip()}%'"

    # Handle !=
    if "!=" in expr:
        key, value = expr.split("!=", 1)
        return f"{key.strip()} != '{value.strip()}'"

    # Handle = (exact match or IN for comma-separated)
    if "=" in expr:
        key, value = expr.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Check for comma-separated values (OR)
        if "," in value:
            values = [v.strip() for v in value.split(",")]
            quoted = ", ".join(f"'{v}'" for v in values)
            return f"{key} IN ({quoted})"

        # Single value
        if ignore_case:
            return f"LOWER({key}) = LOWER('{value}')"
        return f"{key} = '{value}'"

    raise ValueError(f"Invalid filter expression: {expr}. Use key=value, key~pattern, or key!=value")


def cmd_query(args: argparse.Namespace) -> None:
    """Query log files or stored events."""
    # Determine source (file or stored data)
    source = args.files[0] if args.files else None

    # Support multiple files via glob pattern
    if source and "*" in source:
        # Let DuckDB handle the glob
        pass
    elif source and not Path(source).exists():
        print(f"Error: File not found: {source}", file=sys.stderr)
        sys.exit(1)

    # Get lq_dir for stored queries
    lq_dir = None
    if not source:
        lq_dir = ensure_initialized()

    try:
        df = query_source(
            source=source,
            select=args.select,
            where=args.filter,
            order=args.order,
            lq_dir=lq_dir,
            log_format=args.log_format,
        )

        # Determine output format
        if args.json:
            output_format = "json"
        elif args.csv:
            output_format = "csv"
        elif args.markdown:
            output_format = "markdown"
        else:
            output_format = "table"

        output = format_query_output(df, output_format, args.limit)
        print(output)

    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_filter(args: argparse.Namespace) -> None:
    """Filter log files or stored events with simple syntax."""
    # Separate filter expressions from file paths
    # Expressions contain =, ~, or !=
    expressions = []
    files = []
    for arg in args.args:
        if "=" in arg or "~" in arg:
            expressions.append(arg)
        else:
            files.append(arg)

    # Determine source (file or stored data)
    source = files[0] if files else None

    if source and not Path(source).exists() and "*" not in source:
        print(f"Error: File not found: {source}", file=sys.stderr)
        sys.exit(1)

    # Parse filter expressions into SQL WHERE clause
    where_clauses = []
    for expr in expressions:
        try:
            clause = parse_filter_expression(expr, ignore_case=args.ignore_case)
            where_clauses.append(clause)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    where = " AND ".join(where_clauses) if where_clauses else None

    # Invert the filter if -v flag
    if args.invert and where:
        where = f"NOT ({where})"

    # Get lq_dir for stored queries
    lq_dir = None
    if not source:
        lq_dir = ensure_initialized()

    try:
        df = query_source(
            source=source,
            select=None,  # filter always returns all columns
            where=where,
            order=None,
            lq_dir=lq_dir,
            log_format=args.log_format,
        )

        # Count mode
        if args.count:
            print(len(df))
            return

        # Determine output format
        if args.json:
            output_format = "json"
        elif args.csv:
            output_format = "csv"
        elif args.markdown:
            output_format = "markdown"
        else:
            output_format = "table"

        output = format_query_output(df, output_format, args.limit)
        print(output)

    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="lq - Log Query CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Global flags
    parser.add_argument(
        "-F", "--log-format",
        default="auto",
        help="Log format for parsing (default: auto). Use 'lq formats' to list available formats."
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # init
    p_init = subparsers.add_parser("init", help="Initialize .lq directory")
    p_init.set_defaults(func=cmd_init)

    # run
    p_run = subparsers.add_parser("run", help="Run command and capture output")
    p_run.add_argument("command", nargs="+", help="Command to run")
    p_run.add_argument("--name", "-n", help="Source name (default: command name)")
    p_run.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_run.add_argument("--keep-raw", "-r", action="store_true", help="Keep raw output file")
    p_run.add_argument("--json", "-j", action="store_true", help="Output structured JSON result")
    p_run.add_argument("--markdown", "-m", action="store_true", help="Output markdown summary")
    p_run.add_argument("--quiet", "-q", action="store_true", help="Suppress streaming output")
    p_run.add_argument("--include-warnings", "-w", action="store_true", help="Include warnings in structured output")
    p_run.add_argument("--error-limit", type=int, default=20, help="Max errors/warnings in output (default: 20)")
    p_run.set_defaults(func=cmd_run)

    # import
    p_import = subparsers.add_parser("import", help="Import existing log file")
    p_import.add_argument("file", help="Log file to import")
    p_import.add_argument("--name", "-n", help="Source name (default: filename)")
    p_import.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_import.set_defaults(func=cmd_import)

    # capture
    p_capture = subparsers.add_parser("capture", help="Capture from stdin")
    p_capture.add_argument("--name", "-n", default="stdin", help="Source name")
    p_capture.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_capture.set_defaults(func=cmd_capture)

    # status
    p_status = subparsers.add_parser("status", help="Show status of all sources")
    p_status.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    p_status.set_defaults(func=cmd_status)

    # errors
    p_errors = subparsers.add_parser("errors", help="Show recent errors")
    p_errors.add_argument("--source", "-s", help="Filter by source")
    p_errors.add_argument("--limit", "-n", type=int, default=10, help="Max results")
    p_errors.add_argument("--compact", "-c", action="store_true", help="Compact format")
    p_errors.add_argument("--json", "-j", action="store_true", help="JSON output")
    p_errors.set_defaults(func=cmd_errors)

    # warnings
    p_warnings = subparsers.add_parser("warnings", help="Show recent warnings")
    p_warnings.add_argument("--source", "-s", help="Filter by source")
    p_warnings.add_argument("--limit", "-n", type=int, default=10, help="Max results")
    p_warnings.set_defaults(func=cmd_warnings)

    # summary
    p_summary = subparsers.add_parser("summary", help="Aggregate summary")
    p_summary.add_argument("--latest", "-l", action="store_true", help="Latest run only")
    p_summary.set_defaults(func=cmd_summary)

    # history
    p_history = subparsers.add_parser("history", help="Show run history")
    p_history.add_argument("--limit", "-n", type=int, default=20, help="Max results")
    p_history.set_defaults(func=cmd_history)

    # sql
    p_sql = subparsers.add_parser("sql", help="Run arbitrary SQL")
    p_sql.add_argument("query", nargs="+", help="SQL query")
    p_sql.set_defaults(func=cmd_sql)

    # shell
    p_shell = subparsers.add_parser("shell", help="Interactive SQL shell")
    p_shell.set_defaults(func=cmd_shell)

    # prune
    p_prune = subparsers.add_parser("prune", help="Remove old logs")
    p_prune.add_argument("--older-than", "-d", type=int, default=30, help="Days to keep")
    p_prune.add_argument("--dry-run", action="store_true", help="Show what would be removed")
    p_prune.set_defaults(func=cmd_prune)

    # event
    p_event = subparsers.add_parser("event", help="Show event details by reference")
    p_event.add_argument("ref", help="Event reference (e.g., 5:3 for run 5, event 3)")
    p_event.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_event.set_defaults(func=cmd_event)

    # context
    p_context = subparsers.add_parser("context", help="Show context lines around an event")
    p_context.add_argument("ref", help="Event reference (e.g., 5:3)")
    p_context.add_argument("--lines", "-n", type=int, default=3, help="Context lines before/after (default: 3)")
    p_context.set_defaults(func=cmd_context)

    # commands
    p_commands = subparsers.add_parser("commands", help="List registered commands")
    p_commands.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_commands.set_defaults(func=cmd_commands)

    # register
    p_register = subparsers.add_parser("register", help="Register a command")
    p_register.add_argument("name", help="Command name (e.g., 'build', 'test')")
    p_register.add_argument("cmd", nargs="+", help="Command to run")
    p_register.add_argument("--description", "-d", help="Command description")
    p_register.add_argument("--timeout", "-t", type=int, default=300, help="Timeout in seconds (default: 300)")
    p_register.add_argument("--format", "-f", default="auto", help="Log format hint")
    p_register.add_argument("--force", action="store_true", help="Overwrite existing command")
    p_register.set_defaults(func=cmd_register)

    # unregister
    p_unregister = subparsers.add_parser("unregister", help="Remove a registered command")
    p_unregister.add_argument("name", help="Command name to remove")
    p_unregister.set_defaults(func=cmd_unregister)

    # query (with alias 'q')
    p_query = subparsers.add_parser("query", aliases=["q"], help="Query log files or stored events")
    p_query.add_argument("files", nargs="*", help="Log file(s) to query (omit for stored data)")
    p_query.add_argument("-s", "--select", help="Columns to select (comma-separated)")
    p_query.add_argument("-f", "--filter", help="SQL WHERE clause")
    p_query.add_argument("-o", "--order", help="SQL ORDER BY clause")
    p_query.add_argument("-n", "--limit", type=int, help="Max rows to return")
    p_query.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_query.add_argument("--csv", action="store_true", help="Output as CSV")
    p_query.add_argument("--markdown", "--md", action="store_true", help="Output as Markdown table")
    p_query.set_defaults(func=cmd_query)

    # filter (with alias 'f')
    p_filter = subparsers.add_parser("filter", aliases=["f"], help="Filter log files with simple syntax")
    p_filter.add_argument("args", nargs="*", help="Filter expressions and/or file(s)")
    p_filter.add_argument("-v", "--invert", action="store_true", help="Invert match (like grep -v)")
    p_filter.add_argument("-c", "--count", action="store_true", help="Only print count of matches")
    p_filter.add_argument("-i", "--ignore-case", action="store_true", help="Case insensitive matching")
    p_filter.add_argument("-n", "--limit", type=int, help="Max rows to return")
    p_filter.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_filter.add_argument("--csv", action="store_true", help="Output as CSV")
    p_filter.add_argument("--markdown", "--md", action="store_true", help="Output as Markdown table")
    p_filter.set_defaults(func=cmd_filter)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
