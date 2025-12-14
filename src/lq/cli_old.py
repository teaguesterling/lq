"""
lq CLI - Log Query command-line interface.

Usage:
    lq init [--mcp]                  Initialize .lq directory
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
    lq sync [destination]            Sync logs to central location
    lq serve [--transport T]         Start MCP server for AI agents

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
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml

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
    def parse(cls, ref: str) -> EventRef:
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
            f"**Duration:** {self.duration_sec:.1f}s | **Exit code:** {self.exit_code} | "
            f"**Run ID:** {self.run_id}",
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
CONFIG_FILE = "config.yaml"
GLOBAL_LQ_DIR = Path.home() / ".lq"
PROJECTS_DIR = "projects"
GLOBAL_PROJECTS_PATH = GLOBAL_LQ_DIR / PROJECTS_DIR

# Default environment variables to capture for all runs
DEFAULT_CAPTURE_ENV = [
    # Path and executables
    "PATH",
    "HOME",
    "USER",
    "SHELL",
    # Python
    "PYTHONPATH",
    "VIRTUAL_ENV",
    "CONDA_DEFAULT_ENV",
    "CONDA_PREFIX",
    # C/C++
    "CC",
    "CXX",
    "CFLAGS",
    "CXXFLAGS",
    "LDFLAGS",
    "LD_LIBRARY_PATH",
    # Build tools
    "MAKEFLAGS",
    "CMAKE_PREFIX_PATH",
    # Node.js
    "NODE_PATH",
    "NPM_CONFIG_PREFIX",
    # Rust
    "CARGO_HOME",
    "RUSTUP_HOME",
    # Go
    "GOPATH",
    "GOROOT",
    # Java
    "JAVA_HOME",
    "CLASSPATH",
    # CI/CD
    "CI",
    "GITHUB_ACTIONS",
    "GITLAB_CI",
    "JENKINS_URL",
]


# ============================================================================
# Project Configuration
# ============================================================================

@dataclass
class ProjectInfo:
    """Project identity derived from git remote."""
    namespace: str | None = None  # e.g., "teaguesterling" from github.com/teaguesterling/lq
    project: str | None = None    # e.g., "lq"

    def is_detected(self) -> bool:
        """Return True if project info was successfully detected."""
        return self.namespace is not None and self.project is not None


def _extract_provider_from_host(host: str) -> str:
    """Extract provider name from hostname.

    Args:
        host: Git host (e.g., github.com, gitlab.com, bitbucket.org)

    Returns:
        Provider name (e.g., github, gitlab, bitbucket)
    """
    # Common providers - extract short name
    host_lower = host.lower()
    if "github" in host_lower:
        return "github"
    elif "gitlab" in host_lower:
        return "gitlab"
    elif "bitbucket" in host_lower:
        return "bitbucket"
    elif "codeberg" in host_lower:
        return "codeberg"
    elif "gitea" in host_lower:
        return "gitea"
    elif "sr.ht" in host_lower or "sourcehut" in host_lower:
        return "sourcehut"
    else:
        # Use sanitized hostname for self-hosted instances
        return host.replace(".", "_").replace(":", "_")


def detect_project_info() -> ProjectInfo:
    """Detect project namespace and name from git remote or filesystem path.

    Detection order:
    1. Git remote origin URL (if available)
    2. Fallback to filesystem path (parent dirs as namespace, cwd as project)

    Git remote formats supported:
    - git@github.com:namespace/project.git
    - https://github.com/namespace/project.git
    - ssh://git@github.com/namespace/project.git

    Namespace format includes provider:
    - github__teaguesterling (for github.com/teaguesterling)
    - gitlab__myorg (for gitlab.com/myorg)

    Filesystem fallback:
    - /home/teague/Projects/myapp → namespace=local__home__teague__Projects, project=myapp

    Returns:
        ProjectInfo with namespace and project.
    """
    # Try git remote first
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            url = result.stdout.strip()

            # Try SSH format: git@host:owner/project.git
            ssh_match = re.match(r'^git@([^:]+):([^/]+)/([^/]+?)(?:\.git)?$', url)
            if ssh_match:
                host = ssh_match.group(1)
                owner = ssh_match.group(2)
                project = ssh_match.group(3)
                provider = _extract_provider_from_host(host)
                return ProjectInfo(
                    namespace=f"{provider}__{owner}",
                    project=project,
                )

            # Try HTTPS/SSH URL format: https://host/owner/project.git
            url_match = re.match(r'^(?:https?|ssh)://([^/]+)/([^/]+)/([^/]+?)(?:\.git)?$', url)
            if url_match:
                host = url_match.group(1)
                owner = url_match.group(2)
                project = url_match.group(3)
                provider = _extract_provider_from_host(host)
                return ProjectInfo(
                    namespace=f"{provider}__{owner}",
                    project=project,
                )

            # Try simple path format: owner/project (assume local/unknown provider)
            path_match = re.match(r'^([^/]+)/([^/]+?)(?:\.git)?$', url)
            if path_match:
                return ProjectInfo(
                    namespace=f"git__{path_match.group(1)}",
                    project=path_match.group(2),
                )

    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass

    # Fallback to filesystem path
    cwd = Path.cwd()
    project = cwd.name
    # Tokenize parent path: /home/teague/Projects → local__home__teague__Projects
    parent = str(cwd.parent).lstrip("/")
    namespace = f"local__{parent.replace('/', '__')}" if parent else "local"

    return ProjectInfo(namespace=namespace, project=project)


@dataclass
class LqConfig:
    """Project configuration from config.yaml."""
    capture_env: list[str] = field(default_factory=lambda: DEFAULT_CAPTURE_ENV.copy())
    namespace: str | None = None
    project: str | None = None

    @classmethod
    def load(cls, lq_dir: Path) -> LqConfig:
        """Load config from config.yaml, falling back to defaults."""
        config_path = lq_dir / CONFIG_FILE
        if not config_path.exists():
            return cls()

        with open(config_path) as f:
            data = yaml.safe_load(f) or {}

        # Merge with defaults - config can extend or replace
        capture_env = data.get("capture_env")
        if capture_env is None:
            capture_env = DEFAULT_CAPTURE_ENV.copy()
        elif not isinstance(capture_env, list):
            capture_env = DEFAULT_CAPTURE_ENV.copy()

        # Load project info
        project_data = data.get("project", {})
        namespace = project_data.get("namespace")
        project = project_data.get("project")

        return cls(capture_env=capture_env, namespace=namespace, project=project)


def save_config(lq_dir: Path, config: LqConfig) -> None:
    """Save config to config.yaml."""
    config_path = lq_dir / CONFIG_FILE
    data = {"capture_env": config.capture_env}

    # Include project info if present
    if config.namespace or config.project:
        data["project"] = {}
        if config.namespace:
            data["project"]["namespace"] = config.namespace
        if config.project:
            data["project"]["project"] = config.project

    with open(config_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


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
    capture: bool = True  # Whether to capture and parse logs (default: True)
    capture_env: list[str] = field(default_factory=list)  # Additional env vars for this command

    def to_dict(self) -> dict[str, Any]:
        d = {
            "cmd": self.cmd,
            "description": self.description,
            "timeout": self.timeout,
            "format": self.format,
        }
        if not self.capture:
            d["capture"] = False
        if self.capture_env:
            d["capture_env"] = self.capture_env
        return d


def load_commands(lq_dir: Path) -> dict[str, RegisteredCommand]:
    """Load registered commands from commands.yaml."""
    commands_path = lq_dir / COMMANDS_FILE
    if not commands_path.exists():
        return {}

    with open(commands_path) as f:
        data = yaml.safe_load(f) or {}

    commands = {}
    for name, config in data.get("commands", {}).items():
        if isinstance(config, str):
            # Simple format: name: "command"
            commands[name] = RegisteredCommand(name=name, cmd=config)
        else:
            # Full format with options
            capture_env = config.get("capture_env", [])
            if not isinstance(capture_env, list):
                capture_env = []
            commands[name] = RegisteredCommand(
                name=name,
                cmd=config.get("cmd", ""),
                description=config.get("description", ""),
                timeout=config.get("timeout", 300),
                format=config.get("format", "auto"),
                capture=config.get("capture", True),
                capture_env=capture_env,
            )
    return commands


def save_commands(lq_dir: Path, commands: dict[str, RegisteredCommand]) -> None:
    """Save registered commands to commands.yaml."""
    commands_path = lq_dir / COMMANDS_FILE
    data = {"commands": {name: cmd.to_dict() for name, cmd in commands.items()}}
    with open(commands_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


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
                if (
                    "lq_base_path()" in stmt
                    and "CREATE" in stmt.upper()
                    and "MACRO" in stmt.upper()
                ):
                    continue
                # Skip pure comment blocks
                lines = [
                    line
                    for line in stmt.split("\n")
                    if line.strip() and not line.strip().startswith("--")
                ]
                if not lines:
                    continue
                try:
                    conn.execute(stmt)
                except duckdb.Error:
                    # Ignore schema errors (e.g., views on non-existent parquet files)
                    pass


def get_connection(lq_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection with schema loaded.

    This is a convenience wrapper around ConnectionFactory.create() for
    backward compatibility.
    """
    if lq_dir is None:
        lq_dir = ensure_initialized()
    return ConnectionFactory.create(lq_dir=lq_dir, load_schema=True)


def get_data_root(args: argparse.Namespace) -> tuple[Path | None, bool]:
    """Get the data root path based on --global or --database flags.

    Args:
        args: Parsed arguments with optional 'global_' and 'database' attributes

    Returns:
        Tuple of (path, is_raw_parquet):
        - path: Path to data root, or None for local .lq
        - is_raw_parquet: True if path is a raw parquet directory (no schema.sql)
    """
    # Check for --database flag first (explicit path)
    database = getattr(args, "database", None)
    if database:
        db_path = Path(database).expanduser()
        # Raw parquet directory (no .lq structure)
        return db_path, True

    # Check for --global flag
    use_global = getattr(args, "global_", False)
    if use_global:
        return GLOBAL_PROJECTS_PATH, True

    # Default: local .lq directory
    return None, False


def get_store_for_args(args: argparse.Namespace) -> LogStore:
    """Get a LogStore appropriate for the given args.

    Handles --global and --database flags.

    Args:
        args: Parsed arguments

    Returns:
        LogStore instance configured for the appropriate data source
    """
    data_root, is_raw = get_data_root(args)

    if is_raw and data_root is not None:
        # Raw parquet directory - use LogStore.from_parquet_root()
        return LogStore.from_parquet_root(data_root)
    else:
        # Standard .lq directory
        lq_dir = ensure_initialized()
        return LogStore(lq_dir)


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

# Canonical schema columns - always written in this order for consistency
# Format: (column_name, sql_type) - sql_type used for explicit casting
PARQUET_SCHEMA = [
    # Run metadata
    ("run_id", "BIGINT"),
    ("source_name", "VARCHAR"),
    ("source_type", "VARCHAR"),
    ("command", "VARCHAR"),
    ("started_at", "VARCHAR"),
    ("completed_at", "VARCHAR"),
    ("exit_code", "BIGINT"),
    # Execution context
    ("cwd", "VARCHAR"),
    ("executable_path", "VARCHAR"),
    ("environment", "MAP(VARCHAR, VARCHAR)"),
    # System context
    ("hostname", "VARCHAR"),
    ("platform", "VARCHAR"),
    ("arch", "VARCHAR"),
    # Git context
    ("git_commit", "VARCHAR"),
    ("git_branch", "VARCHAR"),
    ("git_dirty", "BOOLEAN"),
    # CI context
    ("ci", "MAP(VARCHAR, VARCHAR)"),
    # Event identification
    ("event_id", "BIGINT"),
    ("severity", "VARCHAR"),
    # Location
    ("file_path", "VARCHAR"),
    ("line_number", "BIGINT"),
    ("column_number", "BIGINT"),
    # Content
    ("message", "VARCHAR"),
    ("raw_text", "VARCHAR"),
    # Classification
    ("tool_name", "VARCHAR"),
    ("category", "VARCHAR"),
    ("error_code", "VARCHAR"),
    ("error_fingerprint", "VARCHAR"),
    # Log position
    ("log_line_start", "BIGINT"),
    ("log_line_end", "BIGINT"),
]

# Just the column names for iteration
PARQUET_SCHEMA_COLUMNS = [col for col, _ in PARQUET_SCHEMA]


def write_run_parquet(
    events: list[dict[str, Any]],
    run_meta: dict[str, Any],
    lq_dir: Path,
) -> Path:
    """Write events to a Hive-partitioned parquet file.

    Always writes all schema columns for consistency, even if values are None.
    """
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

    # Columns that should be stored as MAP(VARCHAR, VARCHAR)
    map_columns = {"environment", "ci"}

    def dict_to_map_entries(d: dict | None) -> list | None:
        """Convert dict to list of {key, value} structs for DuckDB MAP creation."""
        if d is None:
            return None
        return [{"key": str(k), "value": str(v)} for k, v in d.items()]

    # Build enriched events with all schema columns
    enriched_events = []
    for event in events or [{}]:
        # Merge run metadata and event data (run_meta first, then event overrides)
        merged = {**run_meta, **event}
        # Build row with all columns in canonical order (None for missing)
        enriched = {}
        for col in PARQUET_SCHEMA_COLUMNS:
            val = merged.get(col)
            # Convert dict columns to list format for MAP
            if col in map_columns and isinstance(val, dict):
                val = dict_to_map_entries(val)
            enriched[col] = val
        enriched_events.append(enriched)

    # Write using DuckDB relation API with explicit type casting
    conn = duckdb.connect(":memory:")
    df = pd.DataFrame(enriched_events, columns=PARQUET_SCHEMA_COLUMNS)

    # Create relation from dataframe
    rel = conn.from_df(df)

    # Build projection expressions with explicit type casts
    # This ensures consistent schema even when values are NULL
    projections = []
    for col, sql_type in PARQUET_SCHEMA:
        if col in map_columns:
            # MAP columns need map_from_entries conversion
            projections.append(f"map_from_entries({col})::MAP(VARCHAR, VARCHAR) AS {col}")
        else:
            # Cast all other columns to their explicit types
            projections.append(f"{col}::{sql_type} AS {col}")

    # Apply projection and write to parquet
    typed_rel = rel.project(", ".join(projections))
    typed_rel.write_parquet(str(filepath))
    conn.close()

    return filepath


# ============================================================================
# Log Parsing
# ============================================================================


def parse_log_content(content: str, format_hint: str = "auto") -> list[dict[str, Any]]:
    """Parse log content using duck_hunt extension.

    All log parsing is delegated to duck_hunt. If duck_hunt is not available
    or fails to parse, returns an empty list. Parsing improvements should be
    made upstream in duck_hunt, not in lq.

    Args:
        content: Raw log content to parse
        format_hint: Format hint for duck_hunt (default: "auto")

    Returns:
        List of parsed events, or empty list if parsing unavailable
    """
    conn = duckdb.connect(":memory:")

    try:
        conn.execute("LOAD duck_hunt")
        result = conn.execute(
            "SELECT * FROM parse_duck_hunt_log($1, $2)",
            [content, format_hint]
        ).fetchall()
        columns = [desc[0] for desc in conn.description]
        events = [dict(zip(columns, row)) for row in result]
        return events
    except duckdb.Error:
        # duck_hunt not available or parsing failed - return empty list
        # Parsing improvements should be made in duck_hunt, not here
        return []
    finally:
        conn.close()


# ============================================================================
# Commands
# ============================================================================

MCP_CONFIG_FILE = ".mcp.json"

MCP_CONFIG_TEMPLATE = """{
  "mcpServers": {
    "lq": {
      "command": "lq",
      "args": ["serve"]
    }
  }
}
"""


def cmd_init(args: argparse.Namespace) -> None:
    """Initialize .lq directory and install required extensions."""
    lq_dir = Path.cwd() / LQ_DIR
    mcp_config_path = Path.cwd() / MCP_CONFIG_FILE
    create_mcp = getattr(args, "mcp", False)
    detect_commands = getattr(args, "detect", False)
    auto_yes = getattr(args, "yes", False)

    if lq_dir.exists():
        print(f".lq already exists at {lq_dir}")
        # Still try to install extensions if they're missing
        _install_extensions()
        # Check if user wants to add MCP config
        if create_mcp and not mcp_config_path.exists():
            _write_mcp_config(mcp_config_path)
        # Still allow command detection on existing projects
        if detect_commands:
            _detect_and_register_commands(lq_dir, auto_yes)
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

    # Detect project info from git remote (can be overridden)
    project_info = detect_project_info()

    # Apply overrides from command line
    namespace = getattr(args, "namespace", None) or project_info.namespace
    project = getattr(args, "project", None) or project_info.project

    config = LqConfig(
        namespace=namespace,
        project=project,
    )
    save_config(lq_dir, config)

    print(f"Initialized .lq at {lq_dir}")
    print("  logs/      - Hive-partitioned parquet files")
    print("  raw/       - Raw log files (optional)")
    print("  schema.sql - SQL schema and macros")
    if namespace and project:
        print(f"  project    - {namespace}/{project}")

    # Install required extensions
    _install_extensions()

    # Create MCP config if requested
    if create_mcp:
        _write_mcp_config(mcp_config_path)

    # Detect and register commands if requested
    if detect_commands:
        _detect_and_register_commands(lq_dir, auto_yes)


def _write_mcp_config(path: Path) -> None:
    """Write MCP configuration file."""
    path.write_text(MCP_CONFIG_TEMPLATE)
    print(f"  {path.name}   - MCP server configuration")


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


# Build system detection rules
# Each entry: (file_to_check, [(command_name, command, description), ...])
BUILD_SYSTEM_DETECTORS: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("Makefile", [
        ("build", "make", "Build the project"),
        ("test", "make test", "Run tests"),
        ("clean", "make clean", "Clean build artifacts"),
    ]),
    # Yarn takes precedence over npm if yarn.lock exists
    ("yarn.lock", [
        ("build", "yarn build", "Build the project"),
        ("test", "yarn test", "Run tests"),
        ("lint", "yarn lint", "Run linter"),
    ]),
    ("package.json", [
        ("build", "npm run build", "Build the project"),
        ("test", "npm test", "Run tests"),
        ("lint", "npm run lint", "Run linter"),
    ]),
    ("pyproject.toml", [
        ("test", "pytest", "Run tests"),
        ("lint", "ruff check .", "Run linter"),
    ]),
    ("Cargo.toml", [
        ("build", "cargo build", "Build the project"),
        ("test", "cargo test", "Run tests"),
    ]),
    ("go.mod", [
        ("build", "go build ./...", "Build the project"),
        ("test", "go test ./...", "Run tests"),
    ]),
    ("CMakeLists.txt", [
        ("build", "cmake --build .", "Build the project"),
        ("test", "ctest", "Run tests"),
    ]),
    # Autotools
    ("configure", [
        ("configure", "./configure", "Configure the build"),
    ]),
    ("configure.ac", [
        ("autoreconf", "autoreconf -i", "Generate configure script"),
    ]),
    # Java build systems
    ("build.gradle", [
        ("build", "./gradlew build", "Build the project"),
        ("test", "./gradlew test", "Run tests"),
        ("clean", "./gradlew clean", "Clean build artifacts"),
    ]),
    ("build.gradle.kts", [
        ("build", "./gradlew build", "Build the project"),
        ("test", "./gradlew test", "Run tests"),
        ("clean", "./gradlew clean", "Clean build artifacts"),
    ]),
    ("pom.xml", [
        ("build", "mvn package", "Build the project"),
        ("test", "mvn test", "Run tests"),
        ("clean", "mvn clean", "Clean build artifacts"),
    ]),
    # Docker
    ("Dockerfile", [
        ("docker-build", "docker build -t app .", "Build Docker image"),
    ]),
    ("docker-compose.yml", [
        ("docker-up", "docker-compose up", "Start Docker services"),
        ("docker-build", "docker-compose build", "Build Docker services"),
    ]),
    ("docker-compose.yaml", [
        ("docker-up", "docker-compose up", "Start Docker services"),
        ("docker-build", "docker-compose build", "Build Docker services"),
    ]),
    ("compose.yml", [
        ("docker-up", "docker compose up", "Start Docker services"),
        ("docker-build", "docker compose build", "Build Docker services"),
    ]),
    ("compose.yaml", [
        ("docker-up", "docker compose up", "Start Docker services"),
        ("docker-build", "docker compose build", "Build Docker services"),
    ]),
]


def _detect_commands() -> list[tuple[str, str, str]]:
    """Detect available build/test commands based on project files.

    Returns list of (name, command, description) tuples.
    """
    cwd = Path.cwd()
    detected: list[tuple[str, str, str]] = []
    seen_names: set[str] = set()

    for build_file, commands in BUILD_SYSTEM_DETECTORS:
        if (cwd / build_file).exists():
            for name, cmd, desc in commands:
                # Skip if we already have a command with this name
                if name not in seen_names:
                    # For package.json or yarn.lock, verify the script exists in package.json
                    if build_file in ("package.json", "yarn.lock"):
                        if not _package_json_has_script(cwd / "package.json", name):
                            continue
                    detected.append((name, cmd, desc))
                    seen_names.add(name)

    return detected


def _package_json_has_script(path: Path, script_name: str) -> bool:
    """Check if package.json has a specific script defined."""
    try:
        import json
        data = json.loads(path.read_text())
        scripts = data.get("scripts", {})
        # Map our command names to npm script names
        script_map = {"build": "build", "test": "test", "lint": "lint"}
        npm_script = script_map.get(script_name, script_name)
        return npm_script in scripts
    except Exception:
        return False


def _detect_and_register_commands(lq_dir: Path, auto_yes: bool) -> None:
    """Detect and optionally register build/test commands.

    Args:
        lq_dir: Path to .lq directory
        auto_yes: If True, register without prompting
    """
    detected = _detect_commands()

    if not detected:
        print("\n  No build systems detected.")
        return

    # Load existing commands to avoid duplicates
    existing = load_commands(lq_dir)
    new_commands = [(n, c, d) for n, c, d in detected if n not in existing]

    if not new_commands:
        print("\n  All detected commands already registered.")
        return

    print(f"\n  Detected {len(new_commands)} command(s):")
    for name, cmd, desc in new_commands:
        print(f"    {name}: {cmd}")

    if auto_yes:
        # Auto-register all
        for name, cmd, desc in new_commands:
            existing[name] = RegisteredCommand(
                name=name,
                cmd=cmd,
                description=desc,
            )
        save_commands(lq_dir, existing)
        print(f"  Registered {len(new_commands)} command(s).")
    else:
        # Prompt user
        try:
            response = input("\n  Register these commands? [Y/n] ").strip().lower()
            if response in ("", "y", "yes"):
                for name, cmd, desc in new_commands:
                    existing[name] = RegisteredCommand(
                        name=name,
                        cmd=cmd,
                        description=desc,
                    )
                save_commands(lq_dir, existing)
                print(f"  Registered {len(new_commands)} command(s).")
            else:
                print("  Skipped command registration.")
        except (EOFError, KeyboardInterrupt):
            print("\n  Skipped command registration.")


def capture_environment(env_vars: list[str]) -> dict[str, str]:
    """Capture specified environment variables.

    Args:
        env_vars: List of environment variable names to capture

    Returns:
        Dict of captured env vars (only those that exist)
    """
    captured = {}
    for var in env_vars:
        value = os.environ.get(var)
        if value is not None:
            captured[var] = value
    return captured


def find_executable(command: str) -> str | None:
    """Find the full path to the executable for a command.

    Args:
        command: Command string (may include arguments)

    Returns:
        Full path to executable, or None if not found
    """
    # Extract the first word (the executable name)
    parts = command.split()
    if not parts:
        return None

    exe_name = parts[0]

    # If it's already an absolute path, return it
    if os.path.isabs(exe_name) and os.path.exists(exe_name):
        return exe_name

    # Use shutil.which to find in PATH
    return shutil.which(exe_name)


@dataclass
class GitInfo:
    """Git repository state at time of run."""
    commit: str | None = None
    branch: str | None = None
    dirty: bool | None = None


def capture_git_info() -> GitInfo:
    """Capture current git repository state.

    Returns:
        GitInfo with commit hash, branch name, and dirty status.
        Fields are None if not in a git repo or git not available.
    """
    info = GitInfo()

    try:
        # Get current commit hash
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            info.commit = result.stdout.strip()

        # Get current branch name
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            info.branch = result.stdout.strip()

        # Check if working directory is dirty
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            info.dirty = len(result.stdout.strip()) > 0

    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        # Git not available or timed out
        pass

    return info


# CI provider detection: env var to check -> (provider name, env vars to capture)
CI_PROVIDERS = {
    "GITHUB_ACTIONS": ("github", [
        "GITHUB_RUN_ID",
        "GITHUB_RUN_NUMBER",
        "GITHUB_WORKFLOW",
        "GITHUB_JOB",
        "GITHUB_REF",
        "GITHUB_SHA",
        "GITHUB_REPOSITORY",
        "GITHUB_ACTOR",
        "GITHUB_EVENT_NAME",
        "GITHUB_PR_NUMBER",
    ]),
    "GITLAB_CI": ("gitlab", [
        "CI_JOB_ID",
        "CI_PIPELINE_ID",
        "CI_COMMIT_SHA",
        "CI_COMMIT_REF_NAME",
        "CI_PROJECT_PATH",
        "CI_MERGE_REQUEST_IID",
        "GITLAB_USER_LOGIN",
    ]),
    "JENKINS_URL": ("jenkins", [
        "BUILD_NUMBER",
        "BUILD_ID",
        "JOB_NAME",
        "BUILD_URL",
        "GIT_COMMIT",
        "GIT_BRANCH",
        "CHANGE_ID",
    ]),
    "CIRCLECI": ("circleci", [
        "CIRCLE_BUILD_NUM",
        "CIRCLE_WORKFLOW_ID",
        "CIRCLE_JOB",
        "CIRCLE_SHA1",
        "CIRCLE_BRANCH",
        "CIRCLE_PR_NUMBER",
        "CIRCLE_PROJECT_REPONAME",
    ]),
    "TRAVIS": ("travis", [
        "TRAVIS_BUILD_ID",
        "TRAVIS_BUILD_NUMBER",
        "TRAVIS_JOB_ID",
        "TRAVIS_COMMIT",
        "TRAVIS_BRANCH",
        "TRAVIS_PULL_REQUEST",
        "TRAVIS_REPO_SLUG",
    ]),
    "BUILDKITE": ("buildkite", [
        "BUILDKITE_BUILD_ID",
        "BUILDKITE_BUILD_NUMBER",
        "BUILDKITE_JOB_ID",
        "BUILDKITE_COMMIT",
        "BUILDKITE_BRANCH",
        "BUILDKITE_PULL_REQUEST",
        "BUILDKITE_PIPELINE_SLUG",
    ]),
    "AZURE_PIPELINES": ("azure", [
        "BUILD_BUILDID",
        "BUILD_BUILDNUMBER",
        "BUILD_SOURCEVERSION",
        "BUILD_SOURCEBRANCH",
        "SYSTEM_PULLREQUEST_PULLREQUESTID",
        "BUILD_REPOSITORY_NAME",
    ]),
}


def capture_ci_info() -> dict[str, str] | None:
    """Detect CI provider and capture relevant environment variables.

    Returns:
        Dict with 'provider' key and provider-specific env vars, or None if not in CI.
    """
    for detect_var, (provider_name, env_vars) in CI_PROVIDERS.items():
        if os.environ.get(detect_var):
            ci_info = {"provider": provider_name}
            for var in env_vars:
                value = os.environ.get(var)
                if value is not None:
                    # Use short key names (strip common prefixes)
                    short_key = var
                    for prefix in ["GITHUB_", "CI_", "CIRCLE_", "TRAVIS_", "BUILDKITE_", "BUILD_"]:
                        if short_key.startswith(prefix):
                            short_key = short_key[len(prefix):]
                            break
                    ci_info[short_key.lower()] = value
            return ci_info

    # Check generic CI env var
    if os.environ.get("CI"):
        return {"provider": "unknown", "ci": "true"}

    return None


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


def cmd_sql(args: argparse.Namespace) -> None:
    """Run arbitrary SQL."""
    sql = " ".join(args.query)
    try:
        store = get_store_for_args(args)
        result = store.connection.execute(sql).fetchdf()
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_shell(args: argparse.Namespace) -> None:
    """Start interactive DuckDB shell."""
    lq_dir = ensure_initialized()

    # Create init file
    init_sql = """
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
        print(f"{'Name':<15} {'Command':<40} {'Capture':<8} Description")
        print("-" * 80)
        for name, cmd in commands.items():
            cmd_display = cmd.cmd[:37] + "..." if len(cmd.cmd) > 40 else cmd.cmd
            capture_str = "yes" if cmd.capture else "no"
            print(f"{name:<15} {cmd_display:<40} {capture_str:<8} {cmd.description}")


def cmd_register(args: argparse.Namespace) -> None:
    """Register a new command."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    name = args.name
    cmd_str = " ".join(args.cmd)

    if name in commands and not args.force:
        print(f"Command '{name}' already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)

    capture = not getattr(args, "no_capture", False)
    commands[name] = RegisteredCommand(
        name=name,
        cmd=cmd_str,
        description=args.description or "",
        timeout=args.timeout,
        format=args.format,
        capture=capture,
    )

    save_commands(lq_dir, commands)
    capture_note = " (no capture)" if not capture else ""
    print(f"Registered command '{name}': {cmd_str}{capture_note}")


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
# Sync Command
# ============================================================================

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
        print("Run 'lq init --namespace X --project Y' or set in .lq/config.yaml", file=sys.stderr)
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
            print(f"Error: Target exists and is not a symlink or directory: {target}",
                  file=sys.stderr)
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
            print(
                "Error: duck_hunt extension required for querying files directly.", file=sys.stderr
            )
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

    raise ValueError(
        f"Invalid filter expression: {expr}. Use key=value, key~pattern, or key!=value"
    )


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
# MCP Server
# ============================================================================


def cmd_serve(args: argparse.Namespace) -> None:
    """Start the MCP server for AI agent integration."""
    try:
        from lq.serve import serve
    except ImportError:
        print("Error: MCP dependencies not installed.", file=sys.stderr)
        print("Install with: pip install lq[mcp]", file=sys.stderr)
        sys.exit(1)

    # Ensure we're in an initialized directory
    lq_dir = get_lq_dir()
    if lq_dir is None:
        print("Warning: No .lq directory found. Some features may not work.", file=sys.stderr)

    serve(transport=args.transport, port=args.port)


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
        "-F",
        "--log-format",
        default="auto",
        help="Log format for parsing (default: auto). Use 'lq formats' to list available formats.",
    )
    parser.add_argument(
        "-g", "--global",
        action="store_true",
        dest="global_",
        help="Query global store (~/.lq/projects/) instead of local .lq"
    )
    parser.add_argument(
        "-d", "--database",
        metavar="PATH",
        help="Query custom database path (local or remote, e.g., s3://bucket/lq/)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # init
    p_init = subparsers.add_parser("init", help="Initialize .lq directory")
    p_init.add_argument(
        "--mcp", "-m", action="store_true", help="Create .mcp.json for MCP server discovery"
    )
    p_init.add_argument(
        "--project", "-p",
        help="Project name (overrides auto-detection)"
    )
    p_init.add_argument(
        "--namespace", "-n",
        help="Project namespace (overrides auto-detection)"
    )
    p_init.add_argument(
        "--detect", "-d",
        action="store_true",
        help="Auto-detect and register build/test commands"
    )
    p_init.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Non-interactive mode (auto-confirm detected commands)"
    )
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
    p_run.add_argument(
        "--include-warnings",
        "-w",
        action="store_true",
        help="Include warnings in structured output",
    )
    p_run.add_argument(
        "--error-limit", type=int, default=20, help="Max errors/warnings in output (default: 20)"
    )
    p_run.set_defaults(func=cmd_run)
    # Capture control: runtime flags override command config
    capture_group = p_run.add_mutually_exclusive_group()
    capture_group.add_argument("--capture", "-C", action="store_true", dest="capture", default=None,
                               help="Force log capture (override command config)")
    capture_group.add_argument("--no-capture", "-N", action="store_false", dest="capture",
                               help="Skip log capture, just run command")

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
    p_context.add_argument(
        "--lines", "-n", type=int, default=3, help="Context lines before/after (default: 3)"
    )
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
    p_register.add_argument(
        "--timeout", "-t", type=int, default=300, help="Timeout in seconds (default: 300)"
    )
    p_register.add_argument("--format", "-f", default="auto", help="Log format hint")
    p_register.add_argument("--no-capture", "-N", action="store_true",
                           help="Don't capture logs by default")
    p_register.add_argument("--force", action="store_true", help="Overwrite existing command")
    p_register.set_defaults(func=cmd_register)

    # unregister
    p_unregister = subparsers.add_parser("unregister", help="Remove a registered command")
    p_unregister.add_argument("name", help="Command name to remove")
    p_unregister.set_defaults(func=cmd_unregister)

    # sync
    p_sync = subparsers.add_parser("sync", help="Sync project logs to central location")
    p_sync.add_argument("destination", nargs="?",
                        help="Destination path", default=GLOBAL_PROJECTS_PATH)
    p_sync.add_argument("--soft", "-s", action="store_true", default=True,
                        help="Create symlink (default)")
    p_sync.add_argument("--hard", "-H", action="store_true",
                        help="Copy files instead of symlink")
    p_sync.add_argument("--force", "-f", action="store_true",
                        help="Replace existing sync target")
    p_sync.add_argument("--dry-run", "-n", action="store_true",
                        help="Show what would be done without doing it")
    p_sync.add_argument("--status", action="store_true",
                        help="Show current sync status")
    p_sync.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose output")
    p_sync.set_defaults(func=cmd_sync)

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
    p_filter = subparsers.add_parser(
        "filter", aliases=["f"], help="Filter log files with simple syntax"
    )
    p_filter.add_argument("args", nargs="*", help="Filter expressions and/or file(s)")
    p_filter.add_argument("-v", "--invert", action="store_true", help="Invert match (like grep -v)")
    p_filter.add_argument("-c", "--count", action="store_true", help="Only print count of matches")
    p_filter.add_argument(
        "-i", "--ignore-case", action="store_true", help="Case insensitive matching"
    )
    p_filter.add_argument("-n", "--limit", type=int, help="Max rows to return")
    p_filter.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_filter.add_argument("--csv", action="store_true", help="Output as CSV")
    p_filter.add_argument(
        "--markdown", "--md", action="store_true", help="Output as Markdown table"
    )
    p_filter.set_defaults(func=cmd_filter)

    # serve (MCP server)
    p_serve = subparsers.add_parser("serve", help="Start MCP server for AI agent integration")
    p_serve.add_argument(
        "--transport",
        "-t",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport type (default: stdio)",
    )
    p_serve.add_argument(
        "--port", "-p", type=int, default=8080, help="Port for SSE transport (default: 8080)"
    )
    p_serve.set_defaults(func=cmd_serve)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
