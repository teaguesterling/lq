"""
Core utilities and shared types for lq CLI commands.

This module contains data classes, configuration, and utility functions
that are shared across multiple commands.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml

from lq.query import LogStore

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


def get_data_root(args) -> tuple[Path | None, bool]:
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


def get_store_for_args(args) -> LogStore:
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
# Execution Context Capture
# ============================================================================


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
