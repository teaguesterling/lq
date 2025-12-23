"""
Core utilities and shared types for blq CLI commands.

This module contains data classes, configuration, and utility functions
that are shared across multiple commands.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pandas as pd
import yaml

if TYPE_CHECKING:
    from blq.query import LogStore

# ============================================================================
# Configuration
# ============================================================================

LQ_DIR = ".lq"
LOGS_DIR = "logs"
RAW_DIR = "raw"
SCHEMA_FILE = "schema.sql"
DB_FILE = "blq.duckdb"
COMMANDS_FILE = "commands.yaml"
CONFIG_FILE = "config.yaml"
GLOBAL_LQ_DIR = Path.home() / ".lq"
PROJECTS_DIR = "projects"
GLOBAL_PROJECTS_PATH = GLOBAL_LQ_DIR / PROJECTS_DIR

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
    output_stats: dict[str, int | list[str]] = field(default_factory=dict)

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
        if self.output_stats:
            data["output_stats"] = self.output_stats
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

    namespace: str | None = None  # e.g., "teaguesterling" from github.com/teaguesterling/blq-cli
    project: str | None = None  # e.g., "blq"

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
            ssh_match = re.match(r"^git@([^:]+):([^/]+)/([^/]+?)(?:\.git)?$", url)
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
            url_match = re.match(r"^(?:https?|ssh)://([^/]+)/([^/]+)/([^/]+?)(?:\.git)?$", url)
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
            path_match = re.match(r"^([^/]+)/([^/]+?)(?:\.git)?$", url)
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


# ============================================================================
# Watch Configuration
# ============================================================================


@dataclass
class WatchConfig:
    """Configuration for watch mode."""

    debounce_ms: int = 500
    include: list[str] = field(default_factory=lambda: ["src/**/*", "tests/**/*"])
    exclude: list[str] = field(
        default_factory=lambda: [
            "**/__pycache__/**",
            "**/*.pyc",
            "**/.git/**",
            ".lq/**",
            "**/*.egg-info/**",
            "**/node_modules/**",
            "**/.venv/**",
        ]
    )
    clear_screen: bool = False
    quiet: bool = False


# ============================================================================
# Unified Configuration (BlqConfig)
# ============================================================================


@dataclass
class BlqConfig:
    """Unified configuration for blq.

    This class consolidates path management, settings, and command registry
    into a single configuration object. It provides:

    - Core path management (lq_dir and derived paths)
    - Settings from config.yaml (capture_env, namespace, project)
    - Lazy-loaded command registry
    - Factory methods for finding/loading configuration

    Example usage:
        # Find and load config from current directory
        config = BlqConfig.ensure()  # Exits with error if not initialized

        # Access paths
        logs_path = config.logs_dir
        schema_path = config.schema_path

        # Access commands
        if "build" in config.commands:
            cmd = config.commands["build"]
    """

    # Core path (everything derives from this)
    lq_dir: Path

    # Settings from config.yaml
    capture_env: list[str] = field(default_factory=lambda: DEFAULT_CAPTURE_ENV.copy())
    namespace: str | None = None
    project: str | None = None

    # Lazy-loaded commands (private, access via commands property)
    _commands: dict | None = field(default=None, repr=False)

    # Hooks configuration (private, access via hooks_config property)
    _hooks_config: dict | None = field(default=None, repr=False)

    # Watch configuration (private, access via watch_config property)
    _watch_config: WatchConfig | None = field(default=None, repr=False)

    # Computed paths
    @property
    def logs_dir(self) -> Path:
        """Path to logs directory (.lq/logs)."""
        return self.lq_dir / LOGS_DIR

    @property
    def raw_dir(self) -> Path:
        """Path to raw logs directory (.lq/raw)."""
        return self.lq_dir / RAW_DIR

    @property
    def schema_path(self) -> Path:
        """Path to schema file (.lq/schema.sql)."""
        return self.lq_dir / SCHEMA_FILE

    @property
    def db_path(self) -> Path:
        """Path to database file (.lq/blq.duckdb)."""
        return self.lq_dir / DB_FILE

    @property
    def config_path(self) -> Path:
        """Path to config file (.lq/config.yaml)."""
        return self.lq_dir / CONFIG_FILE

    @property
    def commands_path(self) -> Path:
        """Path to commands file (.lq/commands.yaml)."""
        return self.lq_dir / COMMANDS_FILE

    @property
    def commands(self) -> dict:
        """Lazy-load commands from commands.yaml.

        Returns:
            Dict mapping command names to RegisteredCommand instances.
        """
        if self._commands is None:
            self._commands = _load_commands_impl(self.lq_dir)
        return self._commands

    def reload_commands(self) -> None:
        """Force reload of commands from disk."""
        self._commands = None

    @property
    def hooks_config(self) -> dict:
        """Get hooks configuration.

        Returns:
            Dict with hooks configuration, or empty dict if not configured.
        """
        if self._hooks_config is None:
            # Load from config.yaml
            if self.config_path.exists():
                with open(self.config_path) as f:
                    data = yaml.safe_load(f) or {}
                self._hooks_config = data.get("hooks", {})
            else:
                self._hooks_config = {}
        return self._hooks_config

    @property
    def watch_config(self) -> WatchConfig:
        """Get watch configuration.

        Returns:
            WatchConfig with watch settings.
        """
        if self._watch_config is None:
            # Load from config.yaml
            if self.config_path.exists():
                with open(self.config_path) as f:
                    data = yaml.safe_load(f) or {}
                watch_data = data.get("watch", {})
                self._watch_config = WatchConfig(
                    debounce_ms=watch_data.get("debounce_ms", 500),
                    include=watch_data.get("include", ["src/**/*", "tests/**/*"]),
                    exclude=watch_data.get(
                        "exclude",
                        [
                            "**/__pycache__/**",
                            "**/*.pyc",
                            "**/.git/**",
                            ".lq/**",
                            "**/*.egg-info/**",
                            "**/node_modules/**",
                            "**/.venv/**",
                        ],
                    ),
                    clear_screen=watch_data.get("clear_screen", False),
                    quiet=watch_data.get("quiet", False),
                )
            else:
                self._watch_config = WatchConfig()
        return self._watch_config

    @classmethod
    def find(cls, start_dir: Path | None = None) -> BlqConfig | None:
        """Find .lq directory in start_dir or parents and load config.

        Args:
            start_dir: Directory to start searching from (default: cwd)

        Returns:
            BlqConfig if found, None otherwise.
        """
        if start_dir is None:
            start_dir = Path.cwd()

        # Search current directory and parents
        for p in [start_dir, *list(start_dir.parents)]:
            lq_path = p / LQ_DIR
            if lq_path.exists() and lq_path.is_dir():
                return cls.load(lq_path)

        return None

    @classmethod
    def load(cls, lq_dir: Path) -> BlqConfig:
        """Load configuration from an existing .lq directory.

        Args:
            lq_dir: Path to the .lq directory

        Returns:
            BlqConfig loaded from config.yaml (or defaults)
        """
        config_path = lq_dir / CONFIG_FILE

        # Start with defaults
        capture_env = DEFAULT_CAPTURE_ENV.copy()
        namespace = None
        project = None

        # Load from config.yaml if it exists
        if config_path.exists():
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}

            # Load capture_env
            loaded_env = data.get("capture_env")
            if isinstance(loaded_env, list):
                capture_env = loaded_env

            # Load project info
            project_data = data.get("project", {})
            namespace = project_data.get("namespace")
            project = project_data.get("project")

        return cls(
            lq_dir=lq_dir,
            capture_env=capture_env,
            namespace=namespace,
            project=project,
        )

    @classmethod
    def ensure(cls, start_dir: Path | None = None) -> BlqConfig:
        """Find configuration or exit with error.

        This is a convenience method for CLI commands that require
        an initialized project.

        Args:
            start_dir: Directory to start searching from (default: cwd)

        Returns:
            BlqConfig if found

        Raises:
            SystemExit: If .lq directory not found
        """
        config = cls.find(start_dir)
        if config is None:
            print("Error: .lq not initialized. Run 'blq init' first.", file=sys.stderr)
            sys.exit(1)
        return config

    def save(self) -> None:
        """Save configuration to config.yaml."""
        data: dict[str, Any] = {"capture_env": self.capture_env}

        # Include project info if present
        if self.namespace or self.project:
            data["project"] = {}
            if self.namespace:
                data["project"]["namespace"] = self.namespace
            if self.project:
                data["project"]["project"] = self.project

        # Include hooks config if present
        if self._hooks_config:
            data["hooks"] = self._hooks_config

        with open(self.config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    def save_commands(self) -> None:
        """Save commands to commands.yaml."""
        if self._commands is not None:
            _save_commands_impl(self.lq_dir, self._commands)


# ============================================================================
# Command Registry
# ============================================================================

# Command name patterns mapped to format hints
# Keys are substrings/patterns to match in the command, values are format names
COMMAND_FORMAT_HINTS: dict[str, str] = {
    # Python tools
    "pytest": "pytest_text",
    "python -m pytest": "pytest_text",
    "mypy": "mypy_text",
    "ruff": "generic_lint",
    "flake8": "flake8_text",
    "pylint": "pylint_text",
    "black": "black_text",
    "autopep8": "autopep8_text",
    "yapf": "yapf_text",
    "bandit": "bandit_json",
    # Rust tools
    "cargo test": "cargo_test_json",
    "cargo build": "cargo_build",
    "cargo clippy": "clippy_json",
    # JavaScript/TypeScript
    "npm test": "mocha_chai_text",
    "yarn test": "mocha_chai_text",
    "jest": "mocha_chai_text",
    "mocha": "mocha_chai_text",
    "eslint": "eslint_json",
    # Go
    "go test": "gotest_json",
    # Build systems
    "make": "make_error",
    "cmake": "cmake_build",
    "bazel": "bazel_build",
    "gradle": "gradle_build",
    "mvn": "maven_build",
    "maven": "maven_build",
    "msbuild": "msbuild",
    # Other linters
    "shellcheck": "shellcheck_json",
    "hadolint": "hadolint_json",
    "yamllint": "yamllint_json",
    "sqlfluff": "sqlfluff_json",
    "terraform": "terraform_text",
    "tflint": "tflint_json",
    "tfsec": "tfsec_json",
    # CI/tools
    "gh ": "github_cli",
    "ansible": "ansible_text",
    "docker": "node_build",  # Docker build output is similar
    # Ruby
    "rspec": "rspec_text",
    "rubocop": "rubocop_json",
    # Other
    "trivy": "trivy_json",
    "valgrind": "valgrind",
    "gdb": "gdb_lldb",
    "lldb": "gdb_lldb",
}


def detect_format_from_command(cmd: str) -> str:
    """Detect the best format hint based on the command string.

    Analyzes the command to identify known tools and returns the
    appropriate format for parsing their output.

    Args:
        cmd: The command string to analyze

    Returns:
        Format name if a known tool is detected, "auto" otherwise

    Examples:
        >>> detect_format_from_command("python -m pytest tests/")
        'pytest_text'
        >>> detect_format_from_command("mypy src/")
        'mypy_text'
        >>> detect_format_from_command("unknown-tool")
        'auto'
    """
    cmd_lower = cmd.lower()

    # Check for specific patterns (longer patterns first for specificity)
    # Sort by length descending to match more specific patterns first
    for pattern in sorted(COMMAND_FORMAT_HINTS.keys(), key=len, reverse=True):
        if pattern in cmd_lower:
            return COMMAND_FORMAT_HINTS[pattern]

    return "auto"


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


@dataclass
class CommandPlaceholder:
    """A placeholder in a command template.

    Placeholders can be:
    - {name} - keyword-only, required
    - {name=default} - keyword-only, optional
    - {name:} - positional-able, required
    - {name:=default} - positional-able, optional
    """

    name: str
    default: str | None  # None = required
    positional: bool  # Can be filled positionally


# Regex to parse placeholders from command template
# Matches: {name}, {name=default}, {name:}, {name:=default}
_PLACEHOLDER_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(:=?([^}]*)?|=([^}]*))?\}")


def parse_placeholders(template: str) -> list[CommandPlaceholder]:
    """Parse placeholders from a command template.

    Args:
        template: Command template string with placeholders

    Returns:
        List of CommandPlaceholder in template order
    """
    placeholders = []
    for match in _PLACEHOLDER_PATTERN.finditer(template):
        name = match.group(1)
        modifier = match.group(2)  # :, :=default, =default, or None

        if modifier is None:
            # {name} - keyword-only, required
            placeholders.append(CommandPlaceholder(name=name, default=None, positional=False))
        elif modifier == ":":
            # {name:} - positional-able, required
            placeholders.append(CommandPlaceholder(name=name, default=None, positional=True))
        elif modifier.startswith(":="):
            # {name:=default} - positional-able, optional
            default = match.group(3) if match.group(3) is not None else ""
            placeholders.append(CommandPlaceholder(name=name, default=default, positional=True))
        elif modifier.startswith("="):
            # {name=default} - keyword-only, optional
            default = match.group(4) if match.group(4) is not None else ""
            placeholders.append(CommandPlaceholder(name=name, default=default, positional=False))

    return placeholders


def expand_command(
    template: str,
    named_args: dict[str, str],
    positional_args: list[str],
    extra_args: list[str] | None = None,
) -> str:
    """Expand a command template with provided arguments.

    Args:
        template: Command template with placeholders
        named_args: Arguments provided as key=value
        positional_args: Arguments provided positionally
        extra_args: Extra arguments to append (passthrough)

    Returns:
        Expanded command string

    Raises:
        ValueError: If required argument is missing
    """
    placeholders = parse_placeholders(template)

    # Build map of placeholder values
    values: dict[str, str] = {}

    # First, fill from named args
    for name, value in named_args.items():
        # Check if this is a valid placeholder name
        placeholder_names = {p.name for p in placeholders}
        if name not in placeholder_names:
            valid_args = ", ".join(sorted(placeholder_names))
            raise ValueError(f"Unknown argument '{name}'. Valid arguments: {valid_args}")
        values[name] = value

    # Second, fill positional-able placeholders from positional args
    positional_placeholders = [p for p in placeholders if p.positional]
    positional_idx = 0
    for placeholder in positional_placeholders:
        if placeholder.name in values:
            # Already filled by named arg
            continue
        if positional_idx < len(positional_args):
            values[placeholder.name] = positional_args[positional_idx]
            positional_idx += 1

    # Remaining positional args become extra args
    remaining_positional = positional_args[positional_idx:]

    # Third, apply defaults and check required
    for placeholder in placeholders:
        if placeholder.name not in values:
            if placeholder.default is not None:
                values[placeholder.name] = placeholder.default
            else:
                raise ValueError(f"Missing required argument '{placeholder.name}'")

    # Substitute placeholders in template
    result = template
    for match in _PLACEHOLDER_PATTERN.finditer(template):
        name = match.group(1)
        result = result.replace(match.group(0), values[name], 1)

    # Append extra args
    all_extra = remaining_positional + (extra_args or [])
    if all_extra:
        result = result + " " + " ".join(all_extra)

    return result


def format_command_help(cmd: RegisteredCommand) -> str:
    """Format help text for a registered command.

    Args:
        cmd: The registered command

    Returns:
        Formatted help string
    """
    placeholders = parse_placeholders(cmd.cmd)
    lines = [f"{cmd.name}: {cmd.cmd}"]

    if cmd.description:
        lines.append(f"  {cmd.description}")

    if placeholders:
        lines.append("")
        lines.append("Arguments:")
        for p in placeholders:
            mode = "positional or keyword" if p.positional else "keyword-only"
            if p.default is not None:
                lines.append(f"  {p.name:<16} {mode}, default: {p.default}")
            else:
                lines.append(f"  {p.name:<16} {mode}, required")

    return "\n".join(lines)


def _load_commands_impl(lq_dir: Path) -> dict[str, RegisteredCommand]:
    """Internal implementation of load_commands."""
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


def _save_commands_impl(lq_dir: Path, commands: dict[str, RegisteredCommand]) -> None:
    """Internal implementation of save_commands."""
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
        # Use blq.duckdb if it exists (has schema pre-loaded)
        if lq_dir is not None and load_schema:
            db_path = lq_dir / DB_FILE
            if db_path.exists():
                conn = duckdb.connect(str(db_path))
                # Override blq_base_path with actual absolute path
                logs_path = (lq_dir / LOGS_DIR).resolve()
                conn.execute(f"CREATE OR REPLACE MACRO blq_base_path() AS '{logs_path}'")
                # Handle duck_hunt loading
                try:
                    conn.execute("LOAD duck_hunt")
                    cls._duck_hunt_available = True
                except duckdb.Error:
                    if install_duck_hunt:
                        cls.install_duck_hunt(conn)
                    elif require_duck_hunt:
                        raise duckdb.Error(
                            "duck_hunt extension required but not available. "
                            "Run 'blq init' to install required extensions."
                        )
                return conn

        # Fall back to in-memory connection
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
                    "Run 'blq init' to install required extensions."
                )

        # Load schema if requested and lq_dir provided
        if load_schema and lq_dir is not None:
            cls._load_schema(conn, lq_dir)

        return conn

    @classmethod
    def _load_schema(cls, conn: duckdb.DuckDBPyConnection, lq_dir: Path) -> None:
        """Load schema into connection."""
        # Set up absolute path for blq_base_path before loading schema
        logs_path = (lq_dir / LOGS_DIR).resolve()
        conn.execute(f"CREATE OR REPLACE MACRO blq_base_path() AS '{logs_path}'")

        # Load schema (which will use our blq_base_path)
        schema_path = lq_dir / SCHEMA_FILE
        if schema_path.exists():
            schema_sql = schema_path.read_text()
            # Execute each statement separately
            for stmt in schema_sql.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                # Skip the blq_base_path definition since we already set it with absolute path
                if (
                    "blq_base_path()" in stmt
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
        lq_dir = BlqConfig.ensure().lq_dir
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
    # Lazy import to avoid circular imports
    from blq.query import LogStore

    data_root, is_raw = get_data_root(args)

    if is_raw and data_root is not None:
        # Raw parquet directory - use LogStore.from_parquet_root()
        return LogStore.from_parquet_root(data_root)
    else:
        # Standard .lq directory
        config = BlqConfig.ensure()
        return LogStore(config.lq_dir)


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
    # Session (for watch mode)
    ("session_id", "VARCHAR"),
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
    ("fingerprint", "VARCHAR"),
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

    # Apply projection and write to parquet with zstd compression
    # zstd level 3 provides ~15% better compression than snappy with minimal overhead
    typed_rel = rel.project(", ".join(projections))
    conn.register("_write_temp", typed_rel)
    conn.execute(f"""
        COPY _write_temp TO '{filepath}'
        (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL 3)
    """)
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
            "SELECT * FROM parse_duck_hunt_log($1, $2)", [content, format_hint]
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
    "GITHUB_ACTIONS": (
        "github",
        [
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
        ],
    ),
    "GITLAB_CI": (
        "gitlab",
        [
            "CI_JOB_ID",
            "CI_PIPELINE_ID",
            "CI_COMMIT_SHA",
            "CI_COMMIT_REF_NAME",
            "CI_PROJECT_PATH",
            "CI_MERGE_REQUEST_IID",
            "GITLAB_USER_LOGIN",
        ],
    ),
    "JENKINS_URL": (
        "jenkins",
        [
            "BUILD_NUMBER",
            "BUILD_ID",
            "JOB_NAME",
            "BUILD_URL",
            "GIT_COMMIT",
            "GIT_BRANCH",
            "CHANGE_ID",
        ],
    ),
    "CIRCLECI": (
        "circleci",
        [
            "CIRCLE_BUILD_NUM",
            "CIRCLE_WORKFLOW_ID",
            "CIRCLE_JOB",
            "CIRCLE_SHA1",
            "CIRCLE_BRANCH",
            "CIRCLE_PR_NUMBER",
            "CIRCLE_PROJECT_REPONAME",
        ],
    ),
    "TRAVIS": (
        "travis",
        [
            "TRAVIS_BUILD_ID",
            "TRAVIS_BUILD_NUMBER",
            "TRAVIS_JOB_ID",
            "TRAVIS_COMMIT",
            "TRAVIS_BRANCH",
            "TRAVIS_PULL_REQUEST",
            "TRAVIS_REPO_SLUG",
        ],
    ),
    "BUILDKITE": (
        "buildkite",
        [
            "BUILDKITE_BUILD_ID",
            "BUILDKITE_BUILD_NUMBER",
            "BUILDKITE_JOB_ID",
            "BUILDKITE_COMMIT",
            "BUILDKITE_BRANCH",
            "BUILDKITE_PULL_REQUEST",
            "BUILDKITE_PIPELINE_SLUG",
        ],
    ),
    "AZURE_PIPELINES": (
        "azure",
        [
            "BUILD_BUILDID",
            "BUILD_BUILDNUMBER",
            "BUILD_SOURCEVERSION",
            "BUILD_SOURCEBRANCH",
            "SYSTEM_PULLREQUEST_PULLREQUESTID",
            "BUILD_REPOSITORY_NAME",
        ],
    ),
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
                            short_key = short_key[len(prefix) :]
                            break
                    ci_info[short_key.lower()] = value
            return ci_info

    # Check generic CI env var
    if os.environ.get("CI"):
        return {"provider": "unknown", "ci": "true"}

    return None
