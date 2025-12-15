"""
Initialization command for blq CLI.

Handles project initialization, extension installation, and command detection.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from importlib import resources
from pathlib import Path

import yaml

from blq.commands.core import (
    COMMANDS_FILE,
    LOGS_DIR,
    LQ_DIR,
    RAW_DIR,
    SCHEMA_FILE,
    ConnectionFactory,
    LqConfig,
    RegisteredCommand,
    detect_project_info,
    load_commands,
    save_commands,
    save_config,
)

# Detection mode constants
DETECT_NONE = "none"
DETECT_SIMPLE = "simple"
DETECT_INSPECT = "inspect"
DETECT_AUTO = "auto"

MCP_CONFIG_FILE = ".mcp.json"

MCP_CONFIG_TEMPLATE = """{
  "mcpServers": {
    "blq": {
      "command": "blq",
      "args": ["serve"]
    }
  }
}
"""

# Build system detection rules
# Each entry: (file_to_check, [(command_name, command, description), ...])
BUILD_SYSTEM_DETECTORS: list[tuple[str, list[tuple[str, str, str]]]] = [
    (
        "Makefile",
        [
            ("build", "make", "Build the project"),
            ("test", "make test", "Run tests"),
            ("clean", "make clean", "Clean build artifacts"),
        ],
    ),
    # Yarn takes precedence over npm if yarn.lock exists
    (
        "yarn.lock",
        [
            ("build", "yarn build", "Build the project"),
            ("test", "yarn test", "Run tests"),
            ("lint", "yarn lint", "Run linter"),
        ],
    ),
    (
        "package.json",
        [
            ("build", "npm run build", "Build the project"),
            ("test", "npm test", "Run tests"),
            ("lint", "npm run lint", "Run linter"),
        ],
    ),
    (
        "pyproject.toml",
        [
            ("test", "pytest", "Run tests"),
            ("lint", "ruff check .", "Run linter"),
        ],
    ),
    (
        "Cargo.toml",
        [
            ("build", "cargo build", "Build the project"),
            ("test", "cargo test", "Run tests"),
        ],
    ),
    (
        "go.mod",
        [
            ("build", "go build ./...", "Build the project"),
            ("test", "go test ./...", "Run tests"),
        ],
    ),
    (
        "CMakeLists.txt",
        [
            ("build", "cmake --build .", "Build the project"),
            ("test", "ctest", "Run tests"),
        ],
    ),
    # Autotools
    (
        "configure",
        [
            ("configure", "./configure", "Configure the build"),
        ],
    ),
    (
        "configure.ac",
        [
            ("autoreconf", "autoreconf -i", "Generate configure script"),
        ],
    ),
    # Java build systems
    (
        "build.gradle",
        [
            ("build", "./gradlew build", "Build the project"),
            ("test", "./gradlew test", "Run tests"),
            ("clean", "./gradlew clean", "Clean build artifacts"),
        ],
    ),
    (
        "build.gradle.kts",
        [
            ("build", "./gradlew build", "Build the project"),
            ("test", "./gradlew test", "Run tests"),
            ("clean", "./gradlew clean", "Clean build artifacts"),
        ],
    ),
    (
        "pom.xml",
        [
            ("build", "mvn package", "Build the project"),
            ("test", "mvn test", "Run tests"),
            ("clean", "mvn clean", "Clean build artifacts"),
        ],
    ),
    # Docker
    (
        "Dockerfile",
        [
            ("docker-build", "docker build -t app .", "Build Docker image"),
        ],
    ),
    (
        "docker-compose.yml",
        [
            ("docker-up", "docker-compose up", "Start Docker services"),
            ("docker-build", "docker-compose build", "Build Docker services"),
        ],
    ),
    (
        "docker-compose.yaml",
        [
            ("docker-up", "docker-compose up", "Start Docker services"),
            ("docker-build", "docker-compose build", "Build Docker services"),
        ],
    ),
    (
        "compose.yml",
        [
            ("docker-up", "docker compose up", "Start Docker services"),
            ("docker-build", "docker compose build", "Build Docker services"),
        ],
    ),
    (
        "compose.yaml",
        [
            ("docker-up", "docker compose up", "Start Docker services"),
            ("docker-build", "docker compose build", "Build Docker services"),
        ],
    ),
]


def _to_slug(name: str, prefix: str = "") -> str:
    """Convert a name to a CLI-friendly slug.

    Examples:
        "Build package" -> "build-package"
        "Run tests" with prefix "github" -> "github-run-tests"
    """
    # Convert to lowercase and replace spaces/underscores with hyphens
    slug = re.sub(r"[\s_]+", "-", name.lower())
    # Remove non-alphanumeric characters except hyphens
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    # Collapse multiple hyphens
    slug = re.sub(r"-+", "-", slug)
    # Strip leading/trailing hyphens
    slug = slug.strip("-")

    if prefix:
        return f"{prefix}-{slug}"
    return slug


def _contains_lq_reference(text: str) -> bool:
    """Check if text contains a reference to lq as a command (not just mentioned).

    We want to detect:
    - "blq run ..." - lq used as a command
    - "| blq" - blq in a pipeline
    - "./blq" - blq as executable

    But NOT:
    - "pip install blq-cli" or "pip install -e .[dev]" - installing blq-cli
    - Comments mentioning blq
    """
    # Skip lines that are just installing packages
    lines = text.split("\n")
    for line in lines:
        line = line.strip()
        # Skip install commands and comments
        if line.startswith("#") or "pip install" in line or "npm install" in line:
            continue
        # Check if lq is used as a command on this line
        # Patterns: starts with "lq ", "| lq", "./lq"
        if re.search(r"(^|\|)\s*lq\s", line) or re.search(r"\./lq\b", line):
            return True
    return False


def _is_setup_command(cmd: str) -> bool:
    """Check if a command is a setup/install command that should be skipped."""
    setup_patterns = [
        r"^\s*pip\s+install",
        r"^\s*python\s+-m\s+pip\s+install",
        r"^\s*npm\s+install",
        r"^\s*yarn\s+install",
        r"^\s*apt-get\s+install",
        r"^\s*brew\s+install",
        r"^\s*conda\s+install",
    ]
    for pattern in setup_patterns:
        if re.search(pattern, cmd, re.IGNORECASE):
            return True
    return False


def _extract_primary_command(run_commands: list[str]) -> str | None:
    """Extract the primary (non-setup) command from a list of run commands."""
    # Filter out setup commands and find the actual work
    primary_commands = []
    for cmd in run_commands:
        # Handle multiline commands - check each line
        lines = [line.strip() for line in cmd.split("\n") if line.strip()]
        for line in lines:
            if not _is_setup_command(line) and not line.startswith("#"):
                primary_commands.append(line)

    if not primary_commands:
        return None

    # Return the first primary command
    return primary_commands[0]


def _parse_github_workflows(cwd: Path) -> list[tuple[str, str, str, str]]:
    """Parse GitHub Actions workflows to extract jobs.

    Returns list of (slug, command, description, source_file) tuples.
    """
    workflows_dir = cwd / ".github" / "workflows"
    if not workflows_dir.exists():
        return []

    detected: list[tuple[str, str, str, str]] = []

    # Check both .yml and .yaml extensions
    workflow_files = list(workflows_dir.glob("*.yml")) + list(workflows_dir.glob("*.yaml"))

    for workflow_file in workflow_files:
        try:
            content = workflow_file.read_text()

            # Check if workflow references lq as a command
            if _contains_lq_reference(content):
                print(f"  Note: Skipping {workflow_file.name} (uses lq)")
                continue

            data = yaml.safe_load(content)
            if not data or "jobs" not in data:
                continue

            workflow_name = data.get("name", workflow_file.stem)

            for job_name, job_config in data.get("jobs", {}).items():
                if not isinstance(job_config, dict):
                    continue

                # Extract steps to build the command
                steps = job_config.get("steps", [])
                run_commands = []

                for step in steps:
                    if not isinstance(step, dict):
                        continue

                    # Skip checkout and setup steps (uses: actions/...)
                    if "uses" in step:
                        continue

                    run_cmd = step.get("run")
                    if run_cmd:
                        # Skip if this step references lq
                        if _contains_lq_reference(run_cmd):
                            continue
                        run_cmd = run_cmd.strip()
                        if run_cmd:
                            run_commands.append(run_cmd)

                # Extract the primary command (skip setup commands)
                cmd = _extract_primary_command(run_commands)
                if cmd:
                    # Include workflow file stem in slug for uniqueness
                    # e.g., "github-ci-build" instead of just "github-build"
                    workflow_stem = workflow_file.stem  # "ci", "docs", "publish"
                    slug = _to_slug(f"{workflow_stem}-{job_name}", "github")
                    desc = f"GitHub Actions: {workflow_name} / {job_name}"
                    detected.append((slug, cmd, desc, str(workflow_file.relative_to(cwd))))

        except Exception as e:
            print(f"  Warning: Could not parse {workflow_file.name}: {e}", file=sys.stderr)

    return detected


def _parse_makefile_targets(cwd: Path) -> list[tuple[str, str, str, str]]:
    """Parse Makefile to extract targets.

    Returns list of (slug, command, description, source_file) tuples.
    """
    makefile = cwd / "Makefile"
    if not makefile.exists():
        return []

    detected: list[tuple[str, str, str, str]] = []

    try:
        content = makefile.read_text()

        # Check if Makefile references lq
        if _contains_lq_reference(content):
            print("  Note: Makefile references lq, checking individual targets")

        # Find targets: lines starting with word followed by colon (not indented)
        # Pattern: target: [dependencies]
        target_pattern = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_-]*)\s*:", re.MULTILINE)

        # Skip common internal/phony targets
        skip_targets = {
            "all",
            ".PHONY",
            ".DEFAULT",
            ".SUFFIXES",
            ".PRECIOUS",
            ".INTERMEDIATE",
            ".SECONDARY",
            ".DELETE_ON_ERROR",
        }

        for match in target_pattern.finditer(content):
            target = match.group(1)

            if target in skip_targets or target.startswith("."):
                continue

            # Find the recipe (indented lines after target)
            target_pos = match.end()
            recipe_lines = []

            # Look for recipe lines (tab-indented)
            remaining = content[target_pos:]
            for line in remaining.split("\n"):
                if line.startswith("\t"):
                    recipe_lines.append(line[1:].strip())
                elif line.strip() and not line.startswith("#"):
                    # Non-recipe line (next target or variable)
                    break

            if recipe_lines:
                # Check if recipe references lq
                recipe_text = "\n".join(recipe_lines)
                if _contains_lq_reference(recipe_text):
                    continue

            # Generate command as "make <target>"
            cmd = f"make {target}"
            slug = _to_slug(target, "make")
            desc = f"Makefile target: {target}"
            detected.append((slug, cmd, desc, "Makefile"))

    except Exception as e:
        print(f"  Warning: Could not parse Makefile: {e}", file=sys.stderr)

    return detected


def _detect_commands_simple(cwd: Path) -> list[tuple[str, str, str]]:
    """Simple detection based on build system files.

    Returns list of (name, command, description) tuples.
    """
    detected: list[tuple[str, str, str]] = []
    seen_names: set[str] = set()

    for build_file, commands in BUILD_SYSTEM_DETECTORS:
        if (cwd / build_file).exists():
            for name, cmd, desc in commands:
                if name not in seen_names:
                    if build_file in ("package.json", "yarn.lock"):
                        if not _package_json_has_script(cwd / "package.json", name):
                            continue
                    detected.append((name, cmd, desc))
                    seen_names.add(name)

    return detected


def _detect_commands_inspect(cwd: Path) -> list[tuple[str, str, str]]:
    """Inspect mode: parse CI files and Makefiles for actual commands.

    Returns list of (name, command, description) tuples.
    """
    detected: list[tuple[str, str, str]] = []
    seen_slugs: set[str] = set()

    # Parse GitHub Actions workflows
    github_commands = _parse_github_workflows(cwd)
    for slug, cmd, desc, source in github_commands:
        if slug not in seen_slugs:
            detected.append((slug, cmd, desc))
            seen_slugs.add(slug)

    # Parse Makefile targets
    make_commands = _parse_makefile_targets(cwd)
    for slug, cmd, desc, source in make_commands:
        if slug not in seen_slugs:
            detected.append((slug, cmd, desc))
            seen_slugs.add(slug)

    # Also include simple detection for non-CI build systems
    simple_commands = _detect_commands_simple(cwd)
    for name, cmd, desc in simple_commands:
        if name not in seen_slugs:
            detected.append((name, cmd, desc))
            seen_slugs.add(name)

    return detected


def _write_mcp_config(path: Path) -> None:
    """Write MCP configuration file."""
    path.write_text(MCP_CONFIG_TEMPLATE)
    print(f"  {path.name}   - MCP server configuration")


def _install_extensions() -> None:
    """Install required DuckDB extensions."""
    import duckdb

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


def _detect_commands(mode: str = DETECT_AUTO) -> list[tuple[str, str, str]]:
    """Detect available build/test commands based on project files.

    Args:
        mode: Detection mode (none, simple, inspect, auto)

    Returns list of (name, command, description) tuples.
    """
    cwd = Path.cwd()

    if mode == DETECT_NONE:
        return []

    if mode == DETECT_SIMPLE:
        return _detect_commands_simple(cwd)

    if mode == DETECT_INSPECT:
        return _detect_commands_inspect(cwd)

    # Auto mode: use inspect if CI files exist, otherwise simple
    if mode == DETECT_AUTO:
        has_ci = (cwd / ".github" / "workflows").exists() or (cwd / "Makefile").exists()
        if has_ci:
            return _detect_commands_inspect(cwd)
        return _detect_commands_simple(cwd)

    # Fallback to simple
    return _detect_commands_simple(cwd)


def _package_json_has_script(path: Path, script_name: str) -> bool:
    """Check if package.json has a specific script defined."""
    try:
        data = json.loads(path.read_text())
        scripts = data.get("scripts", {})
        # Map our command names to npm script names
        script_map = {"build": "build", "test": "test", "lint": "lint"}
        npm_script = script_map.get(script_name, script_name)
        return npm_script in scripts
    except Exception:
        return False


def _detect_and_register_commands(lq_dir: Path, auto_yes: bool, mode: str = DETECT_AUTO) -> None:
    """Detect and optionally register build/test commands.

    Args:
        lq_dir: Path to .lq directory
        auto_yes: If True, register without prompting
        mode: Detection mode (none, simple, inspect, auto)
    """
    if mode == DETECT_NONE:
        return

    detected = _detect_commands(mode)

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


def _ensure_commands_file(lq_dir: Path, verbose: bool = False) -> None:
    """Ensure commands.yaml exists, creating empty one if needed."""
    commands_path = lq_dir / COMMANDS_FILE
    if not commands_path.exists():
        commands_path.write_text("# blq registered commands\ncommands: {}\n")
        if verbose:
            print(f"  Created {COMMANDS_FILE}")


def _reinit_config_files(lq_dir: Path, args: argparse.Namespace) -> None:
    """Reinitialize configuration files (schema, config, commands)."""
    # Update schema file
    try:
        schema_content = resources.files("blq").joinpath("schema.sql").read_text()
        (lq_dir / SCHEMA_FILE).write_text(schema_content)
        print(f"  Updated {SCHEMA_FILE}")
    except Exception as e:
        print(f"  Warning: Could not update schema.sql: {e}", file=sys.stderr)

    # Update config with project info
    project_info = detect_project_info()
    namespace = getattr(args, "namespace", None) or project_info.namespace
    project = getattr(args, "project", None) or project_info.project

    config = LqConfig(namespace=namespace, project=project)
    save_config(lq_dir, config)
    print(f"  Updated config.yaml (project: {namespace}/{project})")

    # Ensure commands.yaml exists
    _ensure_commands_file(lq_dir, verbose=True)


def cmd_init(args: argparse.Namespace) -> None:
    """Initialize .lq directory and install required extensions."""
    lq_dir = Path.cwd() / LQ_DIR
    mcp_config_path = Path.cwd() / MCP_CONFIG_FILE
    create_mcp = getattr(args, "mcp", False)
    detect_commands = getattr(args, "detect", False)
    detect_mode = getattr(args, "detect_mode", DETECT_AUTO)
    auto_yes = getattr(args, "yes", False)
    force_reinit = getattr(args, "force", False)

    if lq_dir.exists():
        if force_reinit:
            print(f"Reinitializing .lq at {lq_dir}")
            _reinit_config_files(lq_dir, args)
        else:
            print(f".lq already exists at {lq_dir}")
            print("  Use --force to reinitialize config files")

        # Always ensure commands.yaml exists
        _ensure_commands_file(lq_dir)

        # Still try to install extensions if they're missing
        _install_extensions()

        # Check if user wants to add MCP config
        if create_mcp and not mcp_config_path.exists():
            _write_mcp_config(mcp_config_path)

        # Still allow command detection on existing projects
        if detect_commands:
            _detect_and_register_commands(lq_dir, auto_yes, detect_mode)
        return

    # Create directories
    (lq_dir / LOGS_DIR).mkdir(parents=True)
    (lq_dir / RAW_DIR).mkdir(parents=True)

    # Copy schema file from package
    try:
        schema_content = resources.files("blq").joinpath("schema.sql").read_text()
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

    # Create empty commands.yaml
    _ensure_commands_file(lq_dir)

    print(f"Initialized .lq at {lq_dir}")
    print("  logs/         - Hive-partitioned parquet files")
    print("  raw/          - Raw log files (optional)")
    print("  schema.sql    - SQL schema and macros")
    print("  commands.yaml - Registered commands")
    if namespace and project:
        print(f"  project       - {namespace}/{project}")

    # Install required extensions
    _install_extensions()

    # Create MCP config if requested
    if create_mcp:
        _write_mcp_config(mcp_config_path)

    # Detect and register commands if requested
    if detect_commands:
        _detect_and_register_commands(lq_dir, auto_yes, detect_mode)
