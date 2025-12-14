"""
Initialization command for lq CLI.

Handles project initialization, extension installation, and command detection.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from lq.commands.core import (
    CONFIG_FILE,
    LQ_DIR,
    LOGS_DIR,
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
        import sys
        print("  duck_hunt  - Installation failed (some features unavailable)", file=sys.stderr)
        print("             Run manually: INSTALL duck_hunt FROM community", file=sys.stderr)


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


def cmd_init(args: argparse.Namespace) -> None:
    """Initialize .lq directory and install required extensions."""
    from importlib import resources
    
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
        import sys
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
