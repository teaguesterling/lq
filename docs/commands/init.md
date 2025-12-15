# init - Initialize Project

The `blq init` command initializes a `.lq` directory in your project for storing logs and configuration.

## Basic Usage

```bash
blq init                    # Basic initialization
blq init --mcp              # Also create .mcp.json for AI agents
blq init --detect --yes     # Auto-detect and register build commands
```

## Options

| Option | Short | Description |
|--------|-------|-------------|
| `--mcp` | `-m` | Create `.mcp.json` for MCP server discovery |
| `--project NAME` | `-p` | Override auto-detected project name |
| `--namespace NAME` | `-n` | Override auto-detected namespace |
| `--detect` | `-d` | Auto-detect and register build/test commands |
| `--detect-mode MODE` | | Detection mode: `none`, `simple`, `inspect`, `auto` |
| `--yes` | `-y` | Non-interactive mode (auto-confirm detected commands) |
| `--force` | `-f` | Reinitialize config files without deleting data |

## Directory Structure

After initialization, your project will have:

```
.lq/
├── logs/           # Hive-partitioned parquet files
├── raw/            # Optional raw log files (--keep-raw)
├── schema.sql      # SQL schema and macros
├── config.yaml     # Project configuration
└── commands.yaml   # Registered commands
```

## Project Identification

blq automatically detects your project identity from git:

```yaml
# .lq/config.yaml
project:
  namespace: github__username    # From git remote owner
  project: myproject             # From git remote repo name
```

Override with flags:
```bash
blq init --project myapp --namespace myorg
```

## Command Auto-Detection

### Detection Modes

| Mode | Description |
|------|-------------|
| `auto` | Use `inspect` if CI files exist, otherwise `simple` (default) |
| `simple` | Detect from build system files (Makefile, package.json, etc.) |
| `inspect` | Parse CI workflows and Makefiles for actual commands |
| `none` | Skip command detection |

### Simple Mode

Detects commands based on build system files:

| File | Commands Detected |
|------|-------------------|
| `Makefile` | build, test, clean |
| `package.json` | build, test, lint (if scripts exist) |
| `yarn.lock` | build, test, lint (yarn) |
| `pyproject.toml` | test (pytest), lint (ruff) |
| `Cargo.toml` | build, test |
| `go.mod` | build, test |
| `CMakeLists.txt` | build, test |
| `build.gradle` | build, test, clean |
| `pom.xml` | build, test, clean |
| `Dockerfile` | docker-build |
| `docker-compose.yml` | docker-up, docker-build |

### Inspect Mode

Parses CI configuration files to extract actual commands:

```bash
blq init --detect --detect-mode inspect --yes
```

**GitHub Actions:** Parses `.github/workflows/*.yml` files, extracts job commands with slugs like `github-ci-build`, `github-ci-test`.

**Makefiles:** Extracts targets with slugs like `make-build`, `make-test`.

Features:
- Skips setup commands (`pip install`, `npm install`, etc.)
- Detects and skips workflows that use `blq` (avoids circular references)
- Generates unique CLI-friendly slugs

Example output:
```
Detected 5 command(s):
  github-ci-lint: ruff check .
  github-ci-test: pytest --cov=blq
  github-ci-build: python -m build
  test: pytest
  lint: ruff check .
```

## MCP Configuration

With `--mcp`, creates `.mcp.json` for AI agent integration:

```json
{
  "mcpServers": {
    "blq": {
      "command": "blq",
      "args": ["serve"]
    }
  }
}
```

## Reinitializing

Use `--force` to update config files without losing data:

```bash
blq init --force              # Update schema.sql, config.yaml
blq init --force --detect     # Also re-detect commands
```

This is useful when:
- Upgrading blq to get new schema features
- Resetting configuration after manual edits
- Re-running command detection

## Examples

```bash
# Basic init for a new project
blq init

# Full setup with AI agent support and auto-detected commands
blq init --mcp --detect --yes

# Reinitialize with inspect mode for CI-based commands
blq init --force --detect --detect-mode inspect --yes

# Custom project identification
blq init --project myapp --namespace mycompany
```
