# blq - Build Log Query

A CLI tool for capturing, querying, and analyzing build/test logs using [DuckDB](https://duckdb.org) and 
the [duck_hunt](https://duckdb.org/community_extensions/extensions/duck_hunt) extension. We pronouce 
`blq` like "bleak", as in we have bleak outlook on the outcome of our hunt through the logs.

## Features

- **Capture logs** from commands, files, or stdin
- **Query directly** with SQL or simple filter syntax
- **Structured output** in JSON, CSV, or Markdown for agent integration
- **Event references** for drilling into specific errors
- **Command registry** for reusable build/test commands
- **Run metadata** - captures git, environment, system, and CI context
- **MCP server** for AI agent integration
- **60+ log formats** supported via duck_hunt extension

## Installation

```bash
pip install blq-cli
```

Initialize in your project (installs duck_hunt extension):
```bash
blq init                     # Basic init
blq init --mcp               # Also create .mcp.json for AI agents
blq init --detect --yes      # Auto-detect and register build/test commands
blq init --project myapp --namespace myorg  # Override project identification
```

## Quick Start

```bash
# Query a log file directly
blq q build.log
blq q -s file_path,line_number,message build.log

# Filter with simple syntax
blq f severity=error build.log
blq f severity=error,warning file_path~main build.log

# Run and capture a command
blq run make -j8
blq run --json make test

# View recent errors
blq errors

# Drill into a specific error
blq event 1:3
blq context 1:3
```

## Commands

### Querying

| Command | Description |
|---------|-------------|
| `blq query` / `blq q` | Query log files or stored events |
| `blq filter` / `blq f` | Filter with simple key=value syntax |
| `blq sql <query>` | Run arbitrary SQL |
| `blq shell` | Interactive SQL shell |

### Capturing

| Command | Description |
|---------|-------------|
| `blq run <cmd>` | Run registered command and capture output |
| `blq exec <cmd>` | Execute ad-hoc command and capture output |
| `blq import <file>` | Import existing log file |
| `blq capture` | Capture from stdin |

### Viewing

| Command | Description |
|---------|-------------|
| `blq errors` | Show recent errors |
| `blq warnings` | Show recent warnings |
| `blq event <ref>` | Show event details (e.g., `blq event 1:3`) |
| `blq context <ref>` | Show log context around event |
| `blq status` | Show status of all sources |
| `blq history` | Show run history |

### CI Integration

| Command | Description |
|---------|-------------|
| `blq ci check` | Compare errors against baseline, exit 0/1 for CI gates |
| `blq ci comment` | Post error summary as GitHub PR comment |
| `blq report` | Generate markdown report of build/test results |
| `blq watch` | Watch for file changes and auto-run commands |

### Management

| Command | Description |
|---------|-------------|
| `blq init` | Initialize .lq directory |
| `blq register` | Register a reusable command |
| `blq unregister` | Remove a registered command |
| `blq commands` | List registered commands |
| `blq prune` | Remove old logs |
| `blq formats` | List available log formats |
| `blq completions` | Generate shell completions (bash/zsh/fish) |

## Query Examples

```bash
# Select specific columns
blq q -s file_path,line_number,severity,message build.log

# Filter with SQL WHERE clause
blq q -f "severity='error' AND file_path LIKE '%main%'" build.log

# Order and limit results
blq q -o "line_number" -n 10 build.log

# Output as JSON (great for agents)
blq q --json build.log

# Output as CSV
blq q --csv build.log

# Query stored events (no file argument)
blq q -f "severity='error'"
```

## Filter Syntax

The `blq filter` command provides grep-like simplicity:

```bash
# Exact match
blq f severity=error build.log

# Multiple values (OR)
blq f severity=error,warning build.log

# Contains (LIKE)
blq f file_path~main build.log

# Not equal
blq f severity!=info build.log

# Invert match (like grep -v)
blq f -v severity=error build.log

# Count matches
blq f -c severity=error build.log

# Case insensitive
blq f -i message~undefined build.log
```

## Structured Output for Agents

```bash
# JSON output with errors
blq run --json make

# Markdown summary
blq run --markdown make

# Quiet mode (no streaming, just results)
blq run --quiet --json make
```

Output includes event references for drill-down:
```json
{
  "run_id": 1,
  "status": "FAIL",
  "errors": [
    {
      "ref": "1:1",
      "file_path": "src/main.c",
      "line_number": 15,
      "message": "undefined variable 'foo'"
    }
  ]
}
```

## Command Registry

Register frequently-used commands:

```bash
# Auto-detect commands from build files (Makefile, package.json, etc.)
blq init --detect --yes

# Or register manually
blq register build "make -j8" --description "Build project"
blq register test "pytest -v" --timeout 600
blq register format "black ." --no-capture  # Skip log capture for fast commands

# Run by name
blq run build
blq run test

# Run without log capture (fast mode for CI/pre-commit)
blq run --no-capture format

# List registered commands
blq commands
```

**Auto-detected build systems:** Makefile, package.json (npm/yarn), pyproject.toml, Cargo.toml, go.mod, CMakeLists.txt, configure, build.gradle, pom.xml, Dockerfile, docker-compose.yml

**Format auto-detection:** When registering commands, blq automatically detects the appropriate log format based on the command (e.g., `mypy` → `mypy_text`, `pytest` → `pytest_text`).

## CI Integration

blq provides commands for CI/CD pipeline integration:

```bash
# Check for new errors vs baseline (exits 1 if new errors found)
blq ci check                          # Auto-detect baseline from main/master
blq ci check --baseline main          # Compare against specific branch
blq ci check --baseline 42            # Compare against run ID
blq ci check --fail-on-any            # Fail if any errors (no baseline)

# Post error summary as PR comment (requires GITHUB_TOKEN)
blq ci comment                        # Create new comment
blq ci comment --update               # Update existing blq comment
blq ci comment --diff --baseline main # Include diff vs baseline

# Generate markdown report
blq report                            # Report on latest run
blq report --baseline main            # Include comparison
blq report --output report.md         # Save to file
blq report --summary-only             # Summary without error details
```

### GitHub Actions Example

```yaml
- name: Run tests
  run: blq run test

- name: Check for regressions
  run: blq ci check --baseline main

- name: Post results
  if: github.event_name == 'pull_request'
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: blq ci comment --update --diff
```

## Watch Mode

Automatically run commands when files change:

```bash
blq watch build              # Watch and run 'build' on changes
blq watch test --debounce 500  # Custom debounce (ms)
blq watch lint --exclude "*.log,dist/*"  # Exclude patterns
blq watch --once build       # Run once then exit (for CI)
```

## Run Metadata

Each `blq run` automatically captures execution context:

| Field | Description |
|-------|-------------|
| `hostname` | Machine name |
| `platform` | OS (Linux, Darwin, Windows) |
| `arch` | Architecture (x86_64, arm64) |
| `git_commit` | Current commit SHA |
| `git_branch` | Current branch |
| `git_dirty` | Uncommitted changes present |
| `environment` | Captured env vars (PATH, VIRTUAL_ENV, etc.) |
| `ci` | CI provider info (auto-detected) |

Query metadata with SQL:
```bash
blq sql "SELECT hostname, git_branch, environment['VIRTUAL_ENV'] FROM blq_load_events()"
```

## MCP Server

blq includes an MCP server for AI agent integration:

```bash
blq serve                    # stdio transport (Claude Desktop)
blq serve --transport sse    # HTTP/SSE transport
```

Tools available: `run`, `exec`, `query`, `errors`, `warnings`, `event`, `context`, `status`, `history`, `diff`, `register_command`, `unregister_command`, `list_commands`

See [MCP Guide](docs/mcp.md) for details.

## Global Options

| Flag | Description |
|------|-------------|
| `-V, --version` | Show version number |
| `-F, --log-format` | Log format hint (default: auto) |

## Python API

blq provides a fluent Python API for programmatic access:

```python
from blq import LogStore, LogQuery

# Open the repository
store = LogStore.open()

# Query errors with chaining
errors = (
    store.errors()
    .filter(file_path="%main%")
    .select("file_path", "line_number", "message")
    .order_by("line_number")
    .limit(10)
    .df()
)

# Filter patterns
store.events().filter(severity="error")              # exact match
store.events().filter(severity=["error", "warning"]) # IN clause
store.events().filter(file_path="%test%")            # LIKE pattern
store.events().filter("line_number > 100")           # raw SQL

# Query a log file directly (without storing)
events = (
    LogQuery.from_file("build.log")
    .filter(severity="error")
    .df()
)

# Aggregations
store.events().group_by("file_path").count()
store.events().value_counts("severity")
```

See [Python API Guide](docs/python-api.md) for full documentation.

## Storage

Logs are stored as Hive-partitioned parquet files (zstd compressed):

```
.lq/
├── blq.duckdb     # Database with pre-loaded SQL macros
├── logs/
│   └── date=2024-01-15/
│       └── source=build/
│           └── 001_make_103000.parquet
├── raw/           # Optional raw logs (--keep-raw)
├── commands.yaml  # Registered commands
└── schema.sql     # SQL schema reference
```

### SQL Macros (blq_ prefix)

All SQL macros use the `blq_` prefix:

```bash
# Direct DuckDB access
duckdb .lq/blq.duckdb "SELECT * FROM blq_status()"
duckdb .lq/blq.duckdb "SELECT * FROM blq_errors(20)"
duckdb .lq/blq.duckdb "SELECT * FROM blq_load_events() WHERE severity='error'"
```

| Macro | Description |
|-------|-------------|
| `blq_load_events()` | All events from parquet files |
| `blq_load_runs()` | Aggregated run statistics |
| `blq_status()` | Quick status overview |
| `blq_errors(n)` | Recent errors (default: 10) |
| `blq_warnings(n)` | Recent warnings (default: 10) |
| `blq_history(n)` | Run history (default: 20) |
| `blq_diff(run1, run2)` | Compare errors between runs |

## Documentation

See [docs/](docs/) for detailed documentation:

- [Getting Started](docs/getting-started.md)
- [Commands Reference](docs/commands/)
- [Query Guide](docs/query-guide.md)
- [Python API Guide](docs/python-api.md)
- [Integration Guide](docs/integration.md)

## License

MIT
