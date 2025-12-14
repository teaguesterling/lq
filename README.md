# lq - Log Query

A CLI tool for capturing, querying, and analyzing build/test logs using DuckDB.

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
pip install lq
```

Initialize in your project (installs duck_hunt extension):
```bash
lq init         # Basic init
lq init --mcp   # Also create .mcp.json for AI agents
```

## Quick Start

```bash
# Query a log file directly
lq q build.log
lq q -s file_path,line_number,message build.log

# Filter with simple syntax
lq f severity=error build.log
lq f severity=error,warning file_path~main build.log

# Run and capture a command
lq run make -j8
lq run --json make test

# View recent errors
lq errors

# Drill into a specific error
lq event 1:3
lq context 1:3
```

## Commands

### Querying

| Command | Description |
|---------|-------------|
| `lq query` / `lq q` | Query log files or stored events |
| `lq filter` / `lq f` | Filter with simple key=value syntax |
| `lq sql <query>` | Run arbitrary SQL |
| `lq shell` | Interactive SQL shell |

### Capturing

| Command | Description |
|---------|-------------|
| `lq run <cmd>` | Run command and capture output |
| `lq import <file>` | Import existing log file |
| `lq capture` | Capture from stdin |

### Viewing

| Command | Description |
|---------|-------------|
| `lq errors` | Show recent errors |
| `lq warnings` | Show recent warnings |
| `lq event <ref>` | Show event details (e.g., `lq event 1:3`) |
| `lq context <ref>` | Show log context around event |
| `lq status` | Show status of all sources |
| `lq history` | Show run history |

### Management

| Command | Description |
|---------|-------------|
| `lq init` | Initialize .lq directory |
| `lq register` | Register a reusable command |
| `lq unregister` | Remove a registered command |
| `lq commands` | List registered commands |
| `lq prune` | Remove old logs |

## Query Examples

```bash
# Select specific columns
lq q -s file_path,line_number,severity,message build.log

# Filter with SQL WHERE clause
lq q -f "severity='error' AND file_path LIKE '%main%'" build.log

# Order and limit results
lq q -o "line_number" -n 10 build.log

# Output as JSON (great for agents)
lq q --json build.log

# Output as CSV
lq q --csv build.log

# Query stored events (no file argument)
lq q -f "severity='error'"
```

## Filter Syntax

The `lq filter` command provides grep-like simplicity:

```bash
# Exact match
lq f severity=error build.log

# Multiple values (OR)
lq f severity=error,warning build.log

# Contains (LIKE)
lq f file_path~main build.log

# Not equal
lq f severity!=info build.log

# Invert match (like grep -v)
lq f -v severity=error build.log

# Count matches
lq f -c severity=error build.log

# Case insensitive
lq f -i message~undefined build.log
```

## Structured Output for Agents

```bash
# JSON output with errors
lq run --json make

# Markdown summary
lq run --markdown make

# Quiet mode (no streaming, just results)
lq run --quiet --json make
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
# Register commands
lq register build "make -j8" --description "Build project"
lq register test "pytest -v" --timeout 600

# Run by name
lq run build
lq run test

# List registered commands
lq commands
```

## Run Metadata

Each `lq run` automatically captures execution context:

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
lq sql "SELECT hostname, git_branch, environment['VIRTUAL_ENV'] FROM lq_events"
```

## MCP Server

lq includes an MCP server for AI agent integration:

```bash
lq serve                    # stdio transport (Claude Desktop)
lq serve --transport sse    # HTTP/SSE transport
```

Tools available: `run`, `query`, `errors`, `warnings`, `event`, `context`, `status`, `history`, `diff`

See [MCP Guide](docs/mcp.md) for details.

## Global Options

| Flag | Description |
|------|-------------|
| `-F, --log-format` | Log format hint (default: auto) |

## Python API

lq provides a fluent Python API for programmatic access:

```python
from lq import LogStore, LogQuery

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

Logs are stored as Hive-partitioned parquet files:

```
.lq/
├── logs/
│   └── date=2024-01-15/
│       └── source=build/
│           └── 001_make_103000.parquet
├── raw/           # Optional raw logs (--keep-raw)
├── commands.yaml  # Registered commands
└── schema.sql     # SQL schema and macros
```

## Documentation

See [docs/](docs/) for detailed documentation:

- [Getting Started](docs/getting-started.md)
- [Commands Reference](docs/commands/)
- [Query Guide](docs/query-guide.md)
- [Python API Guide](docs/python-api.md)
- [Integration Guide](docs/integration.md)

## License

MIT
