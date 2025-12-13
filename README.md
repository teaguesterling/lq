# lq - Log Query

A CLI tool for capturing, querying, and analyzing build/test logs using DuckDB.

## Features

- **Capture logs** from commands, files, or stdin
- **Query directly** with SQL or simple filter syntax
- **Structured output** in JSON, CSV, or Markdown for agent integration
- **Event references** for drilling into specific errors
- **Command registry** for reusable build/test commands
- **60+ log formats** supported via duck_hunt extension

## Installation

```bash
pip install lq
```

Initialize in your project (installs duck_hunt extension):
```bash
lq init
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

## Global Options

| Flag | Description |
|------|-------------|
| `-F, --log-format` | Log format hint (default: auto) |

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
- [Integration Guide](docs/integration.md)

## License

MIT
