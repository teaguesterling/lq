# Commands Reference

## Global Options

These options apply to all commands:

| Option | Description |
|--------|-------------|
| `-F, --log-format FORMAT` | Log format hint for parsing (default: `auto`) |
| `-h, --help` | Show help message |

## Command Categories

### Setup

| Command | Description | Documentation |
|---------|-------------|---------------|
| `init` | Initialize .lq directory | [init](init.md) |

### Running & Capturing

| Command | Alias | Description | Documentation |
|---------|-------|-------------|---------------|
| `run` | `r` | Run a registered command | [run](run.md) |
| `exec` | `e` | Execute ad-hoc shell command | [exec](exec.md) |
| `import` | | Import an existing log file | [capture](capture.md) |
| `capture` | | Capture from stdin | [capture](capture.md) |

### Viewing Results

| Command | Description | Documentation |
|---------|-------------|---------------|
| `errors` | Show recent errors | [errors](errors.md) |
| `warnings` | Show recent warnings | [errors](errors.md) |
| `event` | Show details for a specific event | [errors](errors.md) |
| `context` | Show log context around an event | [errors](errors.md) |

### Status & History

| Command | Description | Documentation |
|---------|-------------|---------------|
| `status` | Show status of all sources | [status](status.md) |
| `history` | Show run history | [status](status.md) |
| `summary` | Aggregate summary | [status](status.md) |

### Querying

| Command | Alias | Description | Documentation |
|---------|-------|-------------|---------------|
| `query` | `q` | Query log files or stored events with SQL | [query](query.md) |
| `filter` | `f` | Filter with simple key=value syntax | [filter](filter.md) |
| `sql` | | Run arbitrary SQL queries | [sql](sql.md) |
| `shell` | | Interactive DuckDB shell | [sql](sql.md) |

### Command Registry

| Command | Description | Documentation |
|---------|-------------|---------------|
| `register` | Register a reusable command | [registry](registry.md) |
| `unregister` | Remove a registered command | [registry](registry.md) |
| `commands` | List registered commands | [registry](registry.md) |

### Maintenance

| Command | Description | Documentation |
|---------|-------------|---------------|
| `prune` | Remove old log files | [maintenance](maintenance.md) |

### Server

| Command | Description | Documentation |
|---------|-------------|---------------|
| `serve` | Start MCP server for AI agents | [MCP Server](../mcp.md) |

## Quick Reference

```bash
# Query a file
blq q build.log
blq q -s file_path,message build.log
blq q -f "severity='error'" build.log

# Filter (simple syntax)
blq f severity=error build.log
blq f -c severity=error build.log    # count only

# Execute ad-hoc commands
blq exec make
blq e --json pytest -v

# Register and run commands
blq register build "make -j8"
blq run build
blq r --json build

# View events
blq errors
blq event 1:3
blq context 1:3
```
