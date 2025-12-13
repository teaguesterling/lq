# lq - Log Query

Capture and query build/test logs with DuckDB.

## Installation

```bash
pip install -e .
```

With duck_hunt extension for enhanced parsing:
```bash
pip install -e ".[duck-hunt]"
```

## Quick Start

```bash
# Initialize in your project
lq init

# Run a command and capture output
lq run make -j8

# Import existing log file
lq import build.log --name "nightly build"

# Capture from stdin
make 2>&1 | lq capture --name "make"

# Check status
lq status

# View errors
lq errors

# Run SQL queries
lq sql "SELECT * FROM lq_events WHERE severity = 'error'"
```

## Commands

| Command | Description |
|---------|-------------|
| `lq init` | Initialize .lq directory |
| `lq run <cmd>` | Run command and capture output |
| `lq import <file>` | Import existing log file |
| `lq capture` | Capture from stdin |
| `lq status` | Show status of all sources |
| `lq errors` | Show recent errors |
| `lq warnings` | Show recent warnings |
| `lq summary` | Aggregate summary |
| `lq history` | Show run history |
| `lq sql <query>` | Run arbitrary SQL |
| `lq shell` | Interactive SQL shell |
| `lq prune` | Remove old logs |

## Storage

Logs are stored as Hive-partitioned parquet files:

```
.lq/
├── logs/
│   └── date=2024-01-15/
│       └── source=run/
│           └── 001_make_103000.parquet
├── raw/           # Optional raw logs
└── schema.sql     # SQL schema and macros
```

## Integration with duck_hunt

When the `duck_hunt` DuckDB extension is available, `lq` uses it for parsing 44+ log formats:
- Build systems (make, cmake, cargo, maven, gradle, etc.)
- Test frameworks (pytest, jest, go test, etc.)
- Linting tools (eslint, pylint, mypy, etc.)
- CI/CD logs (GitHub Actions, GitLab CI, Jenkins)

Without duck_hunt, basic error/warning detection is used.

## MCP Integration

The schema is designed to work with duckdb_mcp for agent access:

```sql
-- Expose lq status as MCP tool
SELECT mcp_publish_tool('lq_status', 'Get log status',
    'FROM lq_status()', '{}', '[]', 'markdown');
```

## License

MIT
