# lq Agent Interface

This document describes how AI agents should interact with `lq` for efficient log analysis.

## Token-Efficient Workflow

**Problem**: Raw build logs can be 10MB+, burning context when agents just need "what failed?"

**Solution**: Use `lq` commands for structured, minimal-token queries.

### Quick Status Check (~10 tokens output)
```bash
lq status
```
Output:
```
[ OK ] make
[FAIL] gh run 123
[WARN] eslint
```

### Get Errors (~200 tokens for 5 errors)
```bash
lq errors --limit 5
```

### Compact Format (~100 tokens for 10 errors)
```bash
lq errors --compact --limit 10
```
Output:
```
src/main.cpp:42:5: undefined reference to 'foo'
src/utils.cpp:15:1: missing semicolon
```

### JSON for Programmatic Use
```bash
lq errors --json --limit 5
```

## MCP Tools (When Integrated)

When `lq` is exposed via duckdb_mcp, agents can use these tools:

| Tool | Description | Token Cost |
|------|-------------|------------|
| `lq_status` | Quick status badges | ~10 |
| `lq_errors` | Recent errors | ~200 |
| `lq_summary` | Aggregate by tool/category | ~100 |
| `lq_sql` | Custom queries | Variable |

## Recommended Agent Workflow

1. **Start with status**: `lq status` - see if anything failed
2. **Drill into failures**: `lq errors --source "failed_source" --limit 5`
3. **Get context if needed**: `lq sql "SELECT * FROM lq_events WHERE event_id = 42"`

## SQL Macros Available

```sql
-- Quick queries
FROM lq_status();           -- Status badges
FROM lq_errors(10);         -- Recent errors
FROM lq_warnings(10);       -- Recent warnings
FROM lq_summary();          -- Aggregate summary

-- Filtered queries
FROM lq_errors_for('make', 5);  -- Errors for specific source
FROM lq_file('main.cpp');       -- Events for specific file

-- History
FROM lq_history(20);        -- Run history
lq_diff(run1, run2);        -- Compare two runs
```

## Storage Location

Logs are stored in `.lq/logs/` with Hive partitioning:
```
.lq/logs/date=2024-01-15/source=run/001_make_103000.parquet
```

Agents can directly query parquet files if needed:
```sql
SELECT * FROM read_parquet('.lq/logs/**/*.parquet', hive_partitioning=true)
WHERE severity = 'error'
```
