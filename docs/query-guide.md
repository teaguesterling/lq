# Query Guide

This guide covers techniques for effectively querying logs with lq.

## Two Ways to Query

### 1. Query Files Directly

Query log files without importing them:

```bash
lq q build.log
lq f severity=error build.log
```

This uses the `duck_hunt` extension to parse the file on-the-fly.

### 2. Query Stored Events

Query previously captured events:

```bash
lq q -f "severity='error'"
lq f severity=error
```

This queries the `lq_events` view which combines all stored parquet files.

## Choosing Your Tool

| Use Case | Tool | Example |
|----------|------|---------|
| Quick filter | `lq filter` | `lq f severity=error log.txt` |
| Column selection | `lq query` | `lq q -s file,message log.txt` |
| Complex conditions | `lq query` | `lq q -f "line > 100"` |
| Full SQL | `lq sql` | `lq sql "SELECT ..."` |
| Interactive | `lq shell` | `lq shell` |

## Common Patterns

### Find All Errors

```bash
# Simple
lq f severity=error build.log

# With location info
lq q -s file_path,line_number,message -f "severity='error'" build.log
```

### Group by File

```bash
lq sql "SELECT file_path, COUNT(*) as errors
        FROM read_duck_hunt_log('build.log', 'auto')
        WHERE severity='error'
        GROUP BY file_path
        ORDER BY errors DESC"
```

### Find Repeated Errors

Using error fingerprints:

```bash
lq sql "SELECT error_fingerprint, COUNT(*) as occurrences,
               ANY_VALUE(message) as example
        FROM lq_events
        GROUP BY error_fingerprint
        HAVING COUNT(*) > 1
        ORDER BY occurrences DESC"
```

### Compare Runs

```bash
# Errors in latest run but not previous
lq sql "SELECT DISTINCT error_fingerprint, message
        FROM lq_events
        WHERE run_id = (SELECT MAX(run_id) FROM lq_events)
          AND error_fingerprint NOT IN (
              SELECT error_fingerprint FROM lq_events
              WHERE run_id = (SELECT MAX(run_id) - 1 FROM lq_events)
          )"
```

### Timeline of Errors

```bash
lq sql "SELECT date, source_name, COUNT(*) as errors
        FROM lq_events
        WHERE severity = 'error'
        GROUP BY date, source_name
        ORDER BY date DESC"
```

## Available Fields

### From Log Files

| Field | Description |
|-------|-------------|
| `event_id` | Sequential ID within the file |
| `severity` | error, warning, info, note |
| `file_path` | Source file path |
| `line_number` | Line in source file |
| `column_number` | Column in source file |
| `message` | Error/warning text |
| `error_fingerprint` | Unique hash for deduplication |
| `tool_name` | Detected tool (gcc, pytest, etc.) |
| `category` | Error category |

### From Stored Events

Additional fields:

| Field | Description |
|-------|-------------|
| `run_id` | Unique run identifier |
| `source_name` | Name given to the source |
| `source_type` | run, import, or capture |
| `date` | Partition date |

## Output Formats

### Table (Default)

```bash
lq q -s file_path,message build.log
```

```
  file_path                   message
 src/main.c undefined variable 'foo'
```

### JSON

```bash
lq q --json build.log
```

Best for:
- Piping to `jq`
- Agent/LLM consumption
- API responses

### CSV

```bash
lq q --csv build.log > errors.csv
```

Best for:
- Spreadsheet import
- Data analysis tools

### Markdown

```bash
lq q --markdown build.log
```

Best for:
- Documentation
- GitHub comments
- Reports

## Performance Tips

### Limit Results

Always use `-n` when exploring:

```bash
lq q -n 10 build.log
```

### Select Only Needed Columns

```bash
# Fast
lq q -s file_path,message build.log

# Slow (returns all columns)
lq q build.log
```

### Use Date Partitions

Stored data is partitioned by date. Filter by date for faster queries:

```bash
lq sql "SELECT * FROM lq_events WHERE date = '2024-01-15'"
```

## Advanced: Raw SQL

For complex analysis, use `lq sql` or `lq shell`:

```bash
# Ad-hoc query
lq sql "SELECT file_path, COUNT(*) FROM lq_events GROUP BY 1"

# Interactive session
lq shell
```

In the shell, you have full DuckDB SQL available plus:
- `lq_events` view - all stored events
- `lq_ref(run_id, event_id)` - create event reference
- `lq_location(file, line, col)` - format location string
