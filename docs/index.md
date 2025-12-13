# lq Documentation

**lq** (Log Query) is a command-line tool for capturing, querying, and analyzing build and test logs using DuckDB.

## Why lq?

Build and test logs contain valuable information, but they're often:
- Scattered across CI runs, local builds, and log files
- Hard to search and correlate
- Lost after a few days

lq solves this by:
- **Storing logs locally** in efficient parquet format
- **Parsing 60+ formats** via the duck_hunt extension
- **Providing SQL access** for powerful queries
- **Outputting structured data** for AI agent integration

## Core Concepts

### Events

Every error, warning, or notable item in a log becomes an **event** with:
- `severity` - error, warning, info, etc.
- `file_path`, `line_number`, `column_number` - source location
- `message` - the error/warning text
- `error_fingerprint` - unique identifier for deduplication

### Event References

Events are referenced by `run_id:event_id` (e.g., `1:3` means run 1, event 3). This allows drilling down from summaries:

```bash
lq errors              # Shows refs like 1:3, 1:4
lq event 1:3           # Get details for specific event
lq context 1:3         # See surrounding log lines
```

### Sources

A **source** is anything that produces logs:
- A command run (`lq run make`)
- An imported file (`lq import build.log`)
- Stdin capture (`make | lq capture`)

### Storage

Logs are stored in `.lq/` in your project:

```
.lq/
├── logs/              # Parquet files (Hive-partitioned)
├── raw/               # Raw log files (optional)
├── commands.yaml      # Registered commands
└── schema.sql         # SQL schema
```

## Quick Links

- [Getting Started](getting-started.md) - Installation and first steps
- [Commands Reference](commands/) - All commands in detail
- [Query Guide](query-guide.md) - Querying logs effectively
- [Integration Guide](integration.md) - Using with AI agents and CI/CD
