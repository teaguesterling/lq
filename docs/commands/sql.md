# SQL Commands

blq stores all log data in DuckDB-compatible parquet files and provides commands for direct SQL access.

## sql - Execute SQL Queries

Run arbitrary SQL queries against the log database.

```bash
blq sql "SELECT * FROM lq_events LIMIT 10"
blq sql "SELECT file_path, COUNT(*) FROM lq_events GROUP BY file_path"
blq sql "FROM lq_status()"
```

### Usage

```bash
blq sql <query>
```

Queries can span multiple words (quoted or unquoted):
```bash
blq sql SELECT COUNT\(*\) FROM lq_events
blq sql "SELECT COUNT(*) FROM lq_events"
```

### Available Views

| View | Description |
|------|-------------|
| `lq_events` | All parsed events (errors, warnings, info) |
| `lq_runs` | Run metadata (command, exit code, timestamps) |
| `lq_source_status` | Latest run status per source |

### Available Macros

| Macro | Description |
|-------|-------------|
| `lq_status()` | Quick status overview |
| `lq_status_verbose()` | Detailed status with exit codes |
| `lq_errors(n)` | Recent errors (default n=10) |
| `lq_errors_for(src, n)` | Errors for specific source |
| `lq_warnings(n)` | Recent warnings (default n=10) |
| `lq_summary()` | Aggregate by tool/category |
| `lq_summary_latest()` | Summary for latest run only |
| `lq_history(n)` | Run history (default n=20) |
| `lq_diff(run1, run2)` | Compare errors between runs |
| `lq_event(id)` | Get event by ID |
| `lq_files()` | List all files with events |
| `lq_file(path)` | Events for specific file |
| `lq_similar_events(fp, n)` | Events in same file |

### Example Queries

**Errors by file:**
```bash
blq sql "SELECT file_path, COUNT(*) as errors FROM lq_events WHERE severity='error' GROUP BY file_path ORDER BY errors DESC"
```

**Recent runs with errors:**
```bash
blq sql "SELECT run_id, source_name, error_count FROM lq_runs WHERE error_count > 0 ORDER BY started_at DESC LIMIT 10"
```

**Using macros:**
```bash
blq sql "FROM lq_errors(20)"
blq sql "FROM lq_diff(1, 2)"
blq sql "FROM lq_file('src/main.c')"
```

**Time-based queries:**
```bash
blq sql "SELECT * FROM lq_events WHERE started_at > now() - INTERVAL '1 hour'"
```

**Run metadata:**
```bash
blq sql "SELECT run_id, git_commit, git_branch, ci['provider'] as ci FROM lq_runs"
```

## shell - Interactive DuckDB Shell

Start an interactive DuckDB shell with the log database loaded.

```bash
blq shell
```

This opens a DuckDB CLI session with:
- duck_hunt extension loaded
- Schema and macros loaded from `.lq/schema.sql`
- Custom prompt `blq> `

### Interactive Session

```
blq shell
blq> SELECT COUNT(*) FROM lq_events;
┌──────────────┐
│ count_star() │
├──────────────┤
│          142 │
└──────────────┘
blq> FROM lq_status();
...
blq> .quit
```

### Shell Features

The shell supports all DuckDB CLI features:
- Tab completion
- Multi-line queries
- `.commands` for help
- `.timer on` for query timing
- `.mode` for output format

### Use Cases

**Exploratory analysis:**
```sql
-- Check schema
.schema lq_events

-- Sample data
SELECT * FROM lq_events LIMIT 5;

-- Find patterns
SELECT message, COUNT(*)
FROM lq_events
WHERE severity = 'error'
GROUP BY message
ORDER BY COUNT(*) DESC;
```

**Ad-hoc investigation:**
```sql
-- What's breaking?
FROM lq_errors(50);

-- Compare runs
FROM lq_diff(3, 5);

-- Find related errors
FROM lq_similar_events('src/auth.c', 20);
```

## Schema Reference

### lq_events View

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER | Run identifier |
| `event_id` | INTEGER | Event number within run |
| `severity` | VARCHAR | error, warning, info, debug |
| `file_path` | VARCHAR | Source file path |
| `line_number` | INTEGER | Line number |
| `column_number` | INTEGER | Column number |
| `message` | VARCHAR | Event message |
| `tool_name` | VARCHAR | Tool that generated event |
| `category` | VARCHAR | Error category |
| `error_code` | VARCHAR | Error code (e.g., E0001) |
| `source_name` | VARCHAR | Source name (build, test, etc.) |
| `started_at` | TIMESTAMP | When the run started |

### lq_runs View

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER | Run identifier |
| `source_name` | VARCHAR | Source name |
| `command` | VARCHAR | Command executed |
| `started_at` | TIMESTAMP | Start timestamp |
| `completed_at` | TIMESTAMP | End timestamp |
| `exit_code` | INTEGER | Process exit code |
| `error_count` | INTEGER | Number of errors |
| `warning_count` | INTEGER | Number of warnings |
| `event_count` | INTEGER | Total events |
| `cwd` | VARCHAR | Working directory |
| `hostname` | VARCHAR | Machine hostname |
| `platform` | VARCHAR | OS (Linux, Darwin, Windows) |
| `arch` | VARCHAR | Architecture |
| `git_commit` | VARCHAR | Git HEAD SHA |
| `git_branch` | VARCHAR | Git branch |
| `git_dirty` | BOOLEAN | Uncommitted changes |
| `ci` | MAP | CI provider and context |
| `environment` | MAP | Captured env vars |

## Tips

### Escaping in Shell

When using `blq sql` from bash, escape or quote special characters:
```bash
blq sql "SELECT * FROM lq_events WHERE message LIKE '%undefined%'"
blq sql 'SELECT * FROM lq_events WHERE severity = '"'"'error'"'"
```

### Export Results

```bash
# To CSV
blq sql "SELECT * FROM lq_events" > events.csv

# To JSON (use DuckDB format)
blq shell
blq> .mode json
blq> SELECT * FROM lq_events;
```

### Complex Analysis

For complex analysis, use the shell:
```bash
blq shell << 'EOF'
.timer on
WITH error_files AS (
    SELECT file_path, COUNT(*) as errors
    FROM lq_events
    WHERE severity = 'error'
    GROUP BY file_path
)
SELECT * FROM error_files WHERE errors > 5 ORDER BY errors DESC;
EOF
```
