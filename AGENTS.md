# AGENTS.md - Guide for AI Agents Using lq

This document provides guidance for AI agents (Claude, GPT, etc.) on effectively using lq to help users with build failures, test errors, and log analysis.

## Overview

**lq** (Log Query) captures, stores, and queries build/test logs using DuckDB. It's designed for agent integration with:
- Structured JSON output for easy parsing
- Event references for drill-down workflows
- Simple filter syntax for quick queries
- SQL access for complex analysis

## When to Use lq

Use lq when the user:
- Has a build or test failure to investigate
- Wants to analyze log files
- Needs to find patterns across multiple runs
- Asks about errors, warnings, or test failures

## Quick Reference

```bash
# Query a log file directly
lq q build.log                              # all events
lq q -s file_path,line_number,message build.log  # select columns
lq q --json build.log                       # JSON output

# Filter with simple syntax
lq f severity=error build.log               # errors only
lq f severity=error,warning build.log       # errors OR warnings
lq f file_path~main build.log               # file contains "main"
lq f -c severity=error build.log            # count errors

# Run and capture commands
lq run make                                 # run and capture
lq run --json --quiet make                  # structured output, no streaming

# View stored events
lq errors                                   # recent errors
lq event 1:3                                # specific event details
lq context 1:3                              # surrounding log lines
```

## Workflows

### Build Failure Investigation

When a user reports a build failure:

```bash
# Step 1: Run the build with structured output
lq run --json --quiet make

# Step 2: If the JSON shows errors, get the summary
lq errors

# Step 3: For each error ref (e.g., "1:3"), get details
lq event 1:3

# Step 4: If you need more context (surrounding lines)
lq context 1:3 --lines 5
```

**Agent response pattern:**
1. Run the build, capture JSON output
2. Parse the errors array from JSON
3. Present errors to user with file:line locations
4. Offer to investigate specific errors in detail

### Test Failure Analysis

```bash
# Run tests with JSON output
lq run --json pytest -v

# Filter for failed tests
lq f severity=error test_output.log

# Get details on a specific failure
lq event 1:5
```

### Log File Exploration

When the user has an existing log file:

```bash
# Quick overview - count by severity
lq f -c severity=error build.log
lq f -c severity=warning build.log

# List errors with locations
lq q -s file_path,line_number,message -f "severity='error'" build.log

# Find errors in specific files
lq f severity=error file_path~main.c build.log
```

### Finding Patterns Across Runs

```bash
# Errors that appear in multiple runs
lq sql "SELECT error_fingerprint, COUNT(*) as runs, ANY_VALUE(message)
        FROM lq_events
        WHERE severity='error'
        GROUP BY error_fingerprint
        HAVING COUNT(DISTINCT run_id) > 1"

# New errors (in latest run but not previous)
lq sql "SELECT message, file_path, line_number
        FROM lq_events
        WHERE run_id = (SELECT MAX(run_id) FROM lq_events)
          AND severity = 'error'
          AND error_fingerprint NOT IN (
              SELECT error_fingerprint FROM lq_events
              WHERE run_id < (SELECT MAX(run_id) FROM lq_events)
          )"
```

## Output Formats

### When to Use Each Format

| Format | Use When |
|--------|----------|
| `--json` | Parsing output programmatically, storing results |
| `--csv` | User wants to export to spreadsheet |
| `--markdown` | Creating reports, PR comments, documentation |
| (default table) | Displaying to user in conversation |

### JSON Output Structure

```bash
lq run --json make
```

```json
{
  "run_id": 1,
  "command": "make",
  "status": "FAIL",           // "OK", "FAIL", or "WARN"
  "exit_code": 2,
  "duration_sec": 12.5,
  "summary": {
    "total_events": 5,
    "errors": 2,
    "warnings": 3
  },
  "errors": [
    {
      "ref": "1:1",            // Use this for drill-down
      "severity": "error",
      "file_path": "src/main.c",
      "line_number": 15,
      "column_number": 5,
      "message": "undefined variable 'foo'"
    }
  ]
}
```

### Parsing JSON Output

When parsing lq JSON output:
1. Check `status` field: "OK" means success, "FAIL" means errors
2. Use `errors` array for error details
3. Use `ref` field (e.g., "1:1") for drill-down with `lq event` and `lq context`

## Event References

Event references follow the format `run_id:event_id` (e.g., `1:3` means run 1, event 3).

```bash
# Get full event details
lq event 1:3

# Get surrounding log context
lq context 1:3
lq context 1:3 --lines 10  # more context
```

**Best practice:** When presenting errors to users, include the ref so they can ask for more details:

> Error at `src/main.c:15`: undefined variable 'foo' [ref: 1:1]

## Query vs Filter

| Task | Use `lq filter` | Use `lq query` |
|------|-----------------|----------------|
| Simple exact match | `lq f severity=error` | |
| Multiple values (OR) | `lq f severity=error,warning` | |
| Contains/LIKE | `lq f file_path~main` | |
| Select specific columns | | `lq q -s file,message` |
| Complex WHERE | | `lq q -f "line > 100"` |
| ORDER BY | | `lq q -o line_number` |
| Aggregations | | `lq sql "SELECT ..."` |

## MCP Integration (Future)

When lq MCP server is available, these tools will be exposed:

### Planned MCP Tools

| Tool | Description |
|------|-------------|
| `lq_run` | Run a command and capture output |
| `lq_query` | Query log files or stored events |
| `lq_filter` | Filter with simple syntax |
| `lq_errors` | Get recent errors |
| `lq_event` | Get event details by reference |
| `lq_context` | Get log context around event |

### MCP Workflow Example

```
1. User: "The build is failing"
2. Agent: calls lq_run("make", json=true, quiet=true)
3. Agent: parses errors from response
4. Agent: presents summary to user
5. User: "What's error 1:3 about?"
6. Agent: calls lq_event("1:3")
7. Agent: calls lq_context("1:3") for surrounding lines
8. Agent: explains the error with full context
```

## Best Practices

### For Build/Test Runs

1. **Always use `--json --quiet`** for programmatic parsing:
   ```bash
   lq run --json --quiet make
   ```

2. **Check exit code** - lq preserves the command's exit code

3. **Use event refs for drill-down** - don't try to re-parse output

### For Log Analysis

1. **Start with counts** to understand the scope:
   ```bash
   lq f -c severity=error build.log
   ```

2. **Select only needed columns** for cleaner output:
   ```bash
   lq q -s file_path,line_number,message build.log
   ```

3. **Use `--json` when you'll parse the output**

### For Users

1. **Show file:line locations** - users can jump to code
2. **Include refs** - users can ask for more details
3. **Summarize first, detail on request** - don't overwhelm

### Error Handling

If lq commands fail:
- Check if `.lq/` is initialized: `lq init`
- Check if duck_hunt is installed: `lq init` will install it
- For file queries, verify the file exists

## Common Fields

| Field | Description | Example |
|-------|-------------|---------|
| `severity` | error, warning, info, note | `error` |
| `file_path` | Source file | `src/main.c` |
| `line_number` | Line in source | `15` |
| `column_number` | Column in source | `5` |
| `message` | Error/warning text | `undefined variable` |
| `error_fingerprint` | Unique hash for dedup | `abc123...` |
| `ref` | Event reference | `1:3` |
| `run_id` | Run identifier | `1` |

## Example Agent Interaction

**User:** "My build is failing, can you help?"

**Agent actions:**
```bash
# Run the build
lq run --json --quiet make 2>&1
```

**Agent response:**
> I ran the build and found 2 errors:
>
> 1. `src/main.c:15:5` - undefined variable 'foo' [ref: 1:1]
> 2. `src/utils.c:42:1` - expected ';' before '}' [ref: 1:2]
>
> Would you like me to look at either of these in detail?

**User:** "Tell me more about the first one"

**Agent actions:**
```bash
lq context 1:1 --lines 5
```

**Agent response:**
> Here's the context around that error:
> ```c
> 13: int main() {
> 14:     int bar = 10;
> 15:     printf("%d", foo);  // ERROR: 'foo' undefined
> 16:     return 0;
> 17: }
> ```
> It looks like you're using `foo` but only declared `bar`. Did you mean to use `bar` instead?
