# MCP Server Guide

lq provides an MCP (Model Context Protocol) server for AI agent integration. This allows agents to run builds, query logs, and analyze errors through a standardized interface.

## Quick Start

```bash
# Start the MCP server
lq serve

# Or with specific transport
lq serve --transport stdio      # For Claude Desktop, etc.
lq serve --transport sse --port 8080  # For HTTP clients
```

## Overview

The lq MCP server exposes:

- **Tools** - Actions agents can perform (run commands, query logs)
- **Resources** - Data agents can read (events, runs, status)
- **Prompts** - Templates for common workflows (fix errors, analyze regressions)

All tools are namespaced under the `lq` server, so `run` becomes `lq.run` when accessed by agents.

---

## Tools

### run

Run a command and capture its output.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `command` | string | Yes | Command to run (registered name or shell command) |
| `args` | string[] | No | Additional arguments |
| `timeout` | number | No | Timeout in seconds (default: 300) |

**Returns:**

```json
{
  "run_id": 1,
  "status": "FAIL",
  "exit_code": 2,
  "duration_seconds": 12.5,
  "error_count": 3,
  "warning_count": 5,
  "errors": [
    {
      "ref": "1:1",
      "file_path": "src/main.c",
      "line_number": 15,
      "column_number": 5,
      "message": "undefined variable 'foo'",
      "tool_name": "gcc",
      "category": "error"
    }
  ]
}
```

**Example:**

```json
{
  "tool": "run",
  "arguments": {
    "command": "make",
    "args": ["-j8"]
  }
}
```

---

### query

Query stored log events with SQL.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `sql` | string | Yes | SQL query against lq_events view |
| `limit` | number | No | Max rows to return (default: 100) |

**Returns:**

```json
{
  "columns": ["file_path", "line_number", "message"],
  "rows": [
    ["src/main.c", 15, "undefined variable 'foo'"],
    ["src/utils.c", 42, "unused variable 'bar'"]
  ],
  "row_count": 2
}
```

**Example:**

```json
{
  "tool": "query",
  "arguments": {
    "sql": "SELECT file_path, COUNT(*) as count FROM lq_events WHERE severity='error' GROUP BY file_path ORDER BY count DESC",
    "limit": 10
  }
}
```

---

### errors

Get recent errors (convenience wrapper around `query`).

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number | No | Max errors to return (default: 20) |
| `run_id` | number | No | Filter to specific run |
| `source` | string | No | Filter to specific source name |
| `file_pattern` | string | No | Filter by file path pattern (SQL LIKE) |

**Returns:**

```json
{
  "errors": [
    {
      "ref": "1:1",
      "file_path": "src/main.c",
      "line_number": 15,
      "column_number": 5,
      "message": "undefined variable 'foo'",
      "tool_name": "gcc",
      "category": "error"
    }
  ],
  "total_count": 3
}
```

**Example:**

```json
{
  "tool": "errors",
  "arguments": {
    "limit": 10,
    "file_pattern": "%main%"
  }
}
```

---

### warnings

Get recent warnings (convenience wrapper around `query`).

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number | No | Max warnings to return (default: 20) |
| `run_id` | number | No | Filter to specific run |
| `source` | string | No | Filter to specific source name |

**Returns:** Same structure as `errors`.

---

### event

Get details for a specific event by reference.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `ref` | string | Yes | Event reference (e.g., "1:3") |

**Returns:**

```json
{
  "ref": "1:3",
  "run_id": 1,
  "event_id": 3,
  "severity": "error",
  "file_path": "src/main.c",
  "line_number": 15,
  "column_number": 5,
  "message": "undefined variable 'foo'",
  "tool_name": "gcc",
  "category": "error",
  "error_fingerprint": "gcc_error_a1b2c3d4",
  "raw_text": "src/main.c:15:5: error: undefined variable 'foo'",
  "log_line_start": 42,
  "log_line_end": 42,
  "cwd": "/home/user/project",
  "hostname": "dev-machine",
  "platform": "Linux",
  "arch": "x86_64",
  "git_commit": "abc1234",
  "git_branch": "main",
  "git_dirty": false,
  "ci": null
}
```

The event includes run metadata for context (see `history` tool for field descriptions).

**Example:**

```json
{
  "tool": "event",
  "arguments": {
    "ref": "1:3"
  }
}
```

---

### context

Get log context around a specific event.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `ref` | string | Yes | Event reference (e.g., "1:3") |
| `lines` | number | No | Lines of context before/after (default: 5) |

**Returns:**

```json
{
  "ref": "1:3",
  "context_lines": [
    {"line": 40, "text": "gcc -c src/main.c -o main.o"},
    {"line": 41, "text": "In file included from src/main.c:1:"},
    {"line": 42, "text": "src/main.c:15:5: error: undefined variable 'foo'", "is_event": true},
    {"line": 43, "text": "     int x = foo + 1;"},
    {"line": 44, "text": "             ^~~"}
  ]
}
```

---

### status

Get current status summary of all sources.

**Parameters:** None

**Returns:**

```json
{
  "sources": [
    {
      "name": "build",
      "status": "FAIL",
      "error_count": 3,
      "warning_count": 5,
      "last_run": "2024-01-15T10:30:00Z",
      "run_id": 5
    },
    {
      "name": "test",
      "status": "OK",
      "error_count": 0,
      "warning_count": 2,
      "last_run": "2024-01-15T10:25:00Z",
      "run_id": 4
    }
  ]
}
```

---

### history

Get run history.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number | No | Max runs to return (default: 20) |
| `source` | string | No | Filter to specific source name |

**Returns:**

```json
{
  "runs": [
    {
      "run_id": 5,
      "source_name": "build",
      "status": "FAIL",
      "error_count": 3,
      "warning_count": 5,
      "started_at": "2024-01-15T10:30:00Z",
      "duration_seconds": 12.5,
      "exit_code": 2,
      "cwd": "/home/user/project",
      "hostname": "dev-machine",
      "platform": "Linux",
      "arch": "x86_64",
      "git_commit": "abc1234",
      "git_branch": "main",
      "git_dirty": false,
      "ci": null
    }
  ]
}
```

Run metadata fields:

| Field | Type | Description |
|-------|------|-------------|
| `cwd` | string | Working directory |
| `hostname` | string | Machine name |
| `platform` | string | OS (Linux, Darwin, Windows) |
| `arch` | string | Architecture (x86_64, arm64) |
| `git_commit` | string | HEAD commit SHA |
| `git_branch` | string | Current branch |
| `git_dirty` | boolean | Uncommitted changes present |
| `ci` | object | CI provider info (if running in CI) |

---

### diff

Compare errors between two runs.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `run1` | number | Yes | First run ID (baseline) |
| `run2` | number | Yes | Second run ID (comparison) |

**Returns:**

```json
{
  "summary": {
    "run1_errors": 5,
    "run2_errors": 3,
    "fixed": 3,
    "new": 1,
    "unchanged": 2
  },
  "fixed": [
    {
      "file_path": "src/old.c",
      "message": "unused variable"
    }
  ],
  "new": [
    {
      "ref": "6:1",
      "file_path": "src/new.c",
      "message": "undefined function"
    }
  ]
}
```

---

## Resources

Resources provide read-only access to lq data.

### lq://status

Current status of all sources.

**URI:** `lq://status`

**MIME Type:** `application/json`

**Content:** Same as `status` tool response.

---

### lq://runs

List of all runs.

**URI:** `lq://runs`
**URI with filter:** `lq://runs?source=build&limit=10`

**MIME Type:** `application/json`

**Content:** Same as `history` tool response.

---

### lq://events

All stored events (with optional filtering).

**URI:** `lq://events`
**URI with filter:** `lq://events?severity=error&run_id=5`

**MIME Type:** `application/json`

---

### lq://event/{ref}

Single event details.

**URI:** `lq://event/1:3`

**MIME Type:** `application/json`

**Content:** Same as `event` tool response.

---

### lq://commands

Registered commands.

**URI:** `lq://commands`

**MIME Type:** `application/json`

**Content:**

```json
{
  "commands": [
    {
      "name": "build",
      "command": "make -j8",
      "description": "Build the project",
      "timeout": 300
    },
    {
      "name": "test",
      "command": "pytest -v",
      "description": "Run tests",
      "timeout": 600
    }
  ]
}
```

---

## Prompts

Prompts are templates for common agent workflows. When an agent selects a prompt, the server fills in the template variables with current data, giving the agent relevant context and clear instructions.

### fix-errors

Guide the agent through fixing build errors systematically.

**Arguments:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `run_id` | number | No | Specific run to fix (default: latest) |
| `file_pattern` | string | No | Focus on specific files |

**Example prompt (rendered):**

```
You are helping fix build errors in a software project.

## Current Status

| Source | Status | Errors | Warnings |
|--------|--------|--------|----------|
| build  | FAIL   | 3      | 5        |

## Errors to Fix

1. **ref: 5:1** `src/main.c:15:5`
   ```
   error: use of undeclared identifier 'config'
   ```

2. **ref: 5:2** `src/main.c:23:12`
   ```
   error: no member named 'timeout' in 'struct options'
   ```

3. **ref: 5:3** `src/utils.c:42:1`
   ```
   error: expected ';' after expression
   ```

## Instructions

1. Read each error and understand the root cause
2. Use `event(ref="5:1")` for full context if the message is unclear
3. Use `context(ref="5:1")` to see surrounding log lines
4. Fix errors in dependency order:
   - Missing includes/declarations first
   - Then type errors
   - Then syntax errors
5. After fixing, run `run(command="build")` to verify
6. Repeat until build passes

**Tips:**
- Error 5:1 and 5:2 are in the same file - likely related
- Check if 'config' was recently renamed or moved
- The ';' error (5:3) is often caused by macro expansion issues
```

---

### analyze-regression

Help identify why a build started failing between two runs.

**Arguments:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `good_run` | number | No | Last known good run ID |
| `bad_run` | number | No | First failing run ID (default: latest) |

**Example prompt (rendered):**

```
You are analyzing why a build started failing.

## Run Comparison

| Metric | Run 4 (good) | Run 5 (bad) | Delta |
|--------|--------------|-------------|-------|
| Status | OK           | FAIL        |       |
| Errors | 0            | 3           | +3    |
| Warnings | 12         | 15          | +3    |

## New Errors (not in Run 4)

1. **ref: 5:1** `src/auth.c:156:8`
   ```
   error: implicit declaration of function 'validate_token'
   ```

2. **ref: 5:2** `src/auth.c:203:15`
   ```
   error: 'TOKEN_EXPIRY' undeclared
   ```

3. **ref: 5:3** `src/auth.c:210:5`
   ```
   error: too few arguments to function 'create_session'
   ```

## Analysis Hints

- All 3 new errors are in `src/auth.c`
- Errors reference `validate_token`, `TOKEN_EXPIRY`, `create_session`
- These symbols may have been modified or moved

## Instructions

1. Check recent changes to authentication-related files
2. Look for renamed functions or changed signatures
3. Use `event(ref="5:1")` for full error context
4. Identify the root cause (likely a single change that broke multiple things)
5. Suggest the minimal fix to restore the build
```

---

### summarize-run

Generate a concise summary of a build/test run for reports or PR comments.

**Arguments:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `run_id` | number | No | Run to summarize (default: latest) |
| `format` | string | No | Output format: "brief", "detailed", "pr-comment" |

**Example prompt (rendered):**

```
Summarize this build/test run for a PR comment.

## Run Details

- **Run ID:** 5
- **Command:** make -j8
- **Status:** FAIL (exit code 2)
- **Duration:** 45.2 seconds
- **Started:** 2024-01-15 10:30:00

## Results

- **Errors:** 3
- **Warnings:** 15 (12 pre-existing, 3 new)

## Errors by File

| File | Count | Types |
|------|-------|-------|
| src/auth.c | 3 | undeclared identifier, missing args |

## Error Details

1. `src/auth.c:156` - implicit declaration of function 'validate_token'
2. `src/auth.c:203` - 'TOKEN_EXPIRY' undeclared
3. `src/auth.c:210` - too few arguments to function 'create_session'

## Instructions

Generate a summary suitable for a GitHub PR comment:
- Lead with pass/fail status
- List the key errors (not all 15 warnings)
- Group related errors
- Suggest what might have caused the failure
- Keep it concise (under 200 words)

**Output format:**
```markdown
## Build Status: ❌ Failed

**3 errors** in `src/auth.c` related to authentication functions.

### Errors
- `validate_token` - undeclared (missing include?)
- `TOKEN_EXPIRY` - undeclared constant
- `create_session` - wrong number of arguments

### Likely Cause
Recent changes to auth API. Check if `auth.h` was modified.
```
```

---

### investigate-flaky

Help investigate intermittently failing tests.

**Arguments:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `test_pattern` | string | No | Filter to specific test names |
| `lookback` | number | No | Number of runs to analyze (default: 10) |

**Example prompt (rendered):**

```
You are investigating flaky (intermittently failing) tests.

## Test Failure History (last 10 runs)

| Run | Status | Failed Tests |
|-----|--------|--------------|
| 10  | FAIL   | test_concurrent_write |
| 9   | OK     | - |
| 8   | OK     | - |
| 7   | FAIL   | test_concurrent_write |
| 6   | OK     | - |
| 5   | FAIL   | test_concurrent_write, test_timeout |
| 4   | OK     | - |
| 3   | OK     | - |
| 2   | FAIL   | test_concurrent_write |
| 1   | OK     | - |

## Flaky Test Analysis

| Test | Failures | Rate | Pattern |
|------|----------|------|---------|
| test_concurrent_write | 4/10 | 40% | Random |
| test_timeout | 1/10 | 10% | Rare |

## Most Recent Failure Details

**ref: 10:1** `tests/test_db.py:145`
```
FAILED test_concurrent_write - AssertionError: Expected 100 rows, got 98
```

## Instructions

1. Focus on `test_concurrent_write` (most frequent)
2. Use `event(ref="10:1")` to see full failure output
3. Look for patterns:
   - Race conditions (concurrent, parallel, thread)
   - Timing issues (timeout, sleep, wait)
   - Resource contention (connection, file, lock)
4. Check if failures correlate with system load or time of day
5. Suggest fixes or ways to make the test more deterministic
```

---

## Configuration

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "lq": {
      "command": "lq",
      "args": ["serve"],
      "cwd": "/path/to/your/project"
    }
  }
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `LQ_DIR` | Path to .lq directory | Auto-detect |
| `LQ_TIMEOUT` | Default command timeout (seconds) | 300 |

---

## Security Considerations

- **Command execution**: `run` executes shell commands. Only registered commands are allowed by default. Use `--allow-arbitrary` to permit any command.
- **File access**: The server only accesses files within the project directory.
- **SQL injection**: `query` uses parameterized queries where possible. Complex queries are sandboxed to read-only operations.

---

## Examples

### Agent Workflow: Fix Build Errors

```
1. Agent calls run(command="make")
   → Gets structured error list

2. Agent calls event(ref="1:1")
   → Gets full details for first error

3. Agent reads source file and makes fix

4. Agent calls run(command="make")
   → Verifies fix worked

5. Repeat until build passes
```

### Agent Workflow: Investigate Regression

```
1. Agent calls history(limit=10)
   → Sees run 5 failed, run 4 passed

2. Agent calls diff(run1=4, run2=5)
   → Gets list of new errors

3. Agent calls event(ref="5:1")
   → Investigates first new error

4. Agent correlates with recent code changes
```
