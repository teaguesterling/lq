# Integration Guide

This guide covers integrating lq with AI agents, CI/CD pipelines, and other tools.

## AI Agent Integration

lq is designed to work well with AI coding assistants like Claude, GPT, and others.

### Structured Output

Use `--json` for machine-readable output:

```bash
lq run --json --quiet make
```

Output:
```json
{
  "run_id": 1,
  "status": "FAIL",
  "exit_code": 2,
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

### Drill-Down Workflow

The structured output includes event references that agents can use to get more context:

```bash
# Agent runs build, gets error summary
lq run --json make

# Agent sees ref "1:1", gets details
lq event 1:1

# Agent needs more context
lq context 1:1 --lines 5
```

### Query for Analysis

Agents can query logs directly:

```bash
# Get all errors as JSON
lq q --json -f "severity='error'" build.log

# Count errors by file
lq sql "SELECT file_path, COUNT(*) as count
        FROM read_duck_hunt_log('build.log', 'auto')
        WHERE severity='error'
        GROUP BY 1
        ORDER BY 2 DESC"
```

### Markdown for Reports

For generating reports or PR comments:

```bash
lq run --markdown make
lq q --markdown -s file_path,line_number,message build.log
```

## CI/CD Integration

### GitHub Actions

```yaml
- name: Build with log capture
  run: |
    lq init
    lq run --json make > build_result.json
  continue-on-error: true

- name: Upload build results
  uses: actions/upload-artifact@v3
  with:
    name: build-logs
    path: |
      build_result.json
      .lq/logs/
```

### GitLab CI

```yaml
build:
  script:
    - lq init
    - lq run --json make | tee build_result.json
  artifacts:
    paths:
      - build_result.json
      - .lq/logs/
    when: always
```

### Jenkins

```groovy
pipeline {
    stages {
        stage('Build') {
            steps {
                sh 'lq init'
                sh 'lq run --json make > build_result.json || true'
                archiveArtifacts artifacts: 'build_result.json,.lq/logs/**'
            }
        }
    }
}
```

## Command Registry for CI

Register standard commands for consistent CI builds:

```bash
# Setup (in repo or CI init)
lq register build "make -j8" --description "Build project"
lq register test "pytest -v" --timeout 600
lq register lint "ruff check ." --format eslint

# CI script
lq run build
lq run test
lq run lint
```

Store `commands.yaml` in your repo for reproducibility.

## MCP Server Integration

lq is designed to work with MCP (Model Context Protocol) servers for AI agent access.

### With duckdb_mcp

```sql
-- Expose lq_events as a queryable resource
ATTACH ':memory:' AS lq_db;

-- Load lq schema
.read .lq/schema.sql

-- Publish as MCP tool
SELECT mcp_publish_tool(
    'lq_errors',
    'Get recent build errors',
    'SELECT * FROM lq_events WHERE severity = ''error'' ORDER BY run_id DESC LIMIT 20',
    '{}',
    '[]',
    'json'
);
```

### Future: lq serve

A dedicated MCP server for lq is planned:

```bash
lq serve --port 8080
```

This will expose:
- Query endpoints
- Event detail endpoints
- Log capture endpoints

## Shell Integration

### Bash Alias

```bash
# In ~/.bashrc
alias make='lq run make'
alias pytest='lq run pytest'
```

### Fish Function

```fish
function make --wraps make
    lq run make $argv
end
```

### Zsh Hook

```zsh
# Capture all failed commands
preexec() {
    if [[ $? -ne 0 ]]; then
        lq import /tmp/last_output.log --name "$1"
    fi
}
```

## Data Export

### Export to Parquet

The data is already in parquet format:

```bash
cp -r .lq/logs/ /path/to/export/
```

### Export to CSV

```bash
lq sql "COPY (SELECT * FROM lq_events) TO 'events.csv' (HEADER)"
```

### Export to JSON Lines

```bash
lq sql "COPY (SELECT * FROM lq_events) TO 'events.jsonl'"
```

## Programmatic Access

### Python

```python
import duckdb
from pathlib import Path

# Connect and load schema
conn = duckdb.connect(':memory:')
schema = Path('.lq/schema.sql').read_text()
for stmt in schema.split(';'):
    if stmt.strip():
        conn.execute(stmt)

# Query events
df = conn.execute("SELECT * FROM lq_events WHERE severity='error'").fetchdf()
```

### Direct Parquet Access

Any tool that reads parquet can access the data:

```python
import pandas as pd
df = pd.read_parquet('.lq/logs/')
```

```r
library(arrow)
df <- read_parquet('.lq/logs/')
```
