# Getting Started

## Installation

### From PyPI

```bash
pip install lq
```

### From Source

```bash
git clone https://github.com/yourusername/lq.git
cd lq
pip install -e .
```

## Initialize Your Project

Run `lq init` in your project directory:

```bash
cd my-project
lq init
```

This creates a `.lq/` directory and installs the `duck_hunt` extension for log parsing.

```
Initialized .lq at /path/to/my-project/.lq
  logs/      - Hive-partitioned parquet files
  raw/       - Raw log files (optional)
  schema.sql - SQL schema and macros
  duck_hunt  - Installed successfully
```

## Your First Query

### Query a Log File Directly

If you have an existing log file:

```bash
lq q build.log
```

Select specific columns:

```bash
lq q -s file_path,line_number,severity,message build.log
```

Filter for errors:

```bash
lq f severity=error build.log
```

### Run and Capture

Run a command and capture its output:

```bash
lq run make -j8
```

This:
1. Runs `make -j8`
2. Parses the output for errors/warnings
3. Stores events in `.lq/logs/`
4. Prints a summary

### View Results

```bash
# Recent errors
lq errors

# All warnings
lq warnings

# Overall status
lq status
```

## Output Formats

### Default Table

```bash
lq q -s file_path,severity,message build.log
```

```
  file_path severity                  message
 src/main.c    error undefined variable 'foo'
src/utils.c    error        missing semicolon
```

### JSON

```bash
lq q --json build.log
```

```json
[
  {"file_path": "src/main.c", "severity": "error", "message": "undefined variable 'foo'"},
  {"file_path": "src/utils.c", "severity": "error", "message": "missing semicolon"}
]
```

### CSV

```bash
lq q --csv build.log
```

### Markdown

```bash
lq q --markdown build.log
```

## Next Steps

- [Commands Reference](commands/) - Learn all available commands
- [Query Guide](query-guide.md) - Master querying techniques
- [Integration Guide](integration.md) - Use with AI agents
