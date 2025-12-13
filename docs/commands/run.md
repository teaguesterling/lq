# lq run

Run a command and capture its output.

## Synopsis

```bash
lq run [OPTIONS] COMMAND [ARGS...]
```

## Description

The `run` command executes a shell command, captures its output, parses it for errors and warnings, and stores the events in `.lq/logs/`. It can also run registered commands by name.

## Options

| Option | Description |
|--------|-------------|
| `-n, --name NAME` | Source name (default: command name) |
| `-f, --format FORMAT` | Parse format hint (default: auto) |
| `-r, --keep-raw` | Keep raw output file in `.lq/raw/` |
| `-j, --json` | Output structured JSON result |
| `-m, --markdown` | Output markdown summary |
| `-q, --quiet` | Suppress streaming output |
| `-w, --include-warnings` | Include warnings in structured output |
| `--error-limit N` | Max errors/warnings in output (default: 20) |

## Examples

### Basic Usage

```bash
lq run make -j8
lq run pytest -v
lq run cargo build
```

### Named Run

```bash
lq run --name "nightly build" make -j8
```

### Keep Raw Log

```bash
lq run --keep-raw make
# Creates .lq/raw/001_make_103000.log
```

### Structured Output

For CI/CD or agent integration:

```bash
# JSON output
lq run --json make

# Markdown summary
lq run --markdown make

# Quiet mode (no streaming, just result)
lq run --quiet --json make
```

### Include Warnings

By default, structured output only includes errors. To include warnings:

```bash
lq run --json --include-warnings make
```

### Limit Output

```bash
lq run --json --error-limit 5 make
```

## Running Registered Commands

If you've registered commands with `lq register`, you can run them by name:

```bash
# Register
lq register build "make -j8"
lq register test "pytest -v"

# Run by name
lq run build
lq run test
```

## Structured Output Format

With `--json`, the output includes:

```json
{
  "run_id": 1,
  "command": "make -j8",
  "status": "FAIL",
  "exit_code": 2,
  "started_at": "2024-01-15T10:30:00",
  "completed_at": "2024-01-15T10:30:12",
  "duration_sec": 12.345,
  "summary": {
    "total_events": 5,
    "errors": 2,
    "warnings": 3
  },
  "errors": [
    {
      "ref": "1:1",
      "severity": "error",
      "file_path": "src/main.c",
      "line_number": 15,
      "column_number": 5,
      "message": "undefined variable 'foo'"
    }
  ]
}
```

With `--include-warnings`, a `warnings` array is also included.

### Event References

Each error/warning has a `ref` field (e.g., `1:1`) that can be used to get more details:

```bash
lq event 1:1
lq context 1:1
```

## Exit Code

`lq run` exits with the same exit code as the command it ran. This preserves the fail/pass semantics for CI/CD pipelines.

## See Also

- [register](register.md) - Register reusable commands
- [import](import.md) - Import existing log files
- [capture](capture.md) - Capture from stdin
- [event](event.md) - View event details
