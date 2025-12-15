# exec - Execute Ad-hoc Commands

Execute a shell command and capture its output.

## Synopsis

```bash
blq exec [OPTIONS] COMMAND [ARGS...]
blq e [OPTIONS] COMMAND [ARGS...]
```

## Description

The `exec` command executes a shell command directly, captures its output, parses it for errors and warnings, and stores the events in `.lq/logs/`.

Unlike `blq run`, which operates on registered commands, `exec` runs any shell command directly. This is useful for:

- One-off commands you don't want to register
- Testing commands before adding them to your workflow
- Quick debugging sessions

## Options

| Option | Short | Description |
|--------|-------|-------------|
| `--name NAME` | `-n` | Source name (default: derived from command) |
| `--format FORMAT` | `-f` | Parse format hint (default: auto) |
| `--keep-raw` | `-r` | Keep raw output file in `.lq/raw/` |
| `--json` | `-j` | Output structured JSON result |
| `--markdown` | `-m` | Output markdown summary |
| `--quiet` | `-q` | Suppress streaming output |
| `--summary` | `-s` | Show brief summary (errors/warnings count) |
| `--verbose` | `-v` | Show all blq status messages |
| `--include-warnings` | `-w` | Include warnings in structured output |
| `--error-limit N` | | Max errors/warnings in output (default: 20) |
| `--no-capture` | `-N` | Skip log capture, just run command |

## Examples

### Basic Usage

```bash
blq exec make -j8
blq exec pytest -v
blq exec cargo build
blq e npm test  # Short alias
```

### Named Execution

Give the run a meaningful name:

```bash
blq exec --name "nightly build" make -j8
```

### Keep Raw Log

```bash
blq exec --keep-raw make
# Creates .lq/raw/001_make_103000.log
```

### Structured Output

For CI/CD or agent integration:

```bash
# JSON output
blq exec --json make

# Markdown summary
blq exec --markdown make

# Quiet mode (no streaming, just result)
blq exec --quiet --json make
```

### Include Warnings

By default, structured output only includes errors. To include warnings:

```bash
blq exec --json --include-warnings make
```

### Limit Output

```bash
blq exec --json --error-limit 5 make
```

## Verbosity Control

By default, `blq exec` shows only the command's output. Use verbosity flags to control additional output:

### Default (quiet blq output)
```bash
blq exec make
# Shows only: command output + streaming stdout/stderr
```

### Summary Mode
```bash
blq exec --summary make
# Shows: command output + brief summary at end
# Output: ✓ make completed (0 errors, 2 warnings)
```

### Verbose Mode
```bash
blq exec --verbose make
# Shows: command output + all blq status messages
# Output includes: parsing progress, storage info, timing
```

## Skip Capture

For quick execution without log parsing:

```bash
blq exec --no-capture make clean
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
blq event 1:1
blq context 1:1
```

## Exit Code

`blq exec` exits with the same exit code as the command it ran. This preserves the fail/pass semantics for CI/CD pipelines.

## run vs exec

| Command | Purpose | Use When |
|---------|---------|----------|
| `blq run` | Execute registered commands | Running recurring build/test commands |
| `blq exec` | Execute ad-hoc commands | One-off commands, quick tests |

If you find yourself running the same `exec` command repeatedly, consider registering it:

```bash
# Instead of:
blq exec make -j8  # repeatedly

# Do this once:
blq register build "make -j8"

# Then use:
blq run build
```

## See Also

- [run](run.md) - Execute registered commands
- [registry](registry.md) - Register reusable commands
- [capture](capture.md) - Import log files or capture from stdin
- [errors](errors.md) - View errors and event details
