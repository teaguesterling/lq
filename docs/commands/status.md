# Status and History Commands

blq provides commands to monitor the state of your log captures and view run history.

## status - Current Source Status

Show a quick overview of all sources and their latest run status.

```bash
blq status                    # Quick status overview
blq status --verbose          # Detailed status
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Show detailed status with exit codes |

### Output

Default output shows status badges and counts:

```
              status  errors  warnings       age
[FAIL] build       3        5  00:05:32
[WARN] test        0       12  00:02:15
[ OK ] lint        0        0  00:10:45
```

Status badges:
- `[FAIL]` - Run had errors
- `[WARN]` - Run had warnings but no errors
- `[ OK ]` - Run completed cleanly
- `[ .. ]` - Run in progress (no completion time)

Verbose output (`--verbose`):

```
              status               summary        age  exit_code
[FAIL] build       3 errors, 5 warnings    5m ago          1
[WARN] test        0 errors, 12 warnings   2m ago          0
[ OK ] lint        0 errors, 0 warnings   10m ago          0
```

## history - Run History

Show the history of all captured runs.

```bash
blq history                   # Show recent runs
blq history -n 50             # Show last 50 runs
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--limit N` | `-n` | Maximum results (default: 20) |

### Output

```
   run_id  source_name  command        started_at           exit_code  error_count
        5  build        make -j8       2024-01-15 10:30:00          1            3
        4  test         pytest         2024-01-15 10:25:00          0            0
        3  lint         ruff check .   2024-01-15 10:20:00          0            0
        2  build        make -j8       2024-01-15 09:30:00          0            0
        1  build        make -j8       2024-01-15 09:00:00          1            5
```

## summary - Error/Warning Summary

Show aggregate statistics by tool and category.

```bash
blq summary                   # Summary across all runs
blq summary --latest          # Summary for latest run only
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--latest` | `-l` | Show summary for latest run only |

### Output

```
  tool_name    category  errors  warnings  total
       gcc   undefined       2         0      2
       gcc        type       1         3      4
       gcc      unused       0         5      5
    pytest  assertion        3         0      3
```

With `--latest`, only events from the most recent run are counted.

## Use Cases

### Checking Build Status

Quick check after a build:
```bash
blq run make && blq status
```

### Monitoring Multiple Sources

View status of all configured sources:
```bash
blq status
#        status  errors  warnings       age
# [FAIL] build       3        5  00:05:32
# [ OK ] test        0        0  00:02:15
# [ OK ] lint        0        0  00:10:45
```

### Finding Recurring Issues

Use summary to identify patterns:
```bash
blq summary
# Shows which tools/categories produce the most errors
```

### Comparing Runs

Check history to see trends:
```bash
blq history -n 10
# See if error counts are going up or down
```

## Integration with Workflows

### CI Scripts

```bash
#!/bin/bash
blq run make
if [ $? -ne 0 ]; then
    echo "Build failed. Summary:"
    blq summary --latest
    exit 1
fi
```

### Development Workflow

```bash
# Morning check: see what broke overnight
blq status

# After changes: run and check
blq run make
blq status
blq summary --latest
```

### Quick Health Check

```bash
# One-liner to check project health
blq status | grep -E '\[FAIL\]|\[WARN\]' && echo "Issues found" || echo "All clear"
```
