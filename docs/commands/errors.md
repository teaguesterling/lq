# Viewing Errors and Events

blq provides several commands for viewing and investigating errors, warnings, and specific events.

## errors - Show Recent Errors

Display recent errors from captured logs.

```bash
blq errors                    # Show last 10 errors
blq errors -n 20              # Show last 20 errors
blq errors -s build           # Filter by source name
blq errors --json             # Output as JSON
blq errors --compact          # Compact single-line format
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--source NAME` | `-s` | Filter by source name |
| `--limit N` | `-n` | Maximum results (default: 10) |
| `--compact` | `-c` | Compact single-line format |
| `--json` | `-j` | Output as JSON |

### Output Format

Default output shows detailed error information:

```
[1:3] src/main.c:42
  error: undefined reference to 'foo'

[1:5] src/utils.c:15
  error: implicit declaration of function 'bar'
```

Compact format (`--compact`):
```
1:3  src/main.c:42  undefined reference to 'foo'
1:5  src/utils.c:15  implicit declaration of function 'bar'
```

## warnings - Show Recent Warnings

Display recent warnings from captured logs.

```bash
blq warnings                  # Show last 10 warnings
blq warnings -n 20            # Show last 20 warnings
blq warnings -s test          # Filter by source
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--source NAME` | `-s` | Filter by source name |
| `--limit N` | `-n` | Maximum results (default: 10) |

## event - Show Event Details

Display detailed information about a specific event using its reference.

```bash
blq event 1:3                 # Show event 3 from run 1
blq event 1:3 --json          # Output as JSON
```

### Event References

Event references use the format `run_id:event_id`:
- `1:3` - Event 3 from run 1
- `5:12` - Event 12 from run 5

References are shown in error/warning output and structured JSON results.

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--json` | `-j` | Output as JSON |

### Output

```bash
$ blq event 1:3
Run: 1
Event: 3
Severity: error
File: src/main.c
Line: 42
Column: 10
Message: undefined reference to 'foo'
Error Code: E0001
```

## context - Show Log Context

Display the raw log lines surrounding an event. Useful for understanding the full context of an error.

```bash
blq context 1:3               # Show 3 lines before/after
blq context 1:3 -n 10         # Show 10 lines before/after
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--lines N` | `-n` | Context lines before/after (default: 3) |

### Output

```bash
$ blq context 1:3 -n 2
  40: int main() {
  41:     int x = 10;
> 42:     foo(x);  // ERROR HERE
  43:     return 0;
  44: }
```

The `>` marker indicates the line containing the event.

## Workflow Example

A typical debugging workflow:

```bash
# 1. Run a build and see errors
blq run make
# Output shows: Errors: 3

# 2. List all errors
blq errors
# Shows error references like 1:3, 1:5, 1:8

# 3. Get details on specific error
blq event 1:3

# 4. See surrounding log context
blq context 1:3 -n 5

# 5. Get JSON for programmatic processing
blq errors --json | jq '.[] | select(.file_path | contains("main"))'
```

## For AI Agents

When integrating with AI coding assistants:

```bash
# Get structured error data
blq run --json make

# Response includes event references:
# {
#   "errors": [
#     {"ref": "1:3", "file_path": "src/main.c", ...}
#   ]
# }

# Agent can then drill down:
blq event 1:3 --json
blq context 1:3 --lines 10
```

This workflow allows AI agents to:
1. Capture build output with `blq run --json`
2. Identify error locations from the structured response
3. Get full context with `blq context` for informed fixes
