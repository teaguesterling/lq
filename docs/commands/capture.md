# Capturing Logs

blq provides multiple ways to capture log data: running commands, importing files, or reading from stdin.

## run - Run and Capture Commands

The primary way to capture logs. See [run command](run.md) for full details.

```bash
blq run make                  # Run and capture
blq run --json pytest         # Structured output
```

## import - Import Existing Log Files

Import a log file that was created outside of blq.

```bash
blq import build.log                    # Import with auto-detected format
blq import build.log --name mybuild     # Custom source name
blq import build.log --format gcc       # Specify log format
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--name NAME` | `-n` | Source name (default: filename) |
| `--format FORMAT` | `-f` | Parse format hint (default: auto) |

### Use Cases

**Importing CI artifacts:**
```bash
# Download and import CI build logs
curl -O https://ci.example.com/builds/123/log.txt
blq import log.txt --name ci-build-123
```

**Importing historical logs:**
```bash
# Import old build logs for analysis
blq import /var/log/builds/2024-01-15.log --name historical
```

**Batch import:**
```bash
# Import multiple log files
for f in logs/*.log; do
    blq import "$f" --name "$(basename "$f" .log)"
done
```

## capture - Capture from Stdin

Read log content from standard input. Useful for piping output from other commands.

```bash
# Pipe directly
make 2>&1 | blq capture --name build

# From a file via stdin
cat build.log | blq capture --name build

# With format hint
./my-tool 2>&1 | blq capture --name tool --format json
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--name NAME` | `-n` | Source name (default: stdin) |
| `--format FORMAT` | `-f` | Parse format hint (default: auto) |

### Use Cases

**Capturing from scripts:**
```bash
#!/bin/bash
# build.sh
(
    echo "Building project..."
    make -j8 2>&1
    echo "Running tests..."
    pytest 2>&1
) | blq capture --name full-build
```

**Capturing filtered output:**
```bash
# Only capture stderr
make 2>&1 1>/dev/null | blq capture --name errors-only

# Capture with grep filter
make 2>&1 | grep -E "error|warning" | blq capture --name filtered
```

**Capturing from remote:**
```bash
# Capture from SSH
ssh build-server "make -C /project" 2>&1 | blq capture --name remote-build
```

## Log Formats

blq uses the duck_hunt extension to parse 60+ log formats. Format is auto-detected by default.

### Common Formats

| Format | Description |
|--------|-------------|
| `auto` | Automatic detection (default) |
| `gcc` | GCC/Clang compiler output |
| `pytest` | pytest terminal output |
| `eslint` | ESLint JSON output |
| `tsc` | TypeScript compiler |
| `rustc` | Rust compiler |
| `go` | Go compiler/test output |
| `maven` | Maven build output |
| `gradle` | Gradle build output |

### Specifying Format

```bash
blq import build.log --format gcc
blq run --format pytest pytest
cat output.json | blq capture --format eslint
```

### When to Specify Format

Usually auto-detection works well. Specify format when:
- Auto-detection chooses wrong format
- You have custom or unusual log format
- You want to ensure consistent parsing

## Storage

All captured logs are stored as parquet files:

```
.lq/logs/
└── date=2024-01-15/
    └── source=build/
        └── 001_make_103000.parquet
```

Query captured data:
```bash
blq errors                    # View errors
blq q -f "severity='error'"   # Query with SQL
blq sql "SELECT * FROM lq_events WHERE source_name='build'"
```

## Comparison

| Method | Use Case |
|--------|----------|
| `blq run` | Running commands directly with capture |
| `blq import` | Importing existing log files |
| `blq capture` | Piping output from other sources |

Choose based on your workflow:
- **run** - Normal development workflow
- **import** - Analyzing historical or external logs
- **capture** - Integration with scripts or pipelines
