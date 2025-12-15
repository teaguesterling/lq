# Command Registry

blq maintains a registry of named commands that can be executed with `blq run`. This provides consistent command execution across your project.

## commands - List Registered Commands

Show all registered commands.

```bash
blq commands                  # Table format
blq commands --json           # JSON format
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--json` | `-j` | Output as JSON |

### Output

```
Name            Command                                  Capture  Description
--------------------------------------------------------------------------------
build           make -j8                                 yes      Build project
test            pytest                                   yes      Run tests
lint            ruff check .                             yes      Check code style
format          black .                                  no       Format code
```

## register - Register a Command

Add a new command to the registry.

```bash
blq register build "make -j8"
blq register test pytest --description "Run unit tests"
blq register format "black ." --no-capture
```

### Arguments

| Argument | Description |
|----------|-------------|
| `name` | Command name (e.g., 'build', 'test') |
| `cmd` | Command to run (can be multiple words) |

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--description TEXT` | `-d` | Command description |
| `--timeout SECONDS` | `-t` | Timeout in seconds (default: 300) |
| `--format FORMAT` | `-f` | Log format hint for parsing |
| `--no-capture` | `-N` | Don't capture logs by default |
| `--force` | | Overwrite existing command |

### Examples

**Basic registration:**
```bash
blq register build "make -j8"
blq register test "pytest -v"
```

**With description:**
```bash
blq register build "make -j8" --description "Build with parallelism"
```

**With format hint:**
```bash
blq register test "pytest" --format pytest
blq register lint "eslint ." --format eslint
```

**No capture mode:**
```bash
blq register format "black ." --no-capture
blq register clean "make clean" --no-capture
```

**Update existing:**
```bash
blq register build "make -j16" --force
```

### Capture Mode

By default, commands capture and parse output. Use `--no-capture` for commands where:
- Output parsing isn't useful (formatters, cleaners)
- Speed is critical
- You just want to run the command

At runtime, you can override with:
```bash
blq run --capture format      # Force capture
blq run --no-capture build    # Skip capture
```

## unregister - Remove a Command

Remove a command from the registry.

```bash
blq unregister <name>
```

### Example

```bash
blq unregister old-command
# Output: Unregistered command 'old-command'
```

## Storage

Commands are stored in `.lq/commands.yaml`:

```yaml
commands:
  build:
    cmd: make -j8
    description: Build project
    timeout: 300
    capture: true
  test:
    cmd: pytest
    description: Run tests
    timeout: 300
    format: pytest
    capture: true
  format:
    cmd: black .
    description: Format code
    capture: false
```

You can edit this file directly for bulk changes.

## Auto-Detection

Use `blq init --detect` to auto-register commands based on your project's build files:

```bash
blq init --detect --yes       # Auto-confirm all detected commands
blq init --detect             # Interactive confirmation
```

See [init command](init.md) for detection modes and supported build systems.

## Use Cases

### Project Setup

```bash
# Initialize with auto-detected commands
blq init --detect --yes

# Add project-specific commands
blq register integration-test "pytest tests/integration" -d "Integration tests"
blq register deploy "./scripts/deploy.sh" --no-capture
```

### Team Standardization

Share `.lq/commands.yaml` in version control:
```bash
# Team member clones repo
git clone https://github.com/team/project
cd project
blq init  # Commands already configured

# Everyone uses the same commands
blq run build
blq run test
```

### CI Integration

```yaml
# .github/workflows/ci.yml
jobs:
  build:
    steps:
      - uses: actions/checkout@v4
      - run: pip install blq-cli
      - run: blq init
      - run: blq run build
      - run: blq run test
```

### Multiple Build Configurations

```bash
blq register build-debug "make DEBUG=1"
blq register build-release "make RELEASE=1"
blq register build-sanitize "make SANITIZE=1"

# Run specific configuration
blq run build-debug
blq run build-release
```

## Best Practices

1. **Descriptive names**: Use clear, action-oriented names
2. **Add descriptions**: Help team members understand each command
3. **Set appropriate timeouts**: Increase for long builds
4. **Use format hints**: When auto-detection struggles
5. **Skip capture for speed**: Use `--no-capture` for formatters and cleaners
6. **Version control**: Commit `.lq/commands.yaml` for team consistency
