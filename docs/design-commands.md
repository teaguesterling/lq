# Command Registry Enhancements

Design for expanding lq's command registry to serve as a universal interface for CI, agents, and humans.

## Priority Order

1. **capture/no-capture runtime flag** - control log capture at runtime
2. **init enhancements** - `--project`, `--namespace`, `--detect`
3. **MCP register/unregister** - agent command management
4. ~~Template parameters~~ - deferred

---

## 1. Capture/No-Capture Flag

### Problem

Sometimes you want to just run a command without the overhead of log parsing and storage:
- Pre-commit hooks (fast feedback)
- CI steps where only exit code matters
- Quick local iteration

### Solution

Runtime flags with optional config default:

```bash
# Runtime control (default: capture)
lq run build              # captures logs
lq run --no-capture build # just execute, exit code only
lq run -n build           # short form
lq run --capture build    # explicit capture (override config)
```

Config default per-command:

```yaml
commands:
  build:
    cmd: "make -j8"
    capture: true         # default (can omit)

  format:
    cmd: "black ."
    capture: false        # default to no capture for this command
```

### Precedence

1. Runtime flag (`--capture` / `--no-capture`) - highest
2. Command config (`capture: false`)
3. Global default (`true`)

### Implementation

- Add `--capture` and `--no-capture` / `-n` flags to `lq run`
- Add `capture: bool` field to `RegisteredCommand`
- In `cmd_run()`, check flags → config → default
- When `capture=false`: run command, print output, return exit code (skip parsing/storage)

---

## 2. Init Enhancements

### 2a. Project/Namespace Override

```bash
lq init --project myapp --namespace myteam
```

Overrides auto-detection from git remote or filesystem path.

Stored in `.lq/config.yaml`:

```yaml
project:
  namespace: myteam
  project: myapp
```

### 2b. Command Auto-Detection

```bash
lq init --detect         # detect and prompt
lq init --detect --yes   # detect and auto-register
```

Detection rules:

| File | Commands |
|------|----------|
| `Makefile` | `build: make`, `test: make test` |
| `package.json` | `build: npm run build`, `test: npm test`, `lint: npm run lint` |
| `pyproject.toml` | `test: pytest`, `lint: ruff check .` |
| `Cargo.toml` | `build: cargo build`, `test: cargo test` |
| `go.mod` | `build: go build ./...`, `test: go test ./...` |
| `CMakeLists.txt` | `build: cmake --build .`, `test: ctest` |

Logic:
1. Scan for build system files
2. For each found, check if corresponding commands exist (e.g., `scripts.test` in package.json)
3. Present list to user or auto-register with `--yes`

### Combined Example

```bash
lq init --mcp --detect --project myapp --namespace myorg
```

Creates:
- `.lq/` directory with schema
- `.lq/config.yaml` with project info
- `.lq/commands.yaml` with detected commands
- `.mcp.json` for MCP server discovery

---

## 3. MCP Register/Unregister

### New Tools

**register_command**

```json
{
  "tool": "register_command",
  "arguments": {
    "name": "build",
    "cmd": "make -j8",
    "description": "Build the project",
    "timeout": 300,
    "capture": true,
    "force": false
  }
}
```

Returns:
```json
{
  "success": true,
  "message": "Registered command 'build': make -j8"
}
```

**unregister_command**

```json
{
  "tool": "unregister_command",
  "arguments": {
    "name": "build"
  }
}
```

**list_commands** (already exists as resource, maybe add as tool too)

### Use Cases

- Agent detects project type, registers appropriate commands
- Agent updates command after discovering better options
- Agent removes obsolete commands

---

## Future: Template Parameters (Deferred)

```yaml
commands:
  test:
    cmd: "pytest {file} -v"
    args:
      file:
        description: "Test file or directory"
        default: "."
```

Usage: `lq run test file=tests/unit/`

---

## Implementation Plan

### Phase 1: Capture Flag ✅
- [x] Add `capture` field to `RegisteredCommand`
- [x] Add `--capture` / `--no-capture` flags to `lq run`
- [x] Implement no-capture path in `cmd_run()`
- [x] Update tests

### Phase 2: Init Enhancements ✅
- [x] Add `--project` and `--namespace` args to `lq init`
- [x] Add `--detect` flag with detection logic
- [x] Add `--yes` flag for non-interactive mode
- [x] Detected: Makefile, yarn/npm, pyproject.toml, Cargo.toml, go.mod, CMakeLists.txt, configure, Gradle, Maven, Docker

### Phase 3: MCP Tools ✅
- [x] Add `register_command` tool
- [x] Add `unregister_command` tool
- [x] Add `list_commands` tool
- [x] Update docs
