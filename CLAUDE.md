# blq Development Notes

## Project Status

This is the initial scaffolding for `blq` (Build Log Query) - a CLI tool for capturing, storing, and querying build/test logs using DuckDB.

### Completed
- Python package structure with pyproject.toml (hatchling build)
- CLI module (`src/blq/cli.py`) with all core commands
- SQL schema with table-returning macros (`src/blq/schema.sql`)
- blq.duckdb database file with pre-loaded macros
- Hive-partitioned parquet storage design with zstd compression
- Basic error/warning parsing fallback
- Integration hooks for duck_hunt extension
- Pythonic query API (`LogQuery`, `LogStore`, `LogQueryGrouped`)
- Structured output (JSON, Markdown, CSV)
- Command registry for reusable build/test commands
- Query and filter commands for direct log file inspection
- MCP server (`blq serve`) for AI agent integration
- Run metadata capture (environment, git, system, CI context)
- Project detection from git remote or filesystem path
- Command auto-detection from build files (`blq init --detect`)
- Capture/no-capture mode for fast execution (`blq run --no-capture`)
- Ad-hoc command execution (`blq exec`) - run without registry
- Shell completions for bash, zsh, fish (`blq completions`)
- List available log formats (`blq formats`)
- Version flag (`blq --version`)
- 234 unit tests
- Comprehensive documentation (README, docs/)

### TODO
- [ ] Implement sync feature (see `docs/design-sync.md`)
- [ ] Consider integration with duckdb_mcp for ATTACH/DETACH workflow

## Architecture

```
blq (Python CLI)
    │
    ├── .lq/blq.duckdb     - Database with pre-loaded macros (blq_*)
    │
    ├── .lq/logs/          - Hive-partitioned parquet files (zstd compressed)
    │   └── date=.../source=.../
    │
    ├── Uses duckdb Python API directly
    │
    └── Optionally uses duck_hunt extension for 60+ format parsing
```

### SQL Schema (blq_ prefix)

All SQL macros use the `blq_` prefix:

| Macro | Description |
|-------|-------------|
| `blq_load_events()` | Load all events from parquet files |
| `blq_load_runs()` | Aggregated run statistics |
| `blq_status()` | Quick status overview |
| `blq_errors(n)` | Recent errors (default: 10) |
| `blq_warnings(n)` | Recent warnings (default: 10) |
| `blq_history(n)` | Run history (default: 20) |
| `blq_diff(run1, run2)` | Compare two runs |

Direct DuckDB access:
```bash
duckdb .lq/blq.duckdb "SELECT * FROM blq_status()"
```

## Run Metadata

Each `blq run` captures comprehensive execution context:

| Field | Type | Description |
|-------|------|-------------|
| `cwd` | VARCHAR | Working directory |
| `executable_path` | VARCHAR | Full path to command executable |
| `environment` | MAP(VARCHAR, VARCHAR) | Captured env vars (configurable) |
| `hostname` | VARCHAR | Machine hostname |
| `platform` | VARCHAR | OS (Linux, Darwin, Windows) |
| `arch` | VARCHAR | Architecture (x86_64, arm64) |
| `git_commit` | VARCHAR | HEAD SHA |
| `git_branch` | VARCHAR | Current branch |
| `git_dirty` | BOOLEAN | Uncommitted changes present |
| `ci` | MAP(VARCHAR, VARCHAR) | CI provider + context (auto-detected) |

### Environment Capture

Configurable in `.lq/config.yaml`:
```yaml
capture_env:
  - PATH
  - VIRTUAL_ENV
  - CC
  - CXX
  # ... (30+ defaults)
```

Per-command overrides in `commands.yaml`:
```yaml
commands:
  build:
    cmd: "make -j8"
    capture_env:
      - EXTRA_VAR
```

### CI Auto-Detection

Supports: GitHub Actions, GitLab CI, Jenkins, CircleCI, Travis CI, Buildkite, Azure Pipelines

```sql
SELECT ci['provider'], ci['run_id'] FROM blq_load_events() WHERE ci IS NOT NULL
```

## Project Identification

Detected at `blq init` and stored in `.lq/config.yaml`:

```yaml
project:
  namespace: teaguesterling  # from git remote owner
  project: blq               # from git remote repo
```

Fallback for non-git projects uses filesystem path:
- `/home/user/Projects/myapp` → `namespace=home__user__Projects, project=myapp`

## Command Auto-Detection

`blq init --detect` scans for build system files and registers appropriate commands:

| File | Commands |
|------|----------|
| `Makefile` | build, test, clean |
| `yarn.lock` | build, test, lint (yarn, if scripts exist) |
| `package.json` | build, test, lint (npm, if scripts exist) |
| `pyproject.toml` | test (pytest), lint (ruff) |
| `Cargo.toml` | build, test |
| `go.mod` | build, test |
| `CMakeLists.txt` | build, test |
| `configure` | configure |
| `configure.ac` | autoreconf |
| `build.gradle` | build, test, clean (gradlew) |
| `pom.xml` | build, test, clean (mvn) |
| `Dockerfile` | docker-build |
| `docker-compose.yml` | docker-up, docker-build |

Commands can have `capture: false` for fast execution without log parsing:
```yaml
commands:
  format:
    cmd: "black ."
    capture: false  # Skip log capture
```

Runtime override: `blq run --no-capture <cmd>` or `blq run --capture <cmd>`

## Key Design Decisions

1. **Parquet over DuckDB files**: Enables concurrent writes without locking
2. **Hive partitioning**: Efficient date/source-based queries
3. **Project-local storage**: `.lq/` directory in project root
4. **blq.duckdb for macros**: Pre-loaded SQL macros for faster startup and direct CLI access
5. **Table-returning macros**: `blq_load_events()` evaluated at query time, not view creation
6. **Optional duck_hunt**: Works with basic parsing if extension not available
7. **Python duckdb API**: No subprocess calls to duckdb CLI
8. **MAP for variable data**: Environment and CI use MAP(VARCHAR, VARCHAR) for flexible keys
9. **zstd compression**: Parquet files use zstd level 3 for ~40% smaller files than snappy

## Integration Points

- **duck_hunt extension**: For enhanced log parsing (60+ formats)
- **duckdb_mcp**: For MCP server integration (agents can query logs)

## Related Projects

- `../duck_hunt/` - DuckDB extension for log parsing
- `../duckdb_mcp/` - MCP server extension for DuckDB
