# BIRD Migration Plan for blq

This document describes the plan to migrate blq from its current storage model to the BIRD (Buffer and Invocation Record Database) specification.

## Current State

### blq Storage Model

```
.lq/
├── blq.duckdb                  # Database with macros
├── logs/                       # Hive-partitioned parquet files
│   └── date=YYYY-MM-DD/
│       └── source=run|import|capture/
│           └── {run_id}_{name}_{timestamp}.parquet
└── raw/                        # Optional: raw log files
```

### Current Schema (Single Events Table)

```sql
-- All data in one events table (flattened)
events (
    run_id, event_id, source_name, source_type, command,
    started_at, completed_at, exit_code,
    severity, message, file_path, line_number, column_number,
    tool_name, category, code, rule, fingerprint,
    log_line_start, log_line_end, context, metadata,
    -- Run-level metadata mixed in
    cwd, environment, hostname, platform, arch,
    git_commit, git_branch, git_dirty, ci
)
```

**Limitations:**
- No separation between command metadata and parsed events
- Raw output not stored (only parsed events)
- No content-addressed deduplication
- No storage tiers (hot/cold)

---

## Target State: BIRD Specification

### BIRD Storage Model

```
$BIRD_ROOT/                          # Default: .lq/ (or ~/.local/share/bird)
├── db/
│   ├── bird.duckdb                  # Pre-configured views and macros
│   ├── data/
│   │   ├── recent/                  # Last 14 days (hot data)
│   │   │   ├── commands/
│   │   │   │   └── date=YYYY-MM-DD/
│   │   │   │       └── <session>--<exec>--<uuid>.parquet
│   │   │   ├── outputs/
│   │   │   │   └── date=YYYY-MM-DD/
│   │   │   │       └── <session>--<exec>--<uuid>.parquet
│   │   │   ├── events/              # Parsed events (new)
│   │   │   │   └── date=YYYY-MM-DD/
│   │   │   │       └── <session>--<exec>--<uuid>.parquet
│   │   │   └── blobs/
│   │   │       └── content/         # Content-addressed pool
│   │   │           ├── ab/
│   │   │           │   └── abc123def456...789.bin.gz
│   │   │           └── ...          # 256 subdirs (00-ff)
│   │   └── archive/                 # >14 days (cold data)
│   │       └── by-week/
│   │           ├── commands/
│   │           ├── outputs/
│   │           ├── events/
│   │           └── blobs/
│   └── sql/
│       ├── init.sql
│       ├── views.sql
│       └── macros.sql
├── config.toml                      # Configuration (replaces config.yaml)
└── errors.log                       # Capture error log
```

### BIRD Schema (Separate Tables)

```sql
-- Commands table (one row per command execution)
CREATE TABLE commands (
    id                UUID PRIMARY KEY,      -- UUIDv7
    session_id        TEXT NOT NULL,
    timestamp         TIMESTAMP NOT NULL,
    duration_ms       BIGINT,
    cwd               TEXT NOT NULL,
    env_hash          TEXT,
    cmd               TEXT NOT NULL,
    executable        TEXT,
    args              TEXT[],
    exit_code         INT NOT NULL,
    format_hint       TEXT,
    stdout_file       TEXT,
    stderr_file       TEXT,
    has_stdout        BOOLEAN DEFAULT FALSE,
    has_stderr        BOOLEAN DEFAULT FALSE,
    client_id         TEXT NOT NULL,
    hostname          TEXT,
    username          TEXT,
    -- blq additions
    source_name       TEXT,                  -- Registered command name
    source_type       TEXT,                  -- 'run', 'exec', 'import', 'capture'
    environment       MAP(VARCHAR, VARCHAR),
    platform          TEXT,
    arch              TEXT,
    git_commit        TEXT,
    git_branch        TEXT,
    git_dirty         BOOLEAN,
    ci                MAP(VARCHAR, VARCHAR),
    date              DATE GENERATED ALWAYS AS (CAST(timestamp AS DATE))
);

-- Outputs table (stdout/stderr per command)
CREATE TABLE outputs (
    id                UUID PRIMARY KEY,
    command_id        UUID NOT NULL,         -- References commands.id
    content_hash      TEXT NOT NULL,         -- BLAKE3 hash (hex)
    byte_length       BIGINT NOT NULL,
    storage_type      TEXT NOT NULL,         -- 'inline', 'blob', 'archive'
    storage_ref       TEXT NOT NULL,         -- URI to content
    stream            TEXT NOT NULL,         -- 'stdout' or 'stderr'
    content_type      TEXT,
    encoding          TEXT DEFAULT 'utf-8',
    compressed        BOOLEAN DEFAULT FALSE,
    truncated         BOOLEAN DEFAULT FALSE,
    date              DATE NOT NULL
);

-- Events table (parsed errors/warnings)
CREATE TABLE events (
    id                UUID PRIMARY KEY,
    command_id        UUID NOT NULL,         -- References commands.id
    event_index       INTEGER NOT NULL,      -- Index within command
    severity          TEXT NOT NULL,         -- 'error', 'warning', 'info', 'note'
    message           TEXT,
    file_path         TEXT,
    line_number       INTEGER,
    column_number     INTEGER,
    tool_name         TEXT,
    category          TEXT,
    code              TEXT,
    rule              TEXT,
    fingerprint       TEXT,
    log_line_start    INTEGER,
    log_line_end      INTEGER,
    context           TEXT,
    metadata          JSON,
    date              DATE NOT NULL
);

-- Blob registry (for content-addressed storage)
CREATE TABLE blob_registry (
    content_hash      TEXT PRIMARY KEY,
    byte_length       BIGINT NOT NULL,
    compression       TEXT DEFAULT 'gzip',
    ref_count         INT DEFAULT 0,
    first_seen        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    storage_tier      TEXT,                  -- 'recent', 'archive'
    storage_path      TEXT,
    verified_at       TIMESTAMP,
    corrupt           BOOLEAN DEFAULT FALSE
);
```

---

## Migration Phases

### Phase 0: Preparation (Non-Breaking)

**Goal:** Prepare codebase for BIRD without changing storage format.

1. **Add BLAKE3 dependency**
   - Add `blake3` to pyproject.toml dependencies
   - Create `src/blq/hashing.py` with hash utilities

2. **Add UUID generation**
   - Add UUIDv7 generation (timestamp-ordered UUIDs)
   - Create `src/blq/ids.py` with ID utilities

3. **Refactor storage module**
   - Extract storage logic from `LogStore` into `src/blq/storage/`
   - Create abstract `StorageBackend` interface
   - Implement `LegacyStorage` for current format

4. **Add configuration for BIRD**
   - Add BIRD-related config options (disabled by default)
   - `storage_version: "v1"` (current) vs `"bird"` (new)

**Files to create:**
- `src/blq/hashing.py`
- `src/blq/ids.py`
- `src/blq/storage/__init__.py`
- `src/blq/storage/base.py`
- `src/blq/storage/legacy.py`

---

### Phase 1: BIRD Schema Implementation

**Goal:** Implement BIRD tables alongside existing storage.

1. **Create BIRD schema SQL**
   - `src/blq/bird_schema.sql` with commands, outputs, events tables
   - Update macros to support both schemas

2. **Implement BirdStorage class**
   - `src/blq/storage/bird.py`
   - Write to separate commands/outputs/events tables
   - Support inline storage for small outputs (<1MB)

3. **Update LogStore**
   - Add `storage_backend` attribute
   - Route writes through storage backend
   - Keep reads backward-compatible

4. **Update CLI commands**
   - `blq init --bird` to initialize BIRD storage
   - `blq status` to detect storage version
   - Keep all commands working with both formats

**Files to create:**
- `src/blq/bird_schema.sql`
- `src/blq/storage/bird.py`

**Files to modify:**
- `src/blq/query.py` (LogStore)
- `src/blq/commands/init_cmd.py`

---

### Phase 2: Content-Addressed Blobs

**Goal:** Add deduplication for large outputs.

1. **Implement blob storage**
   - `src/blq/storage/blobs.py`
   - Content-addressed writes with BLAKE3 hash
   - Subdirectory sharding (256 dirs)
   - Atomic writes with race condition handling

2. **Add blob registry**
   - Create blob_registry table in bird.duckdb
   - Implement ref_count tracking
   - Add dedup check on capture

3. **Update output storage**
   - Route outputs ≥1MB to blob storage
   - Store `content_hash`, `storage_type`, `storage_ref`
   - Support reading from blob storage

4. **Add blob CLI commands**
   - `blq blob stats` - Show deduplication statistics
   - `blq blob verify` - Verify blob integrity

**Files to create:**
- `src/blq/storage/blobs.py`
- `src/blq/commands/blob_cmd.py`

**Files to modify:**
- `src/blq/storage/bird.py`
- `src/blq/cli.py`

---

### Phase 3: Storage Tiers

**Goal:** Implement hot/cold storage tiers.

1. **Implement recent tier**
   - Date-partitioned parquet in `recent/`
   - 14-day retention (configurable)
   - Optimized for fast access

2. **Implement archive tier**
   - Week-partitioned parquet in `archive/by-week/`
   - Global blob pool (no date partitioning for blobs)
   - Compressed for storage efficiency

3. **Add archival command**
   - `blq archive` - Move old data to archive tier
   - `blq archive --days 14` - Archive data older than N days
   - Move parquets by date, blobs by reference

4. **Update queries**
   - Union recent + archive for full queries
   - Optimize queries to use recent tier first

**Files to modify:**
- `src/blq/storage/bird.py`
- `src/blq/bird_schema.sql`
- `src/blq/commands/management.py`

---

### Phase 4: Compaction

**Goal:** Implement parquet file compaction.

1. **Implement compaction logic**
   - `src/blq/storage/compact.py`
   - Merge small parquet files into larger ones
   - Session-based grouping for efficient compaction
   - Lock-based coordination for concurrent access

2. **Add compaction command**
   - `blq compact` - Compact parquet files
   - `blq compact --today` - Fast compaction for current day
   - `blq compact --threshold 50` - Configure file threshold

3. **Update naming convention**
   - `<session>--<exec>--<uuid>.parquet` for individual files
   - `<session>--__compacted-N__--<uuid>.parquet` for compacted

**Files to create:**
- `src/blq/storage/compact.py`

**Files to modify:**
- `src/blq/commands/management.py`
- `src/blq/cli.py`

---

### Phase 5: Migration Tooling

**Goal:** Tools to migrate existing blq data to BIRD format.

1. **Implement migration command**
   - `blq migrate --to bird` - Migrate to BIRD format
   - `blq migrate --dry-run` - Preview migration
   - Preserve all existing data

2. **Migration process:**
   ```
   For each existing parquet file:
   1. Read events
   2. Group by run_id
   3. Create command record (extract run metadata)
   4. Create output record (if raw output available)
   5. Create event records (parsed events)
   6. Write to BIRD tables
   7. Mark original as migrated
   ```

3. **Rollback support**
   - Keep original files until migration confirmed
   - `blq migrate --rollback` to revert

**Files to create:**
- `src/blq/commands/migrate_cmd.py`

---

### Phase 6: duck_hunt Integration (Optional)

**Goal:** Enhanced log parsing via duck_hunt extension.

1. **Detect duck_hunt availability**
   - Check if duck_hunt extension installed
   - Graceful fallback to basic parsing

2. **Use duck_hunt for parsing**
   - Call `read_duck_hunt_log()` for supported formats
   - Map duck_hunt events to blq event schema

3. **Format detection**
   - Use duck_hunt's format detection
   - Fall back to blq's `detect_format_from_command()`

**Files to modify:**
- `src/blq/commands/events.py`
- `src/blq/storage/bird.py`

---

## Implementation Details

### Storage Type Routing

```python
def store_output(self, command_id: UUID, stream: str, data: bytes) -> OutputRecord:
    # Compute hash
    content_hash = blake3.hash(data).hexdigest()

    # Size-based routing
    if len(data) < self.config.max_inline_bytes:  # Default: 1MB
        # Small: inline with data: URI
        storage_type = "inline"
        b64 = base64.b64encode(data).decode()
        storage_ref = f"data:application/octet-stream;base64,{b64}"
    else:
        # Large: content-addressed blob
        existing = self.check_blob_exists(content_hash)
        if existing:
            # Dedup hit
            self.increment_ref_count(content_hash)
            storage_type = "blob"
            storage_ref = f"file://{existing}"
        else:
            # Dedup miss - write new blob
            path = self.write_blob(content_hash, data)
            self.register_blob(content_hash, len(data), path)
            storage_type = "blob"
            storage_ref = f"file://{path}"

    return OutputRecord(
        id=generate_uuid(),
        command_id=command_id,
        content_hash=content_hash,
        byte_length=len(data),
        storage_type=storage_type,
        storage_ref=storage_ref,
        stream=stream,
    )
```

### Blob Path Convention

```python
def blob_path(content_hash: str) -> Path:
    """Generate content-addressed blob path.

    Example: abc123def456... -> recent/blobs/content/ab/abc123def456.bin.gz
    """
    subdir = content_hash[:2]  # First 2 hex chars
    filename = f"{content_hash}.bin.gz"
    return Path("recent/blobs/content") / subdir / filename
```

### Atomic Blob Writes

```python
def write_blob(self, content_hash: str, data: bytes) -> str:
    path = self.bird_root / blob_path(content_hash)
    path.parent.mkdir(parents=True, exist_ok=True)

    temp = path.parent / f".tmp.{content_hash}.bin.gz"

    # Compress and write to temp file
    with gzip.open(temp, "wb") as f:
        f.write(data)

    # Atomic rename (handles race conditions)
    try:
        temp.rename(path)
    except FileExistsError:
        # Another process wrote same hash - that's fine!
        temp.unlink()

    return str(path.relative_to(self.bird_root / "db/data"))
```

---

## Schema Mapping

### Legacy Events → BIRD Commands

| Legacy Field | BIRD Command Field | Notes |
|--------------|-------------------|-------|
| run_id | id | Convert to UUID |
| source_name | source_name | Direct mapping |
| source_type | source_type | Direct mapping |
| command | cmd | Direct mapping |
| started_at | timestamp | Direct mapping |
| exit_code | exit_code | Direct mapping |
| cwd | cwd | Direct mapping |
| environment | environment | Direct mapping |
| hostname | hostname | Direct mapping |
| platform | platform | Direct mapping |
| arch | arch | Direct mapping |
| git_commit | git_commit | Direct mapping |
| git_branch | git_branch | Direct mapping |
| git_dirty | git_dirty | Direct mapping |
| ci | ci | Direct mapping |
| - | session_id | Generate from run context |
| - | client_id | Generate from hostname |
| - | duration_ms | Calculate from started_at/completed_at |
| - | executable | Extract from command |
| - | format_hint | Use detected format |

### Legacy Events → BIRD Events

| Legacy Field | BIRD Event Field | Notes |
|--------------|-----------------|-------|
| event_id | event_index | Direct mapping |
| severity | severity | Direct mapping |
| message | message | Direct mapping |
| file_path | file_path | Direct mapping |
| line_number | line_number | Direct mapping |
| column_number | column_number | Direct mapping |
| tool_name | tool_name | Direct mapping |
| category | category | Direct mapping |
| code | code | Direct mapping |
| rule | rule | Direct mapping |
| fingerprint | fingerprint | Direct mapping |
| log_line_start | log_line_start | Direct mapping |
| log_line_end | log_line_end | Direct mapping |
| context | context | Direct mapping |
| metadata | metadata | Direct mapping |
| run_id | command_id | Reference to command |

---

## Backward Compatibility

### During Migration

1. **Detect storage version on init**
   ```python
   def detect_storage_version(root: Path) -> str:
       if (root / "db/data/recent/commands").exists():
           return "bird"
       elif (root / "logs").exists():
           return "v1"
       else:
           return "none"
   ```

2. **Support both read paths**
   - BIRD queries work on new schema
   - Legacy queries work on old schema
   - Union queries combine both during transition

3. **Gradual migration**
   - New writes go to BIRD format
   - Old data remains readable
   - Migration command moves old → new

### After Migration

1. **Remove legacy code** (future release)
   - Drop LegacyStorage class
   - Remove v1 schema support
   - Clean up migration tooling

---

## Configuration

### New config.toml Options

```toml
[storage]
version = "bird"                  # "v1" or "bird"
max_inline_bytes = 1048576        # 1MB threshold
compression = "gzip"              # gzip, zstd
compression_level = 6

[deduplication]
enabled = true
hash_algorithm = "blake3"

[tiers]
recent_days = 14                  # Days before archival
auto_archive = false              # Auto-archive on write

[compaction]
enabled = true
threshold = 50                    # Files per partition
auto_compact = true               # Auto-compact on write

[garbage_collection]
enabled = false                   # MVP: never delete
strategy = "ref_counting"
grace_period_days = 30
```

---

## Testing Strategy

### Unit Tests

1. **Blob storage tests**
   - Content-addressed writes
   - Deduplication verification
   - Concurrent write handling
   - Hash collision detection (should never occur)

2. **Schema tests**
   - Commands/outputs/events separation
   - Foreign key relationships
   - Partition handling

3. **Migration tests**
   - Legacy → BIRD conversion
   - Data integrity verification
   - Rollback functionality

### Integration Tests

1. **End-to-end capture**
   - Run command → BIRD storage
   - Verify all tables populated
   - Verify blob deduplication

2. **Query compatibility**
   - All existing queries work
   - New queries use BIRD schema
   - Performance comparison

### Performance Tests

1. **Deduplication ratio**
   - Measure storage savings
   - Compare hash + lookup overhead

2. **Query performance**
   - Recent vs archive tiers
   - Compacted vs non-compacted

---

## Estimated Effort

| Phase | Description | Effort |
|-------|-------------|--------|
| 0 | Preparation | 1-2 days |
| 1 | BIRD Schema | 2-3 days |
| 2 | Content-Addressed Blobs | 2-3 days |
| 3 | Storage Tiers | 2-3 days |
| 4 | Compaction | 2-3 days |
| 5 | Migration Tooling | 2-3 days |
| 6 | duck_hunt Integration | 1-2 days |
| - | Testing & Documentation | 2-3 days |
| **Total** | | **15-22 days** |

---

## Design Decisions (Resolved)

### 1. Raw Output Storage: YES

Store raw output in blobs according to BIRD spec, with retrieval capability.

**Rationale:**
- Enables re-parsing with different/updated parsers
- Supports debugging when parsed events are insufficient
- Content-addressing minimizes storage cost (70-90% dedup)

**Implementation:**
- Capture stdout/stderr during `blq run` and `blq exec`
- Store in outputs table with content-addressed blobs
- Add `blq show <run_id>` to retrieve raw output

### 2. Session ID: Deterministic Path+Date

Session ID should be deterministic and configurable, defaulting to project path + date.

**Rationale:**
- blq sessions are logical (many CLI invocations over a day)
- Different from shq where session = shell process
- MCP sessions may need different strategy

**Format:**
```
{project_name}-{date}

Examples:
- blq-2024-12-30
- myproject-2024-12-30
```

**Configuration:**
```toml
[session]
# Strategy: "path_date" (default), "uuid", "custom"
strategy = "path_date"
# Custom prefix (optional)
prefix = ""
```

**MCP sessions:** Use `blq-mcp-{date}` or per-conversation UUID.

### 3. Client ID: Tool-Specific

Client ID identifies the blq client type:

| Context | Client ID |
|---------|-----------|
| CLI shell | `blq-shell` |
| MCP server | `blq-mcp` |
| Import | `blq-import` |
| Capture (stdin) | `blq-capture` |

**Format:** `blq-{context}`

This enables queries like:
```sql
SELECT * FROM commands WHERE client_id = 'blq-mcp'
```

### 4. duck_hunt Integration: YES

duck_hunt is now stable. Integrate for enhanced parsing.

**Implementation:**
- Check for duck_hunt extension on startup
- Use `read_duck_hunt_log()` for supported formats
- Graceful fallback to basic parsing if not available
- Store `format_hint` with commands for re-parsing

### 5. Storage Location: Project-Local First

Start with `.lq/` for project-local storage. Plan for BIRD remotes later.

**Phase 1 (Now):**
- `.lq/` directory in project root
- BIRD-compatible structure within `.lq/`

**Phase 2 (Future):**
- `blq sync` command for remote BIRD storage
- Support for `~/.local/share/bird` as global fallback
- See `docs/design-sync.md` for remote sync design

---

## Next Steps

1. **Review plan** - Any additional feedback?
2. **Prioritize phases** - Ship incrementally or batch?
3. **Begin Phase 0** - Add BLAKE3, UUIDs, storage abstraction
4. **Decide on raw output** - Capture immediately or defer?

---

## Critique & Revisions (v2)

Based on review, several issues were identified and resolved:

### Issue 1: 3 Files Per Run = Overhead (RESOLVED: Use DuckDB Directly)

**Problem:** BIRD's parquet-based approach means 3 files per run, complex globs, compaction.

**Resolution:** Use a **single DuckDB database file** instead of parquet files.

**Why this works for blq:**
- blq has sequential CLI invocations (not concurrent shell hooks)
- Connect → write → disconnect is fast (<50ms)
- DuckDB handles storage/compaction internally
- No file proliferation, no glob patterns, no manual compaction

**Storage model:**
```
.lq/
├── blq.duckdb              # Single database with all tables
├── blobs/                  # Content-addressed output storage
│   └── content/
│       ├── ab/
│       │   └── abc123...bin.gz
│       └── ...
└── config.yaml
```

**Schema (same BIRD tables, in DuckDB):**
```sql
-- All in blq.duckdb
CREATE TABLE commands (...);     -- BIRD-compatible schema
CREATE TABLE outputs (...);      -- References blobs/
CREATE TABLE events (...);       -- Parsed errors/warnings
CREATE TABLE blob_registry (...); -- Dedup tracking
```

**Comparison:**

| Aspect | Parquet (shq) | DuckDB (blq) |
|--------|---------------|--------------|
| Concurrency | Many writers | Single writer |
| Files | Many parquet | One .duckdb |
| Compaction | Manual | Automatic |
| Queries | Glob patterns | Direct SQL |
| Archive | Move parquets | Export/ATTACH |
| Use case | Shell hooks | CLI tool |

**BIRD compatibility:**
- Same table schemas as BIRD spec
- Can export to parquet for BIRD consumers
- Can ATTACH other BIRD databases for queries
- Blobs stored externally (same as BIRD)

**Connection management (single-writer model):**

```python
class BirdStore:
    def __init__(self, root: Path):
        self.db_path = root / "blq.duckdb"
        self.blobs = root / "blobs" / "content"
        # No persistent connection - open on demand

    def save_run(self, result: RunResult):
        """Write results - hold lock only during INSERT."""
        with duckdb.connect(str(self.db_path)) as conn:
            # ~10-50ms total
            cmd_id = self._insert_command(conn, result)
            self._insert_events(conn, cmd_id, result.events)
            if result.capture_output:
                self._insert_output(conn, cmd_id, result.output)
        # Lock released immediately

    def query(self, sql: str):
        """Read query - uses read-only mode for concurrency."""
        with duckdb.connect(str(self.db_path), read_only=True) as conn:
            return conn.execute(sql).fetchall()
```

**Timing:**
- Command execution: seconds to minutes
- DB write: ~10-50ms
- Lock held: only during write phase

**Concurrency:**
| Scenario | Behavior |
|----------|----------|
| Two `blq run` finish simultaneously | Second waits ~50ms |
| MCP query during CLI write | MCP uses `read_only=True`, no blocking |
| Multiple concurrent reads | All allowed (read-only mode) |

### Issue 2: Session Semantics Don't Fit

**Problem:** BIRD's session = shell process, but blq sessions are logical groupings.

**Resolution:**
- **CLI/local:** `session_id` = `source_name` (registered command name)
- **CI integration:**
  - `client_id` = CI workflow name (e.g., `ci-build-and-test`)
  - `session_id` = workflow run ID (e.g., GitHub Actions `run_id`)
- This enables natural grouping while staying BIRD-compatible

### Issue 3: UUIDs Break Human-Readable References

**Problem:** `blq event 01937a2b-3c4d-7e8f...:3` is unwieldy.

**Resolution:** Adopt shq's range selector syntax:

```bash
# Range selectors (like shq)
blq errors ~5           # Last 5 runs with errors
blq show ~1             # Output from last run
blq event ~3:5          # Event 5 from 3rd-last run

# Sequential refs (mapped to UUIDs internally)
blq event 5:3           # Run 5, event 3 (within current date)
blq context 5:3         # Context around that event
```

**Implementation:**
- UUIDv7 is timestamp-ordered, so runs have natural sequence
- `run_number` = position in date-ordered UUID list
- Display: `5:3` (short) → Storage: `UUIDv7:event_index`

### Issue 4: Raw Output Capture Should Be Optional

**Problem:** Storing all output increases storage significantly.

**Resolution:** Make output capture **opt-in per command**:

```yaml
# commands.yaml
commands:
  build:
    cmd: "make -j8"
    capture_output: false  # Default: no raw output

  test:
    cmd: "pytest"
    capture_output: true   # Store raw output for debugging
```

```bash
# CLI override
blq run --capture-output build    # Force capture
blq run --no-capture-output test  # Force skip
```

### Issue 5: blq as BIRD Source AND Consumer

**Problem:** Plan only covered writing to BIRD, not reading from other sources.

**Resolution:** blq participates in the BIRD ecosystem both ways:

**As BIRD source:**
- Writes to `.lq/db/data/` in BIRD format
- Other tools can query blq's data via BIRD macros
- `blq sync push` exports to remote BIRD

**As BIRD consumer:**
- Can attach external BIRD databases
- Pull CI results from remote BIRD sources
- Unified queries across local + remote data

```bash
# Attach remote CI data
blq attach ci https://bird.ci.example.com/project

# Query across all sources
blq sql "SELECT * FROM blq_errors() UNION SELECT * FROM ci.errors"

# Pull specific CI run
blq pull ci run-12345
```

### Issue 6: pybirdstore - PyO3 Bindings to Rust bird-store Library

The Rust crate exists in `../magic/bird/` and will be renamed to `bird-store`. We'll add **PyO3 bindings** to expose it to Python as the `pybirdstore` package (note: `pybird` is already taken on PyPI by a BGP routing daemon interface).

**Rust library (`magic/bird-store/`):**
```
bird-store/
├── Cargo.toml
└── src/
    ├── lib.rs              # Exports Store, Config, Records
    ├── schema.rs           # InvocationRecord, OutputRecord, EventRecord
    ├── store/
    │   ├── mod.rs          # Store struct, connection management
    │   ├── invocations.rs  # write_invocation, recent_invocations
    │   ├── outputs.rs      # write_output, get_output
    │   ├── events.rs       # parse_events, query events
    │   └── compact.rs      # compaction logic
    ├── query/
    │   ├── mod.rs          # Query micro-language
    │   └── parser.rs       # ~5, %/pattern/, etc.
    └── config.rs           # Config management
```

**Add PyO3 bindings (`bird/src/python.rs`):**

```rust
use pyo3::prelude::*;
use pyo3::types::PyDict;
use bird_store::{Store, Config, InvocationRecord, OutputRecord, EventRecord};

/// Python wrapper for InvocationRecord
#[pyclass(name = "InvocationRecord")]
#[derive(Clone)]
pub struct PyInvocationRecord(pub InvocationRecord);

#[pymethods]
impl PyInvocationRecord {
    #[new]
    #[pyo3(signature = (session_id, cmd, cwd, exit_code, client_id, duration_ms=None, format_hint=None))]
    fn new(
        session_id: &str,
        cmd: &str,
        cwd: &str,
        exit_code: i32,
        client_id: &str,
        duration_ms: Option<i64>,
        format_hint: Option<&str>,
    ) -> Self {
        let mut record = InvocationRecord::new(session_id, cmd, cwd, exit_code, client_id);
        if let Some(d) = duration_ms {
            record = record.with_duration(d);
        }
        if let Some(f) = format_hint {
            record = record.with_format_hint(f);
        }
        Self(record)
    }

    #[getter]
    fn id(&self) -> String { self.0.id.to_string() }
    #[getter]
    fn session_id(&self) -> &str { &self.0.session_id }
    #[getter]
    fn cmd(&self) -> &str { &self.0.cmd }
    #[getter]
    fn exit_code(&self) -> i32 { self.0.exit_code }
    #[getter]
    fn executable(&self) -> Option<&str> { self.0.executable.as_deref() }
    #[getter]
    fn duration_ms(&self) -> Option<i64> { self.0.duration_ms }
    #[getter]
    fn format_hint(&self) -> Option<&str> { self.0.format_hint.as_deref() }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("id", self.id())?;
        dict.set_item("session_id", self.session_id())?;
        dict.set_item("cmd", self.cmd())?;
        dict.set_item("exit_code", self.exit_code())?;
        dict.set_item("executable", self.executable())?;
        dict.set_item("duration_ms", self.duration_ms())?;
        dict.set_item("format_hint", self.format_hint())?;
        Ok(dict)
    }
}

/// Python wrapper for Store
#[pyclass(name = "Store")]
pub struct PyStore(Store);

#[pymethods]
impl PyStore {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let config = Config::with_root(path);
        Store::open(config)
            .map(PyStore)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn write_invocation(&self, record: &PyInvocationRecord) -> PyResult<()> {
        self.0.write_invocation(&record.0)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn recent_invocations(&self, limit: usize, py: Python) -> PyResult<Vec<PyObject>> {
        let invocations = self.0.recent_invocations(limit)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

        invocations.into_iter()
            .map(|inv| {
                let dict = PyDict::new(py);
                dict.set_item("id", &inv.id)?;
                dict.set_item("cmd", &inv.cmd)?;
                dict.set_item("exit_code", inv.exit_code)?;
                dict.set_item("timestamp", &inv.timestamp)?;
                dict.set_item("duration_ms", inv.duration_ms)?;
                Ok(dict.into())
            })
            .collect()
    }

    fn query(&self, sql: &str, py: Python) -> PyResult<PyObject> {
        let result = self.0.query(sql)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("columns", &result.columns)?;
        dict.set_item("rows", &result.rows)?;
        Ok(dict.into())
    }

    fn invocation_count(&self) -> PyResult<i64> {
        self.0.invocation_count()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }
}

/// Python module
#[pymodule]
fn pybirdstore(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStore>()?;
    m.add_class::<PyInvocationRecord>()?;
    // Add more classes as needed
    Ok(())
}
```

**Cargo.toml changes:**
```toml
[features]
default = []
python = ["pyo3", "pythonize"]

[dependencies.pyo3]
version = "0.22"
features = ["extension-module"]
optional = true

[dependencies.pythonize]
version = "0.22"
optional = true
```

**Build with maturin:**
```bash
cd magic/bird-store
pip install maturin
maturin develop --features python   # Local dev
maturin build --features python     # Build wheel
maturin publish --features python   # Publish to PyPI as pybirdstore
```

**Python usage:**
```python
from pybirdstore import Store, InvocationRecord

# Open store
store = Store.open(".lq/")

# Write invocation
record = InvocationRecord(
    session_id="test",
    cmd="pytest",
    cwd="/home/user/project",
    exit_code=0,
    client_id="blq-shell",
    duration_ms=1500,
    format_hint="pytest",
)
store.write_invocation(record)

# Query
result = store.query("SELECT * FROM recent_invocations LIMIT 5")
print(result["columns"])  # ['id', 'cmd', 'exit_code', ...]
print(result["rows"])     # [['...', 'pytest', '0', ...], ...]

# Get recent invocations as dicts
for inv in store.recent_invocations(10):
    print(f"{inv['cmd']} -> {inv['exit_code']}")
```

**blq integration:**
```python
# blq/serve.py - MCP tools use pybirdstore directly
from pybirdstore import Store

store = Store.open(".lq/")

@mcp.tool()
def errors(limit: int = 10) -> list[dict]:
    result = store.query(f"""
        SELECT * FROM events
        WHERE severity = 'error'
        ORDER BY id DESC
        LIMIT {limit}
    """)
    return [dict(zip(result["columns"], row)) for row in result["rows"]]

@mcp.tool()
def run(command: str) -> dict:
    # Execute command, create record
    record = InvocationRecord(...)
    store.write_invocation(record)
    return record.to_dict()
```

**Benefits of PyO3 approach:**
| Aspect | Pure Python | PyO3 Bindings |
|--------|-------------|---------------|
| Performance | Good (DuckDB is fast) | Excellent (Rust + DuckDB) |
| Code sharing | Separate implementation | Same code as shq |
| Maintenance | Two codebases | One codebase |
| Features | Must reimplement | Get compaction, queries, etc. free |
| duck_hunt | Via DuckDB SQL | Native integration |
| Build complexity | Simple | Requires Rust toolchain |

**Effort: 2-3 days** (the Rust library already exists and is tested)

### Revised Session/Client Model

| Context | client_id | session_id | Example |
|---------|-----------|------------|---------|
| CLI run | `blq-shell` | source_name | `test` |
| CLI exec | `blq-shell` | `exec-{date}` | `exec-2024-12-30` |
| MCP | `blq-mcp` | conversation_id | `conv-abc123` |
| CI workflow | `ci-{workflow}` | run_id | `12345` |
| Import | `blq-import` | import_date | `2024-12-30` |

### Revised Reference System

**Display format:** `N:M` where N = run number (within scope), M = event index

**Scoping:**
- Default scope: today's runs
- `blq errors` shows `1:3`, `2:1`, etc. for today
- `blq errors --all` shows `2024-12-29/5:3` for historical

**Range selectors (shq-compatible):**
```bash
~1              # Last run
~5              # Last 5 runs
~5:~2           # Runs 5-2 ago (3 runs)
%/test/~10      # Last 10 test runs
```

### Revised Phase Order

1. **Phase 0:** Create `pybirdstore` package (PyO3 bindings to Rust bird crate)
2. **Phase 1:** Integrate pybirdstore into blq (replace current LogStore)
3. **Phase 2:** Migration Tooling (convert existing parquet → DuckDB)
4. **Phase 3:** Content-Addressed Blobs (optional output capture with dedup)
5. **Phase 4:** BIRD Consumer (ATTACH external sources, export parquet)
6. **Phase 5:** duck_hunt Integration

**Simplified by DuckDB + SQLModel approach:**
- No compaction phase (DuckDB handles internally)
- No archive tier (use date-based queries, VACUUM for cleanup)
- Type-safe models with Pydantic validation
- MCP tools get free JSON serialization
- Reusable across BIRD ecosystem

**Package structure:**
```
magic/
├── bird-store/          # Rust crate (renamed from bird/)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── python.rs    # NEW: PyO3 bindings
│   │   └── ...
│   ├── Cargo.toml       # Add pyo3 feature
│   └── pyproject.toml   # NEW: maturin config for pybirdstore
│
├── blq/                 # Uses pybirdstore
│   ├── src/blq/
│   │   ├── cli.py
│   │   ├── serve.py     # MCP - uses pybirdstore directly
│   │   └── ...
│   └── pyproject.toml   # depends on pybirdstore
│
└── shq/                 # Uses bird-store Rust crate directly
```

---

*This plan aligns blq with the BIRD specification while maintaining backward compatibility and enabling incremental adoption.*
