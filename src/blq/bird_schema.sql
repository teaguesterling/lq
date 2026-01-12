-- bird_schema.sql - BIRD (Buffer and Invocation Record Database) Schema for blq
--
-- This schema implements the BIRD specification using DuckDB tables (single-writer mode).
-- All reads go through views, writes go directly to tables.
--
-- Directory structure:
--   .lq/
--   ├── blq.duckdb          # Database with tables and views
--   ├── blobs/              # Content-addressed output storage
--   │   └── content/
--   │       ├── ab/
--   │       │   └── {hash}--{hint}.bin
--   │       └── ...
--   └── config.yaml
--
-- BIRD spec: https://github.com/teaguesterling/magic/blob/main/docs/bird_spec.md

-- ============================================================================
-- CONFIGURATION
-- ============================================================================

-- Schema version for migrations
CREATE TABLE IF NOT EXISTS blq_metadata (
    key VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL
);

-- Insert schema version (ignore if exists)
INSERT OR IGNORE INTO blq_metadata VALUES ('schema_version', '2.0.0');
INSERT OR IGNORE INTO blq_metadata VALUES ('storage_mode', 'duckdb');

-- Base path for blob storage (set at runtime)
CREATE OR REPLACE MACRO blq_blob_root() AS '.lq/blobs/content';

-- ============================================================================
-- CORE TABLES (BIRD Schema)
-- ============================================================================

-- Sessions table: tracks invoker sessions (shell, CLI, MCP)
CREATE TABLE IF NOT EXISTS sessions (
    -- Identity
    session_id        VARCHAR PRIMARY KEY,      -- e.g., "test" (source_name), "exec-2024-12-30"
    client_id         VARCHAR NOT NULL,         -- e.g., "blq-shell", "blq-mcp"

    -- Invoker information
    invoker           VARCHAR NOT NULL,         -- e.g., "blq", "blq-mcp"
    invoker_pid       INTEGER,                  -- Process ID (if applicable)
    invoker_type      VARCHAR NOT NULL,         -- "cli", "mcp", "import", "capture"

    -- Timing
    registered_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Context
    cwd               VARCHAR,                  -- Initial working directory

    -- Partitioning
    date              DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Invocations table: command executions (was "runs" in blq v1)
CREATE TABLE IF NOT EXISTS invocations (
    -- Identity
    id                UUID PRIMARY KEY DEFAULT uuid(),  -- UUIDv7 when available
    session_id        VARCHAR NOT NULL,                 -- References sessions.session_id

    -- Timing
    timestamp         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_ms       BIGINT,

    -- Context
    cwd               VARCHAR NOT NULL,

    -- Command
    cmd               VARCHAR NOT NULL,                 -- Full command string
    executable        VARCHAR,                          -- Extracted executable name

    -- Result
    exit_code         INTEGER NOT NULL,

    -- Format detection
    format_hint       VARCHAR,                          -- Detected format (gcc, pytest, etc.)

    -- Client identity
    client_id         VARCHAR NOT NULL,                 -- e.g., "blq-shell"
    hostname          VARCHAR,
    username          VARCHAR,

    -- blq-specific fields
    source_name       VARCHAR,                          -- Registered command name
    source_type       VARCHAR,                          -- 'run', 'exec', 'import', 'capture'
    environment       JSON,                             -- Captured environment variables
    platform          VARCHAR,                          -- OS (Linux, Darwin, Windows)
    arch              VARCHAR,                          -- Architecture (x86_64, arm64)
    git_commit        VARCHAR,                          -- HEAD SHA
    git_branch        VARCHAR,                          -- Current branch
    git_dirty         BOOLEAN,                          -- Uncommitted changes
    ci                JSON,                             -- CI provider context

    -- Partitioning
    date              DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Outputs table: captured stdout/stderr
CREATE TABLE IF NOT EXISTS outputs (
    -- Identity
    id                UUID PRIMARY KEY DEFAULT uuid(),
    invocation_id     UUID NOT NULL,                    -- References invocations.id

    -- Stream
    stream            VARCHAR NOT NULL,                 -- 'stdout', 'stderr', 'combined'

    -- Content identification
    content_hash      VARCHAR NOT NULL,                 -- BLAKE3 hash (hex, 64 chars)
    byte_length       BIGINT NOT NULL,

    -- Storage location (polymorphic)
    storage_type      VARCHAR NOT NULL,                 -- 'inline' or 'blob'
    storage_ref       VARCHAR NOT NULL,                 -- data: URI or file: path

    -- Content metadata
    content_type      VARCHAR,                          -- MIME type or format hint

    -- Partitioning
    date              DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Events table: parsed diagnostics (errors, warnings, test results)
CREATE TABLE IF NOT EXISTS events (
    -- Identity
    id                UUID PRIMARY KEY DEFAULT uuid(),
    invocation_id     UUID NOT NULL,                    -- References invocations.id
    event_index       INTEGER NOT NULL,                 -- Index within invocation

    -- Client identity (denormalized for cross-client queries)
    client_id         VARCHAR NOT NULL,
    hostname          VARCHAR,

    -- Event classification
    event_type        VARCHAR,                          -- 'diagnostic', 'test_result', etc.
    severity          VARCHAR,                          -- 'error', 'warning', 'info', 'note'

    -- Source location
    file_path         VARCHAR,                          -- Source file path
    line_number       INTEGER,                          -- Line number
    column_number     INTEGER,                          -- Column number

    -- Content
    message           VARCHAR,                          -- Error/warning message
    code              VARCHAR,                          -- Error code (e.g., "E0308")
    rule              VARCHAR,                          -- Rule name (e.g., "no-unused-vars")

    -- blq-specific fields
    tool_name         VARCHAR,                          -- Tool that generated event
    category          VARCHAR,                          -- Error category
    fingerprint       VARCHAR,                          -- Unique identifier for dedup
    log_line_start    INTEGER,                          -- Start line in raw log
    log_line_end      INTEGER,                          -- End line in raw log
    context           VARCHAR,                          -- Surrounding context
    metadata          JSON,                             -- Format-specific extras

    -- Parsing metadata
    format_used       VARCHAR,                          -- Parser format (gcc, cargo, pytest)

    -- Partitioning
    date              DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Blob registry: tracks content-addressed blobs for deduplication
CREATE TABLE IF NOT EXISTS blob_registry (
    content_hash      VARCHAR PRIMARY KEY,              -- BLAKE3 hash (hex)
    byte_length       BIGINT NOT NULL,
    compression       VARCHAR DEFAULT 'none',           -- 'none', 'gzip', 'zstd'
    ref_count         INTEGER DEFAULT 1,
    first_seen        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    storage_path      VARCHAR NOT NULL                  -- Relative path within blobs/
);

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_invocations_session ON invocations(session_id);
CREATE INDEX IF NOT EXISTS idx_invocations_date ON invocations(date);
CREATE INDEX IF NOT EXISTS idx_invocations_source ON invocations(source_name);
CREATE INDEX IF NOT EXISTS idx_invocations_timestamp ON invocations(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_invocation ON events(invocation_id);
CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity);
CREATE INDEX IF NOT EXISTS idx_events_date ON events(date);
CREATE INDEX IF NOT EXISTS idx_events_file ON events(file_path);

CREATE INDEX IF NOT EXISTS idx_outputs_invocation ON outputs(invocation_id);

-- ============================================================================
-- COMPATIBILITY VIEWS (blq v1 API)
-- ============================================================================

-- blq_load_events() - returns events with invocation metadata joined
-- This provides backward compatibility with the v1 flat schema
CREATE OR REPLACE VIEW blq_events_flat AS
SELECT
    -- Event identity (v1 style)
    e.event_index AS event_id,

    -- Invocation as "run" (v1 terminology)
    ROW_NUMBER() OVER (ORDER BY i.timestamp) AS run_id,

    -- Invocation fields (denormalized for v1 compatibility)
    i.source_name,
    i.source_type,
    i.cmd AS command,
    i.timestamp AS started_at,
    i.timestamp + INTERVAL (i.duration_ms / 1000) SECOND AS completed_at,
    i.exit_code,
    i.cwd,
    i.executable AS executable_path,
    i.hostname,
    i.platform,
    i.arch,
    i.git_commit,
    i.git_branch,
    i.git_dirty,
    i.ci,
    i.environment,

    -- Event fields
    e.severity,
    e.message,
    e.file_path,
    e.line_number,
    e.column_number,
    e.tool_name,
    e.category,
    e.code,
    e.rule,
    e.fingerprint,
    e.log_line_start,
    e.log_line_end,
    e.context,
    e.metadata,

    -- Partition info
    i.date AS log_date,
    i.source_type AS partition_source,

    -- Internal IDs for advanced queries
    i.id AS invocation_id,
    e.id AS event_uuid
FROM events e
JOIN invocations i ON e.invocation_id = i.id;

-- blq_load_events() macro for backward compatibility
CREATE OR REPLACE MACRO blq_load_events() AS TABLE
SELECT * FROM blq_events_flat;

-- ============================================================================
-- BIRD-NATIVE VIEWS
-- ============================================================================

-- Recent invocations (last 14 days)
CREATE OR REPLACE VIEW invocations_recent AS
SELECT * FROM invocations
WHERE date >= CURRENT_DATE - INTERVAL '14 days';

-- Recent events (last 14 days)
CREATE OR REPLACE VIEW events_recent AS
SELECT * FROM events
WHERE date >= CURRENT_DATE - INTERVAL '14 days';

-- ============================================================================
-- MACROS (Updated for BIRD schema)
-- ============================================================================

-- Status badge
CREATE OR REPLACE MACRO blq_status_badge(error_count, warning_count) AS
    CASE
        WHEN error_count > 0 THEN '[FAIL]'
        WHEN warning_count > 0 THEN '[WARN]'
        ELSE '[ OK ]'
    END;

-- Load runs with aggregated stats (uses invocations table)
CREATE OR REPLACE MACRO blq_load_runs() AS TABLE
SELECT
    ROW_NUMBER() OVER (ORDER BY i.timestamp) AS run_id,
    i.id AS invocation_id,
    i.source_name,
    i.source_type,
    i.cmd AS command,
    i.timestamp AS started_at,
    i.timestamp + INTERVAL (COALESCE(i.duration_ms, 0) / 1000) SECOND AS completed_at,
    i.exit_code,
    COUNT(e.id) AS event_count,
    COUNT(e.id) FILTER (WHERE e.severity = 'error') AS error_count,
    COUNT(e.id) FILTER (WHERE e.severity = 'warning') AS warning_count,
    i.date AS log_date
FROM invocations i
LEFT JOIN events e ON e.invocation_id = i.id
GROUP BY i.id, i.source_name, i.source_type, i.cmd, i.timestamp, i.duration_ms, i.exit_code, i.date;

-- Load latest run per source with status badge
CREATE OR REPLACE MACRO blq_load_source_status() AS TABLE
SELECT
    source_name,
    blq_status_badge(error_count, warning_count) AS badge,
    error_count,
    warning_count,
    event_count,
    started_at,
    completed_at,
    exit_code,
    run_id,
    invocation_id
FROM blq_load_runs()
QUALIFY row_number() OVER (PARTITION BY source_name ORDER BY started_at DESC) = 1
ORDER BY source_name;

-- Quick status overview
CREATE OR REPLACE MACRO blq_status() AS TABLE
SELECT
    badge || ' ' || source_name AS status,
    error_count AS errors,
    warning_count AS warnings,
    age(now(), started_at::TIMESTAMP) AS age
FROM blq_load_source_status()
ORDER BY
    CASE WHEN badge = '[FAIL]' THEN 0
         WHEN badge = '[WARN]' THEN 1
         ELSE 2 END,
    source_name;

-- Recent errors
CREATE OR REPLACE MACRO blq_errors(n := 10) AS TABLE
SELECT
    i.source_name,
    e.file_path,
    e.line_number,
    e.column_number,
    LEFT(e.message, 200) AS message,
    e.tool_name,
    e.category
FROM events e
JOIN invocations i ON e.invocation_id = i.id
WHERE e.severity = 'error'
ORDER BY i.timestamp DESC, e.event_index
LIMIT n;

-- Recent warnings
CREATE OR REPLACE MACRO blq_warnings(n := 10) AS TABLE
SELECT
    i.source_name,
    e.file_path,
    e.line_number,
    e.column_number,
    LEFT(e.message, 200) AS message,
    e.tool_name,
    e.category
FROM events e
JOIN invocations i ON e.invocation_id = i.id
WHERE e.severity = 'warning'
ORDER BY i.timestamp DESC, e.event_index
LIMIT n;

-- Run history
CREATE OR REPLACE MACRO blq_history(n := 20) AS TABLE
SELECT
    run_id,
    blq_status_badge(error_count, warning_count) AS badge,
    source_name,
    error_count,
    warning_count,
    started_at,
    age(completed_at::TIMESTAMP, started_at::TIMESTAMP) AS duration
FROM blq_load_runs()
ORDER BY started_at DESC
LIMIT n;

-- Compare two runs by run_id
CREATE OR REPLACE MACRO blq_diff(run1, run2) AS TABLE
WITH runs AS (
    SELECT run_id, invocation_id FROM blq_load_runs()
),
r1 AS (
    SELECT e.tool_name, e.category,
           COUNT(*) FILTER (WHERE e.severity = 'error') AS errors
    FROM events e
    JOIN runs r ON e.invocation_id = r.invocation_id
    WHERE r.run_id = run1
    GROUP BY e.tool_name, e.category
),
r2 AS (
    SELECT e.tool_name, e.category,
           COUNT(*) FILTER (WHERE e.severity = 'error') AS errors
    FROM events e
    JOIN runs r ON e.invocation_id = r.invocation_id
    WHERE r.run_id = run2
    GROUP BY e.tool_name, e.category
)
SELECT
    COALESCE(r1.tool_name, r2.tool_name) AS tool_name,
    COALESCE(r1.category, r2.category) AS category,
    COALESCE(r1.errors, 0) AS run1_errors,
    COALESCE(r2.errors, 0) AS run2_errors,
    COALESCE(r2.errors, 0) - COALESCE(r1.errors, 0) AS delta
FROM r1 FULL OUTER JOIN r2
  ON r1.tool_name = r2.tool_name AND r1.category = r2.category
WHERE COALESCE(r1.errors, 0) != COALESCE(r2.errors, 0)
ORDER BY ABS(delta) DESC;

-- ============================================================================
-- REFERENCE MACROS
-- ============================================================================

-- Create event reference string: "5:3" for run 5, event 3
CREATE OR REPLACE MACRO blq_ref(run_id, event_id) AS
    run_id::VARCHAR || ':' || event_id::VARCHAR;

-- Parse event reference
CREATE OR REPLACE MACRO blq_parse_ref(ref) AS {
    run_id: CAST(split_part(ref, ':', 1) AS INTEGER),
    event_id: CAST(split_part(ref, ':', 2) AS INTEGER)
};

-- Format location string
CREATE OR REPLACE MACRO blq_location(file_path, line_number, column_number) AS
    COALESCE(file_path, '?') ||
    CASE WHEN line_number IS NOT NULL THEN ':' || line_number::VARCHAR ELSE '' END ||
    CASE WHEN column_number IS NOT NULL AND column_number > 0 THEN ':' || column_number::VARCHAR ELSE '' END;

-- ============================================================================
-- OUTPUT ACCESS
-- ============================================================================

-- Get output content for an invocation
CREATE OR REPLACE MACRO blq_output(inv_id, stream_name := 'combined') AS TABLE
SELECT
    o.stream,
    o.storage_type,
    o.storage_ref,
    o.byte_length,
    o.content_hash
FROM outputs o
WHERE o.invocation_id = inv_id
  AND (stream_name = 'combined' OR o.stream = stream_name);

-- ============================================================================
-- JSON OUTPUT (for MCP/agents)
-- ============================================================================

CREATE OR REPLACE MACRO blq_errors_json(n := 10) AS TABLE
SELECT to_json(list(err)) AS json FROM (
    SELECT {
        ref: blq_ref(
            (SELECT run_id FROM blq_load_runs() r WHERE r.invocation_id = e.invocation_id),
            e.event_index
        ),
        file_path: e.file_path,
        line: e.line_number,
        col: e.column_number,
        message: e.message,
        tool: e.tool_name,
        category: e.category,
        fingerprint: e.fingerprint
    } AS err
    FROM events e
    JOIN invocations i ON e.invocation_id = i.id
    WHERE e.severity = 'error'
    ORDER BY i.timestamp DESC, e.event_index
    LIMIT n
);
