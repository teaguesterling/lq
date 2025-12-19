# Duck Hunt Schema V3 Migration Plan

## Overview

This document outlines the changes needed to migrate blq to duck_hunt's Schema V3.

## Key Changes

### 1. Field Renames

| Old Field (blq) | New Field (V3) | Description |
|-----------------|----------------|-------------|
| `file_path` | `ref_file` | Source file referenced in log message |
| `line_number` | `ref_line` | Line number in referenced file |
| `column_number` | `ref_column` | Column number in referenced file |
| `raw_text` | `log_content` | Raw content from log file |

### 2. New Fields

| Field | Type | Description |
|-------|------|-------------|
| `log_file` | VARCHAR | Path to the log file being parsed (useful for globs) |

### 3. New Parameters

The `parse_duck_hunt_log()` function gains an `emit` parameter:

```sql
parse_duck_hunt_log(content, format, emit:='valid')
```

| Value | Description |
|-------|-------------|
| `'valid'` | Only successfully parsed rows (default, current behavior) |
| `'invalid'` | Only rows that failed to parse |
| `'all'` | All rows from the log file |

In all modes, `log_content` is always populated.

### 4. Semantic Clarity

The V3 naming makes the distinction clear:
- **`ref_*`** fields: Location referenced in the log message (e.g., "error at main.c:42")
- **`log_*`** fields: Context from the log file itself (source file, line position, content)

## Files to Update

### Core Schema

| File | Changes |
|------|---------|
| `src/blq/commands/core.py` | Update `PARQUET_SCHEMA`, `EventSummary` dataclass |
| `src/blq/schema.sql` | Update all `blq_*` macros with new field names |

### Commands

| File | Changes |
|------|---------|
| `src/blq/commands/events.py` | Update field references in `cmd_event`, `cmd_context` |
| `src/blq/commands/execution.py` | Update `_build_event_summary()` |
| `src/blq/commands/management.py` | Update status/history display |
| `src/blq/commands/query_cmd.py` | Update default select columns |

### MCP Server

| File | Changes |
|------|---------|
| `src/blq/serve.py` | Update all `_*_impl` functions that reference fields |

### Tests

| File | Changes |
|------|---------|
| `tests/test_*.py` | Update field name assertions |
| `tests/conftest.py` | Update fixtures if they reference fields |

### Documentation

| File | Changes |
|------|---------|
| `README.md` | Update examples |
| `docs/*.md` | Update field references |
| `CLAUDE.md` | Update schema documentation |

## Migration Strategy

### Option A: Backward Compatible (Recommended)

1. Add new V3 fields alongside old fields during transition
2. Populate both old and new field names
3. Deprecate old fields with warnings
4. Remove old fields in a future release

### Option B: Breaking Change

1. Update all fields at once
2. Bump major version
3. Document migration path for users

## Field Mapping Reference

```python
# Old → New mapping
FIELD_RENAMES = {
    # Reference context (location in source code)
    "file_path": "ref_file",
    "line_number": "ref_line",
    "column_number": "ref_column",
    # Log context
    "raw_text": "log_content",
}

# New fields
NEW_FIELDS = [
    ("log_file", "VARCHAR"),
]
```

## PARQUET_SCHEMA Update

```python
PARQUET_SCHEMA = [
    # Run metadata (unchanged)
    ("run_id", "BIGINT"),
    ("source_name", "VARCHAR"),
    # ... other run metadata ...

    # Event identification (unchanged)
    ("event_id", "BIGINT"),
    ("severity", "VARCHAR"),

    # Reference context (renamed)
    ("ref_file", "VARCHAR"),      # was: file_path
    ("ref_line", "BIGINT"),       # was: line_number
    ("ref_column", "BIGINT"),     # was: column_number

    # Content
    ("message", "VARCHAR"),

    # Log context (renamed + new)
    ("log_file", "VARCHAR"),      # NEW
    ("log_content", "VARCHAR"),   # was: raw_text
    ("log_line_start", "BIGINT"),
    ("log_line_end", "BIGINT"),

    # Classification (unchanged)
    ("tool_name", "VARCHAR"),
    ("category", "VARCHAR"),
    ("error_code", "VARCHAR"),
    ("fingerprint", "VARCHAR"),
]
```

## SQL Macro Updates

```sql
-- blq_location() helper
CREATE OR REPLACE MACRO blq_location(ref_file, ref_line, ref_column) AS
    CASE
        WHEN ref_file IS NULL THEN '?'
        WHEN ref_line IS NULL THEN ref_file
        WHEN ref_column IS NULL OR ref_column = 0 THEN ref_file || ':' || ref_line
        ELSE ref_file || ':' || ref_line || ':' || ref_column
    END;

-- blq_errors() update
CREATE OR REPLACE MACRO blq_errors(n := 10) AS TABLE
SELECT
    blq_ref(run_id, event_id) AS ref,
    blq_location(ref_file, ref_line, ref_column) AS location,
    message,
    ref_file,
    ref_line,
    tool_name
FROM blq_load_events()
WHERE severity = 'error'
ORDER BY run_id DESC, event_id
LIMIT n;
```

## Timeline

1. [ ] Create V3 migration branch
2. [ ] Update PARQUET_SCHEMA with new field names
3. [ ] Update schema.sql macros
4. [ ] Update Python code (EventSummary, commands)
5. [ ] Update MCP server
6. [ ] Update tests
7. [ ] Update documentation
8. [ ] Test with duck_hunt V3 extension
9. [ ] Release

## Existing Data

Existing `.lq/` directories will be incompatible. Users should reinitialize:

```bash
mv .lq .lq.backup
blq init
```

Or we could provide a migration command:

```bash
blq migrate --from-v2
```
