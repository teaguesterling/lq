# Duck Hunt Schema V2 Migration Plan

## Overview

This document outlines the changes needed to migrate blq to duck_hunt's Schema V2.

## Key Changes

### Renamed Fields

| Old Field (blq) | New Field | Location |
|-----------------|-----------|----------|
| `error_fingerprint` | `fingerprint` | PARQUET_SCHEMA, schema.sql, Python code |

### blq-Owned Fields (No Change Needed)

These fields are blq's own metadata, NOT from duck_hunt:

- `completed_at` - Run completion timestamp (blq computes this)
- `environment` - Captured env vars (blq's MAP field)
- `duration_sec` - Run duration (blq computes this)

### New duck_hunt Fields (Available but Optional)

These fields are new in duck_hunt V2 and can be used if needed:

- `target` - Destination (IP:port, HTTP path)
- `actor_type` - Type: user, service, system, anonymous
- `external_id` - External correlation ID
- `subunit` - Hierarchy level 4
- `subunit_id` - ID for level 4
- `scope`, `group`, `unit` - Generic hierarchy (was workflow/job/step)
- `pattern_id` - Pattern cluster ID
- `similarity_score` - Pattern similarity

## Files to Update

### 1. `src/blq/commands/core.py`

**PARQUET_SCHEMA** (line ~997):
```python
# Change:
("error_fingerprint", "VARCHAR"),
# To:
("fingerprint", "VARCHAR"),
```

### 2. `src/blq/schema.sql`

**lq_get_event macro** (line ~300):
```sql
-- Change:
error_fingerprint,
-- To:
fingerprint,
```

**lq_similar_events macro** (line ~316):
```sql
-- Change:
WHERE error_fingerprint = fp
-- To:
WHERE fingerprint = fp
```

**lq_errors_json macro** (line ~345):
```sql
-- Change:
fingerprint: lq_short_fp(error_fingerprint),
-- To:
fingerprint: lq_short_fp(fingerprint),
```

### 3. `src/blq/commands/events.py`

**Lines 49-50:**
```python
# Change:
if event.get("error_fingerprint"):
    print(f"  Fingerprint: {event.get('error_fingerprint')}")
# To:
if event.get("fingerprint"):
    print(f"  Fingerprint: {event.get('fingerprint')}")
```

### 4. `src/blq/commands/execution.py`

**Line 317:**
```python
# Change:
fingerprint=e.get("error_fingerprint"),
# To:
fingerprint=e.get("fingerprint"),
```

**Line 488:**
```python
# Change:
fingerprint=e.get("error_fingerprint"),
# To:
fingerprint=e.get("fingerprint"),
```

### 5. `src/blq/serve.py`

**Line 361:**
```python
# Change:
"error_fingerprint": event_data.get("error_fingerprint"),
# To:
"fingerprint": event_data.get("fingerprint"),
```

**Line 538:**
```python
# Change:
fp = row.get("error_fingerprint")
# To:
fp = row.get("fingerprint")
```

## Testing

### SQL Tests

Run the SQL migration tests with the new duck_hunt:
```bash
../duck_hunt/build/release/duckdb < tests/sql/test_duck_hunt_v2_migration.sql
```

### Python Tests

After updating the code, run:
```bash
python -m pytest -v
```

## Existing Data

Existing `.lq/` directories are incompatible with the new schema. Rename to `.lq~` before using the new version:

```bash
mv .lq .lq~
blq init
```

## Timeline

1. ✅ Create SQL tests for new schema
2. ✅ Update PARQUET_SCHEMA in core.py
3. ✅ Update schema.sql
4. ✅ Update Python code
5. ✅ Run all tests
