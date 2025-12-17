-- Test Duck Hunt Schema V2 Migration
--
-- Run with: ../duck_hunt/build/release/duckdb < tests/sql/test_duck_hunt_v2_migration.sql
--
-- These tests verify that blq's queries work correctly with the new duck_hunt schema.
-- Key changes:
--   - error_fingerprint → fingerprint (renamed)
--   - completed_at removed (compute from started_at + execution_time)
--   - duration removed (use execution_time)

-- Load the duck_hunt extension
LOAD duck_hunt;

-- ============================================================================
-- Test 1: Verify 'fingerprint' field exists (was 'error_fingerprint')
-- ============================================================================
SELECT '=== Test 1: fingerprint field exists ===' as test;

SELECT
    fingerprint,
    message,
    severity
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
LIMIT 3;

-- ============================================================================
-- Test 2: Verify old 'error_fingerprint' field does NOT exist
-- ============================================================================
SELECT '=== Test 2: error_fingerprint should fail ===' as test;

-- This should fail - uncomment to verify
-- SELECT error_fingerprint FROM read_duck_hunt_log(
--     '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
--     'mypy'
-- ) LIMIT 1;

-- ============================================================================
-- Test 3: Query pattern matching similar errors by fingerprint
-- ============================================================================
SELECT '=== Test 3: Group by fingerprint ===' as test;

SELECT
    fingerprint,
    COUNT(*) as occurrences,
    ANY_VALUE(message) as example_message
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
WHERE fingerprint IS NOT NULL
GROUP BY fingerprint
ORDER BY occurrences DESC
LIMIT 5;

-- ============================================================================
-- Test 4: Verify execution_time field (replaces duration)
-- ============================================================================
SELECT '=== Test 4: execution_time field ===' as test;

SELECT
    test_name,
    execution_time,
    status
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/test_frameworks/pytest_failures.txt',
    'pytest'
)
WHERE execution_time IS NOT NULL
LIMIT 5;

-- ============================================================================
-- Test 5: Verify hierarchy fields (scope, group, unit)
-- ============================================================================
SELECT '=== Test 5: Hierarchy fields ===' as test;

-- Note: github_actions format may not be available yet
-- These fields exist in schema but may be NULL for non-CI logs
SELECT
    scope,
    "group",  -- Reserved word, must be quoted
    unit,
    scope_status,
    group_status,
    unit_status
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
LIMIT 5;

-- ============================================================================
-- Test 6: Verify new fields exist
-- ============================================================================
SELECT '=== Test 6: New fields exist ===' as test;

SELECT
    target,
    actor_type,
    external_id,
    subunit,
    subunit_id
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
LIMIT 1;

-- ============================================================================
-- Test 7: Verify pattern_id and similarity_score fields
-- ============================================================================
SELECT '=== Test 7: Pattern analysis fields ===' as test;

SELECT
    fingerprint,
    pattern_id,
    similarity_score,
    message
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
WHERE fingerprint IS NOT NULL
LIMIT 5;

-- ============================================================================
-- Test 8: blq-style error query with new schema
-- ============================================================================
SELECT '=== Test 8: blq-style error query ===' as test;

SELECT
    file_path,
    line_number,
    column_number,
    LEFT(message, 100) AS message,
    tool_name,
    category,
    fingerprint  -- Changed from error_fingerprint
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
WHERE severity = 'error'
ORDER BY event_id
LIMIT 10;

-- ============================================================================
-- Test 9: Fingerprint shortening macro (blq uses this)
-- ============================================================================
SELECT '=== Test 9: Fingerprint shortening ===' as test;

-- blq's lq_short_fp macro: tool_firstN chars of hash
CREATE OR REPLACE MACRO lq_short_fp(fp) AS
    CASE WHEN fp IS NULL THEN NULL
         ELSE split_part(fp, '_', 1) || '_' || LEFT(split_part(fp, '_', 3), 8)
    END;

SELECT
    fingerprint as full_fingerprint,
    lq_short_fp(fingerprint) as short_fingerprint,
    message
FROM read_duck_hunt_log(
    '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
    'mypy'
)
WHERE fingerprint IS NOT NULL
LIMIT 5;

-- ============================================================================
-- Test 10: Verify schema column count (should be 39)
-- ============================================================================
SELECT '=== Test 10: Schema column count ===' as test;

-- Use DESCRIBE to get column count
SELECT COUNT(*) as column_count
FROM (
    DESCRIBE SELECT * FROM read_duck_hunt_log(
        '../duck_hunt/test/samples/linting_tools/mypy_output.txt',
        'mypy'
    )
);

SELECT '=== All tests completed ===' as result;
