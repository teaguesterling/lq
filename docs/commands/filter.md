# lq filter

Filter log files with simple, grep-like syntax.

**Alias:** `lq f`

## Synopsis

```bash
lq filter [OPTIONS] [EXPRESSION...] [FILE...]
lq f [OPTIONS] [EXPRESSION...] [FILE...]
```

## Description

The `filter` command provides a simple, grep-like interface for filtering log events. Unlike `query` which uses SQL syntax, `filter` uses intuitive `key=value` expressions.

## Options

| Option | Description |
|--------|-------------|
| `-v, --invert` | Invert match (show non-matching rows) |
| `-c, --count` | Only print count of matches |
| `-i, --ignore-case` | Case insensitive matching |
| `-n, --limit N` | Maximum rows to return |
| `--json, -j` | Output as JSON |
| `--csv` | Output as CSV |
| `--markdown, --md` | Output as Markdown table |

## Filter Expressions

### Exact Match (`=`)

```bash
lq f severity=error build.log
lq f file_path=src/main.c build.log
```

### Multiple Values (`=v1,v2`)

Matches if the field equals any of the values (OR):

```bash
lq f severity=error,warning build.log
```

Equivalent SQL: `severity IN ('error', 'warning')`

### Contains (`~`)

Pattern matching with ILIKE (case insensitive):

```bash
lq f file_path~main build.log
lq f message~undefined build.log
```

Equivalent SQL: `file_path ILIKE '%main%'`

### Not Equal (`!=`)

```bash
lq f severity!=info build.log
```

### Multiple Expressions

Multiple expressions are combined with AND:

```bash
lq f severity=error file_path~main build.log
```

Equivalent SQL: `severity = 'error' AND file_path ILIKE '%main%'`

## Examples

### Filter Errors

```bash
lq f severity=error build.log
```

### Filter Errors and Warnings

```bash
lq f severity=error,warning build.log
```

### Filter by File

```bash
lq f file_path~utils build.log
lq f file_path~.c build.log     # All C files
```

### Exclude Info Messages

```bash
lq f severity!=info build.log
```

### Invert Match

Show everything except errors (like `grep -v`):

```bash
lq f -v severity=error build.log
```

### Count Matches

```bash
lq f -c severity=error build.log
# Output: 5
```

### Case Insensitive

```bash
lq f -i message~error build.log
```

### Combine Options

```bash
lq f -c severity=error,warning file_path~main build.log
```

### Query Stored Events

Without a file, queries stored events:

```bash
lq f severity=error
lq f -c severity=warning
```

### Output Formats

```bash
lq f severity=error --json build.log
lq f severity=error --csv build.log
```

## Comparison with query

| Task | filter | query |
|------|--------|-------|
| Errors only | `lq f severity=error` | `lq q -f "severity='error'"` |
| Contains | `lq f file_path~main` | `lq q -f "file_path LIKE '%main%'"` |
| Multiple values | `lq f severity=error,warning` | `lq q -f "severity IN ('error','warning')"` |
| Select columns | Not supported | `lq q -s file_path,message` |
| Complex SQL | Not supported | `lq q -f "line_number > 100"` |

Use `filter` for quick, simple filtering. Use `query` when you need column selection, complex conditions, or ordering.

## See Also

- [query](query.md) - SQL-based querying
- [errors](errors.md) - Quick error viewing
