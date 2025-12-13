# lq Development Notes

## Project Status

This is the initial scaffolding for `lq` (Log Query) - a CLI tool for capturing, storing, and querying build/test logs using DuckDB.

### Completed
- Python package structure with pyproject.toml (hatchling build)
- CLI module (`src/lq/cli.py`) with all core commands
- SQL schema with views and macros (`src/lq/schema.sql`)
- Hive-partitioned parquet storage design
- Basic error/warning parsing fallback
- Integration hooks for duck_hunt extension

### TODO
- [ ] Test the package installation (`pip install -e .`)
- [ ] Test all CLI commands
- [ ] Fix schema.sql to work without duck_hunt's `status_badge` function
- [ ] Add unit tests
- [ ] Add `lq serve` command for MCP server mode
- [ ] Consider integration with duckdb_mcp for ATTACH/DETACH workflow

## Architecture

```
lq (Python CLI)
    │
    ├── Writes parquet files to .lq/logs/date=.../source=.../
    │
    ├── Uses duckdb Python API directly
    │
    └── Optionally uses duck_hunt extension for 44+ format parsing
```

## Key Design Decisions

1. **Parquet over DuckDB files**: Enables concurrent writes without locking
2. **Hive partitioning**: Efficient date/source-based queries
3. **Project-local storage**: `.lq/` directory in project root
4. **Optional duck_hunt**: Works with basic parsing if extension not available
5. **Python duckdb API**: No subprocess calls to duckdb CLI

## Integration Points

- **duck_hunt extension**: For enhanced log parsing
- **duckdb_mcp**: For MCP server integration (agents can query logs)
- **status_badge function**: From duck_hunt, provides `[ OK ]`/`[FAIL]`/etc. badges

## Related Projects

- `../duck_hunt/` - DuckDB extension for log parsing
- `../duckdb_mcp/` - MCP server extension for DuckDB
