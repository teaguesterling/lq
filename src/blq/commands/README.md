# lq Commands Module

This directory contains the modular command implementations for the lq CLI.

## Structure

The commands are organized into logical groups:

- **core.py** - Shared data classes, configuration, connection management, and utilities
- **init_cmd.py** - Project initialization (`lq init`)
- **execution.py** - Command execution and log capture (`lq run`, `lq import`, `lq capture`)
- **query_cmd.py** - Log querying and filtering (`lq query`, `lq filter`, `lq sql`, `lq shell`)
- **management.py** - Status and history management (`lq status`, `lq errors`, `lq warnings`, `lq summary`, `lq history`, `lq prune`)
- **events.py** - Event inspection (`lq event`, `lq context`)
- **registry.py** - Command registry (`lq commands`, `lq register`, `lq unregister`)
- **sync_cmd.py** - Log synchronization (`lq sync`)
- **serve_cmd.py** - MCP server (`lq serve`)
- **__init__.py** - Module exports

## Adding New Commands

When adding a new command:

1. Choose the appropriate module based on the command's purpose
2. If the command doesn't fit any existing module, create a new module
3. Add the command function to the module (follow the `cmd_<name>` naming convention)
4. Export the function in `__init__.py`
5. Import and wire up the command in `cli.py`
6. Add tests for the new command

## Shared Utilities

The `core.py` module contains shared utilities used across multiple commands:

- Data classes: `EventRef`, `EventSummary`, `RunResult`, `LqConfig`, `RegisteredCommand`
- Configuration management: `save_config()`, `load_commands()`, `save_commands()`
- Database connections: `ConnectionFactory`, `get_connection()`, `ensure_initialized()`
- Parquet writing: `write_run_parquet()`
- Log parsing: `parse_log_content()`
- Execution context capture: `capture_environment()`, `capture_git_info()`, `capture_ci_info()`

## Backward Compatibility

The main `cli.py` re-exports commonly used items from the commands modules to maintain backward compatibility with existing code and tests.
