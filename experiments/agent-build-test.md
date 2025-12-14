# Experiment: Agent Build Test with lq

## Objective

Test that a new AI agent can successfully use lq MCP tools to:
1. Discover available commands
2. Run the project's test suite
3. Interpret results and handle any failures

## Project Context

This is the `lq` project itself - a Python CLI tool for log capture and querying.

- **Build system**: Python with hatchling (pyproject.toml)
- **Test framework**: pytest
- **Linter**: ruff

## Experiment Steps

### Phase 1: Discovery

The agent should:

1. **List available commands** using `list_commands` tool
   - Expected: Should see `test`, `lint`, `format` commands

2. **Check project status** using `status` tool
   - Expected: Shows any previous run history

### Phase 2: Run Tests

The agent should:

1. **Run the test suite** using `run` tool with command="test"
   - This runs the registered `pytest` command
   - Expected: 173 tests should pass

2. **Interpret results**
   - Parse the returned JSON for status, error_count, warning_count
   - Report success/failure to user

### Phase 3: Handle Failures (Optional)

If tests fail, the agent should:

1. **Get error details** using `errors` tool
2. **Drill down** using `event` and `context` tools
3. **Suggest fixes** based on error messages

## Success Criteria

- [ ] Agent discovers registered commands without guidance
- [ ] Agent uses `lq run test` (not direct `pytest`)
- [ ] Agent correctly interprets pass/fail status
- [ ] Agent can explain any failures encountered

## MCP Tools Reference

| Tool | Use For |
|------|---------|
| `list_commands` | Discover what commands are available |
| `run` | Execute a command (prefer registered names) |
| `status` | Check overall project status |
| `errors` | Get list of errors from latest run |
| `event` | Get details for specific error |
| `context` | Get log lines around an error |

## Sample Agent Prompt

> You have access to the lq MCP server for this project. Your task is to:
> 1. Check what build/test commands are available
> 2. Run the project's test suite
> 3. Report the results
>
> Use the lq tools (list_commands, run, status, errors, etc.) to accomplish this.
> Do NOT run pytest or other commands directly - use `lq run` to capture logs.

## Notes

- The lq MCP server should already be configured and running
- All 173 tests should pass if the codebase is in a good state
- This experiment validates both lq functionality and AGENTS.md documentation
