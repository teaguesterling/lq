"""Tests for the blq MCP server.

Uses FastMCP's in-memory transport for efficient testing without
subprocess or network overhead.
"""

import pytest

# Skip all tests if fastmcp not installed
fastmcp = pytest.importorskip("fastmcp")

if fastmcp:
    from fastmcp import Client


def get_data(result):
    """Extract data from CallToolResult."""
    if hasattr(result, "data"):
        return result.data
    return result


@pytest.fixture
def mcp_server(initialized_project, sample_build_script):
    """Create MCP server with initialized project and sample data."""
    # Run a build to generate some data
    import subprocess

    # Import here to avoid errors if fastmcp not installed
    from blq.serve import mcp

    # Use exec for ad-hoc command execution (run is for registered commands only)
    subprocess.run(
        ["blq", "exec", "--quiet", str(sample_build_script)],
        capture_output=True,
    )

    return mcp


@pytest.fixture
def mcp_server_empty(initialized_project):
    """Create MCP server with initialized project but no data."""
    from blq.serve import mcp

    return mcp


# ============================================================================
# Tool Tests
# ============================================================================


class TestExecTool:
    """Tests for the exec tool (ad-hoc command execution)."""

    @pytest.mark.asyncio
    async def test_exec_command(self, mcp_server_empty, sample_build_script):
        """Execute an ad-hoc command and capture output."""
        async with Client(mcp_server_empty) as client:
            raw = await client.call_tool("exec", {"command": str(sample_build_script)})
            result = get_data(raw)

            assert "run_id" in result
            assert "status" in result
            assert result["status"] in ["OK", "FAIL"]

    @pytest.mark.asyncio
    async def test_exec_with_args(self, mcp_server_empty):
        """Execute a command with arguments."""
        async with Client(mcp_server_empty) as client:
            raw = await client.call_tool("exec", {"command": "echo", "args": ["hello", "world"]})
            result = get_data(raw)

            assert result["status"] == "OK"
            assert result["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_exec_failing_command(self, mcp_server_empty):
        """Execute a command that fails."""
        async with Client(mcp_server_empty) as client:
            raw = await client.call_tool(
                "exec",
                {"command": "false"},  # Always exits with 1
            )
            result = get_data(raw)

            assert result["status"] == "FAIL"
            assert result["exit_code"] != 0


class TestRunTool:
    """Tests for the run tool (registered commands)."""

    @pytest.mark.asyncio
    async def test_run_unregistered_command_fails(self, mcp_server_empty):
        """Run should fail for unregistered commands."""
        async with Client(mcp_server_empty) as client:
            raw = await client.call_tool("run", {"command": "nonexistent"})
            result = get_data(raw)

            assert result["status"] == "FAIL"
            assert "not a registered command" in result.get("error", "")


class TestQueryTool:
    """Tests for the query tool."""

    @pytest.mark.asyncio
    async def test_query_simple(self, mcp_server):
        """Run a simple SQL query."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool(
                "query", {"sql": "SELECT COUNT(*) as count FROM lq_events"}
            )
            result = get_data(raw)

            assert "columns" in result
            assert "rows" in result
            assert result["row_count"] >= 0

    @pytest.mark.asyncio
    async def test_query_with_limit(self, mcp_server):
        """Query with limit parameter."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("query", {"sql": "SELECT * FROM lq_events", "limit": 5})
            result = get_data(raw)

            assert len(result["rows"]) <= 5

    @pytest.mark.asyncio
    async def test_query_errors_only(self, mcp_server):
        """Query filtering to errors only."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool(
                "query", {"sql": "SELECT * FROM lq_events WHERE severity = 'error'"}
            )
            result = get_data(raw)

            # All returned rows should be errors
            if result["rows"]:
                severity_idx = result["columns"].index("severity")
                for row in result["rows"]:
                    assert row[severity_idx] == "error"


class TestErrorsTool:
    """Tests for the errors convenience tool."""

    @pytest.mark.asyncio
    async def test_errors_default(self, mcp_server):
        """Get errors with default parameters."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("errors", {})
            result = get_data(raw)

            assert "errors" in result
            assert "total_count" in result
            assert isinstance(result["errors"], list)

    @pytest.mark.asyncio
    async def test_errors_with_limit(self, mcp_server):
        """Get errors with limit."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("errors", {"limit": 5})
            result = get_data(raw)

            assert len(result["errors"]) <= 5

    @pytest.mark.asyncio
    async def test_errors_with_file_pattern(self, mcp_server):
        """Get errors filtered by file pattern."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("errors", {"file_pattern": "%main%"})
            result = get_data(raw)

            for error in result["errors"]:
                if error.get("file_path"):
                    assert "main" in error["file_path"].lower()

    @pytest.mark.asyncio
    async def test_errors_structure(self, mcp_server):
        """Verify error structure."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("errors", {"limit": 1})
            result = get_data(raw)

            if result["errors"]:
                error = result["errors"][0]
                assert "ref" in error
                assert "message" in error
                # ref should be in format "run_id:event_id"
                assert ":" in error["ref"]


class TestWarningsTool:
    """Tests for the warnings convenience tool."""

    @pytest.mark.asyncio
    async def test_warnings_default(self, mcp_server):
        """Get warnings with default parameters."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("warnings", {})
            result = get_data(raw)

            assert "warnings" in result
            assert "total_count" in result


class TestEventTool:
    """Tests for the event detail tool."""

    @pytest.mark.asyncio
    async def test_event_by_ref(self, mcp_server):
        """Get event details by reference."""
        async with Client(mcp_server) as client:
            # First get an error to find a valid ref
            errors_raw = await client.call_tool("errors", {"limit": 1})
            errors = get_data(errors_raw)

            if errors["errors"]:
                ref = errors["errors"][0]["ref"]

                raw = await client.call_tool("event", {"ref": ref})
                result = get_data(raw)

                assert result is not None
                assert result["ref"] == ref
                assert "message" in result
                assert "severity" in result

    @pytest.mark.asyncio
    async def test_event_not_found(self, mcp_server):
        """Event not found returns appropriate response."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("event", {"ref": "99999:99999"})
            result = get_data(raw)

            # Should return None
            assert result is None


class TestContextTool:
    """Tests for the context tool."""

    @pytest.mark.asyncio
    async def test_context_default_lines(self, mcp_server):
        """Get context with default line count."""
        async with Client(mcp_server) as client:
            errors_raw = await client.call_tool("errors", {"limit": 1})
            errors = get_data(errors_raw)

            if errors["errors"]:
                ref = errors["errors"][0]["ref"]

                raw = await client.call_tool("context", {"ref": ref})
                result = get_data(raw)

                assert "context_lines" in result
                assert isinstance(result["context_lines"], list)

    @pytest.mark.asyncio
    async def test_context_custom_lines(self, mcp_server):
        """Get context with custom line count."""
        async with Client(mcp_server) as client:
            errors_raw = await client.call_tool("errors", {"limit": 1})
            errors = get_data(errors_raw)

            if errors["errors"]:
                ref = errors["errors"][0]["ref"]

                raw = await client.call_tool("context", {"ref": ref, "lines": 10})
                result = get_data(raw)

                assert "context_lines" in result


class TestStatusTool:
    """Tests for the status tool."""

    @pytest.mark.asyncio
    async def test_status(self, mcp_server):
        """Get status summary."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("status", {})
            result = get_data(raw)

            assert "sources" in result
            assert isinstance(result["sources"], list)

    @pytest.mark.asyncio
    async def test_status_structure(self, mcp_server):
        """Verify status structure."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("status", {})
            result = get_data(raw)

            if result["sources"]:
                source = result["sources"][0]
                assert "name" in source
                assert "status" in source
                assert source["status"] in ["OK", "FAIL", "WARN"]


class TestHistoryTool:
    """Tests for the history tool."""

    @pytest.mark.asyncio
    async def test_history_default(self, mcp_server):
        """Get run history with defaults."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("history", {})
            result = get_data(raw)

            assert "runs" in result
            assert isinstance(result["runs"], list)

    @pytest.mark.asyncio
    async def test_history_with_limit(self, mcp_server):
        """Get history with limit."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("history", {"limit": 5})
            result = get_data(raw)

            assert len(result["runs"]) <= 5

    @pytest.mark.asyncio
    async def test_history_structure(self, mcp_server):
        """Verify history entry structure."""
        async with Client(mcp_server) as client:
            raw = await client.call_tool("history", {"limit": 1})
            result = get_data(raw)

            if result["runs"]:
                run = result["runs"][0]
                assert "run_id" in run
                assert "status" in run


class TestDiffTool:
    """Tests for the diff tool."""

    @pytest.mark.asyncio
    async def test_diff_two_runs(self, mcp_server_empty, sample_build_script):
        """Compare two runs."""
        async with Client(mcp_server_empty) as client:
            # Create two runs using exec (ad-hoc execution)
            run1_raw = await client.call_tool("exec", {"command": str(sample_build_script)})
            run1 = get_data(run1_raw)

            run2_raw = await client.call_tool("exec", {"command": str(sample_build_script)})
            run2 = get_data(run2_raw)

            if run1.get("run_id") and run2.get("run_id"):
                raw = await client.call_tool(
                    "diff", {"run1": run1["run_id"], "run2": run2["run_id"]}
                )
                result = get_data(raw)

                assert "summary" in result
                assert "run1_errors" in result["summary"]
                assert "run2_errors" in result["summary"]


# ============================================================================
# Resource Tests
# ============================================================================


class TestResources:
    """Tests for MCP resources."""

    @pytest.mark.asyncio
    async def test_list_resources(self, mcp_server):
        """List available resources."""
        async with Client(mcp_server) as client:
            resources = await client.list_resources()

            resource_uris = [str(r.uri) for r in resources]
            assert any("status" in uri for uri in resource_uris)

    @pytest.mark.asyncio
    async def test_read_status_resource(self, mcp_server):
        """Read the status resource."""
        async with Client(mcp_server) as client:
            content = await client.read_resource("lq://status")

            assert content is not None

    @pytest.mark.asyncio
    async def test_read_commands_resource(self, mcp_server):
        """Read the commands resource."""
        async with Client(mcp_server) as client:
            content = await client.read_resource("lq://commands")

            assert content is not None


# ============================================================================
# Prompt Tests
# ============================================================================


class TestPrompts:
    """Tests for MCP prompts."""

    @pytest.mark.asyncio
    async def test_list_prompts(self, mcp_server):
        """List available prompts."""
        async with Client(mcp_server) as client:
            prompts = await client.list_prompts()

            prompt_names = [p.name for p in prompts]
            assert "fix-errors" in prompt_names
            assert "analyze-regression" in prompt_names
            assert "summarize-run" in prompt_names

    @pytest.mark.asyncio
    async def test_get_fix_errors_prompt(self, mcp_server):
        """Get the fix-errors prompt."""
        async with Client(mcp_server) as client:
            prompt = await client.get_prompt("fix-errors", {})

            assert prompt is not None
            assert len(prompt.messages) > 0

    @pytest.mark.asyncio
    async def test_get_summarize_run_prompt(self, mcp_server):
        """Get the summarize-run prompt."""
        async with Client(mcp_server) as client:
            prompt = await client.get_prompt("summarize-run", {})

            assert prompt is not None
            assert len(prompt.messages) > 0


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests for common workflows."""

    @pytest.mark.asyncio
    async def test_build_and_query_workflow(self, mcp_server_empty, sample_build_script):
        """Test typical build -> query -> drill-down workflow."""
        async with Client(mcp_server_empty) as client:
            # 1. Run build using exec (ad-hoc execution)
            run_raw = await client.call_tool("exec", {"command": str(sample_build_script)})
            run_result = get_data(run_raw)
            assert "run_id" in run_result

            # 2. Get errors
            errors_raw = await client.call_tool("errors", {})
            errors_result = get_data(errors_raw)
            assert "errors" in errors_result

            # 3. If errors, drill down
            if errors_result["errors"]:
                ref = errors_result["errors"][0]["ref"]
                event_raw = await client.call_tool("event", {"ref": ref})
                event_result = get_data(event_raw)
                assert event_result is not None

    @pytest.mark.asyncio
    async def test_status_check_workflow(self, mcp_server):
        """Test status check workflow."""
        async with Client(mcp_server) as client:
            # 1. Check status
            status_raw = await client.call_tool("status", {})
            status = get_data(status_raw)
            assert "sources" in status

            # 2. Get history
            history_raw = await client.call_tool("history", {"limit": 5})
            hist = get_data(history_raw)
            assert "runs" in hist

            # 3. Query specific run if available
            if hist["runs"]:
                run_id = hist["runs"][0]["run_id"]
                errors_raw = await client.call_tool("errors", {"run_id": run_id})
                errors = get_data(errors_raw)
                assert "errors" in errors
