"""Tests for lq query and filter commands."""

import argparse
import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from lq.cli import (
    ConnectionFactory,
    parse_filter_expression,
    format_query_output,
    query_source,
    cmd_query,
    cmd_filter,
    cmd_init,
    cmd_run,
)


# ============================================================================
# ConnectionFactory Tests
# ============================================================================


class TestConnectionFactory:
    """Tests for ConnectionFactory class."""

    def test_create_returns_connection(self):
        """Create returns a working DuckDB connection."""
        conn = ConnectionFactory.create(load_schema=False)
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1

    def test_create_with_schema(self, lq_dir):
        """Create with schema loads lq_base_path macro."""
        conn = ConnectionFactory.create(lq_dir=lq_dir, load_schema=True)
        result = conn.execute("SELECT lq_base_path()").fetchone()
        assert result[0] is not None
        assert "logs" in result[0]

    def test_require_duck_hunt_without_load(self):
        """Require duck_hunt raises error if not available and install=False."""
        # Reset the cached state
        ConnectionFactory._duck_hunt_available = None

        # This will depend on whether duck_hunt is installed in the test env
        # We just verify it doesn't crash
        try:
            conn = ConnectionFactory.create(
                load_schema=False,
                require_duck_hunt=True,
            )
            # If we get here, duck_hunt is available
            assert ConnectionFactory._duck_hunt_available is True
        except duckdb.Error:
            # Expected if duck_hunt not installed
            pass

    def test_check_duck_hunt_caches_result(self):
        """Check duck_hunt caches its result."""
        # Reset state
        ConnectionFactory._duck_hunt_available = None

        conn = duckdb.connect(":memory:")
        result1 = ConnectionFactory.check_duck_hunt(conn)
        result2 = ConnectionFactory.check_duck_hunt(conn)

        # Same result both times
        assert result1 == result2


# ============================================================================
# parse_filter_expression Tests
# ============================================================================


class TestParseFilterExpression:
    """Tests for parse_filter_expression function."""

    def test_exact_match(self):
        """Parse exact match expression."""
        result = parse_filter_expression("severity=error")
        assert result == "severity = 'error'"

    def test_exact_match_with_spaces(self):
        """Parse expression with spaces around =."""
        result = parse_filter_expression("severity = error")
        assert result == "severity = 'error'"

    def test_multiple_values_or(self):
        """Parse comma-separated values as OR (IN clause)."""
        result = parse_filter_expression("severity=error,warning")
        assert result == "severity IN ('error', 'warning')"

    def test_contains_pattern(self):
        """Parse ~ as ILIKE contains pattern."""
        result = parse_filter_expression("file_path~main")
        assert result == "file_path ILIKE '%main%'"

    def test_not_equal(self):
        """Parse != as not equal."""
        result = parse_filter_expression("severity!=info")
        assert result == "severity != 'info'"

    def test_ignore_case_exact_match(self):
        """Parse exact match with ignore_case=True."""
        result = parse_filter_expression("severity=Error", ignore_case=True)
        assert "LOWER" in result
        assert "severity" in result
        assert "Error" in result

    def test_invalid_expression_raises(self):
        """Invalid expression raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_filter_expression("invalid_no_operator")
        assert "Invalid filter expression" in str(exc_info.value)


# ============================================================================
# format_query_output Tests
# ============================================================================


class TestFormatQueryOutput:
    """Tests for format_query_output function."""

    @pytest.fixture
    def sample_df(self):
        """Create a sample DataFrame for testing."""
        return pd.DataFrame({
            "severity": ["error", "warning"],
            "file_path": ["main.c", "utils.c"],
            "message": ["undefined var", "unused var"],
        })

    def test_table_format(self, sample_df):
        """Format as table (default)."""
        result = format_query_output(sample_df, output_format="table")
        assert "severity" in result
        assert "error" in result
        assert "warning" in result

    def test_json_format(self, sample_df):
        """Format as JSON."""
        result = format_query_output(sample_df, output_format="json")
        data = json.loads(result)
        assert len(data) == 2
        assert data[0]["severity"] == "error"

    def test_csv_format(self, sample_df):
        """Format as CSV."""
        result = format_query_output(sample_df, output_format="csv")
        lines = result.strip().split("\n")
        assert "severity" in lines[0]  # Header
        assert "error" in lines[1]

    def test_markdown_format(self, sample_df):
        """Format as Markdown."""
        try:
            result = format_query_output(sample_df, output_format="markdown")
            assert "|" in result  # Markdown tables use pipes
            assert "severity" in result
        except ImportError:
            # tabulate package not installed - skip
            pytest.skip("tabulate not installed")

    def test_limit_rows(self, sample_df):
        """Limit number of rows in output."""
        result = format_query_output(sample_df, output_format="json", limit=1)
        data = json.loads(result)
        assert len(data) == 1


# ============================================================================
# query_source Tests
# ============================================================================


class TestQuerySource:
    """Tests for query_source function."""

    def test_query_stored_events(self, initialized_project, sample_build_script, capsys):
        """Query stored events without specifying a file."""
        # First create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        lq_dir = Path(".lq")
        df = query_source(source=None, lq_dir=lq_dir)

        # Should return some data
        assert len(df) > 0
        assert "severity" in df.columns

    def test_query_with_where(self, initialized_project, sample_build_script):
        """Query with WHERE clause."""
        # First create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        lq_dir = Path(".lq")
        df = query_source(source=None, where="severity = 'error'", lq_dir=lq_dir)

        # Should return only errors
        assert all(df["severity"] == "error")

    def test_query_with_select(self, initialized_project, sample_build_script):
        """Query with column selection."""
        # First create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        lq_dir = Path(".lq")
        df = query_source(source=None, select="severity, message", lq_dir=lq_dir)

        # Should only have selected columns
        assert set(df.columns) == {"severity", "message"}

    def test_query_file_not_found(self, initialized_project):
        """Query non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            query_source(source="/nonexistent/file.log")


# ============================================================================
# cmd_query Tests
# ============================================================================


class TestCmdQuery:
    """Tests for cmd_query command."""

    def test_query_stored_data(self, initialized_project, sample_build_script, capsys):
        """Query stored data without file argument."""
        # Create some data first
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()  # Clear

        # Query stored data
        args = argparse.Namespace(
            files=[],
            select=None,
            filter=None,
            order=None,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_query(args)

        captured = capsys.readouterr()
        assert "severity" in captured.out

    def test_query_with_json_output(self, initialized_project, sample_build_script, capsys):
        """Query with JSON output format."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Query with JSON
        args = argparse.Namespace(
            files=[],
            select=None,
            filter="severity='error'",
            order=None,
            limit=None,
            json=True,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_query(args)

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, list)

    def test_query_with_csv_output(self, initialized_project, sample_build_script, capsys):
        """Query with CSV output format."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Query with CSV
        args = argparse.Namespace(
            files=[],
            select="severity,message",
            filter=None,
            order=None,
            limit=None,
            json=False,
            csv=True,
            markdown=False,
            log_format="auto",
        )
        cmd_query(args)

        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert "severity" in lines[0]

    def test_query_file_not_found_exits(self, initialized_project, capsys):
        """Query non-existent file exits with error."""
        args = argparse.Namespace(
            files=["/nonexistent/file.log"],
            select=None,
            filter=None,
            order=None,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )

        with pytest.raises(SystemExit) as exc_info:
            cmd_query(args)

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err.lower()


# ============================================================================
# cmd_filter Tests
# ============================================================================


class TestCmdFilter:
    """Tests for cmd_filter command."""

    def test_filter_stored_data(self, initialized_project, sample_build_script, capsys):
        """Filter stored data."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Filter errors only
        args = argparse.Namespace(
            args=["severity=error"],
            invert=False,
            count=False,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        assert "error" in captured.out.lower()

    def test_filter_count_mode(self, initialized_project, sample_build_script, capsys):
        """Filter with count mode returns only count."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Count errors
        args = argparse.Namespace(
            args=["severity=error"],
            invert=False,
            count=True,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        # Should be just a number
        count = int(captured.out.strip())
        assert count > 0

    def test_filter_invert(self, initialized_project, sample_build_script, capsys):
        """Filter with invert flag."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Filter NOT errors (invert)
        args = argparse.Namespace(
            args=["severity=error"],
            invert=True,
            count=False,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        # Should not contain "error" as severity (may contain in message though)
        assert "warning" in captured.out.lower()

    def test_filter_multiple_expressions(self, initialized_project, sample_build_script, capsys):
        """Filter with multiple expressions (AND)."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Filter by severity AND file_path
        args = argparse.Namespace(
            args=["severity=error", "file_path~main"],
            invert=False,
            count=False,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        # Should have results or be empty (depending on data)
        # Just check it doesn't crash
        assert captured.out is not None

    def test_filter_json_output(self, initialized_project, sample_build_script, capsys):
        """Filter with JSON output."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Filter with JSON output
        args = argparse.Namespace(
            args=["severity=error"],
            invert=False,
            count=False,
            ignore_case=False,
            limit=None,
            json=True,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, list)

    def test_filter_file_not_found_exits(self, initialized_project, capsys):
        """Filter non-existent file exits with error."""
        args = argparse.Namespace(
            args=["severity=error", "/nonexistent/file.log"],
            invert=False,
            count=False,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )

        with pytest.raises(SystemExit) as exc_info:
            cmd_filter(args)

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err.lower()

    def test_filter_or_values(self, initialized_project, sample_build_script, capsys):
        """Filter with OR values (comma-separated)."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Filter for errors OR warnings
        args = argparse.Namespace(
            args=["severity=error,warning"],
            invert=False,
            count=True,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        count = int(captured.out.strip())
        # Should have both errors and warnings
        assert count >= 2

    def test_filter_contains_pattern(self, initialized_project, sample_build_script, capsys):
        """Filter with contains pattern (~)."""
        # Create some data
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Filter by file path containing "main"
        args = argparse.Namespace(
            args=["file_path~main"],
            invert=False,
            count=True,
            ignore_case=False,
            limit=None,
            json=False,
            csv=False,
            markdown=False,
            log_format="auto",
        )
        cmd_filter(args)

        captured = capsys.readouterr()
        count = int(captured.out.strip())
        # Should have at least one match
        assert count > 0
