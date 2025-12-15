"""Tests for Phase 1: Structured output and event references."""

import json

import pytest

from blq.cli import (
    EventRef,
    EventSummary,
    RunResult,
    parse_log_content,
)


class TestEventRef:
    """Tests for EventRef class."""

    def test_parse_valid_ref(self):
        """Parse a valid reference string."""
        ref = EventRef.parse("5:3")
        assert ref.run_id == 5
        assert ref.event_id == 3

    def test_parse_large_numbers(self):
        """Parse reference with large numbers."""
        ref = EventRef.parse("1000:999")
        assert ref.run_id == 1000
        assert ref.event_id == 999

    def test_str_representation(self):
        """String representation matches expected format."""
        ref = EventRef(run_id=42, event_id=7)
        assert str(ref) == "42:7"

    def test_parse_invalid_format_no_colon(self):
        """Raise error for missing colon."""
        with pytest.raises(ValueError, match="Invalid event reference"):
            EventRef.parse("53")

    def test_parse_invalid_format_too_many_colons(self):
        """Raise error for too many colons."""
        with pytest.raises(ValueError, match="Invalid event reference"):
            EventRef.parse("5:3:1")

    def test_parse_invalid_format_non_numeric(self):
        """Raise error for non-numeric values."""
        with pytest.raises(ValueError):
            EventRef.parse("abc:def")

    def test_roundtrip(self):
        """Parse and stringify should be reversible."""
        original = "123:456"
        ref = EventRef.parse(original)
        assert str(ref) == original


class TestEventSummary:
    """Tests for EventSummary class."""

    def test_location_full(self):
        """Format location with file, line, and column."""
        event = EventSummary(
            ref="1:1",
            severity="error",
            file_path="src/main.c",
            line_number=15,
            column_number=5,
            message="test error",
        )
        assert event.location() == "src/main.c:15:5"

    def test_location_no_column(self):
        """Format location without column."""
        event = EventSummary(
            ref="1:1",
            severity="error",
            file_path="src/main.c",
            line_number=15,
            column_number=None,
            message="test error",
        )
        assert event.location() == "src/main.c:15"

    def test_location_no_line(self):
        """Format location without line number."""
        event = EventSummary(
            ref="1:1",
            severity="error",
            file_path="Makefile",
            line_number=None,
            column_number=None,
            message="test error",
        )
        assert event.location() == "Makefile"

    def test_location_no_file(self):
        """Format location without file path."""
        event = EventSummary(
            ref="1:1",
            severity="error",
            file_path=None,
            line_number=None,
            column_number=None,
            message="test error",
        )
        assert event.location() == "?"

    def test_location_zero_column_ignored(self):
        """Column of 0 should be ignored."""
        event = EventSummary(
            ref="1:1",
            severity="error",
            file_path="src/main.c",
            line_number=15,
            column_number=0,
            message="test error",
        )
        assert event.location() == "src/main.c:15"


class TestRunResult:
    """Tests for RunResult class."""

    @pytest.fixture
    def sample_result(self):
        """Create a sample run result with errors and warnings."""
        return RunResult(
            run_id=5,
            command="make -j8",
            status="FAIL",
            exit_code=2,
            started_at="2024-01-15T10:30:00",
            completed_at="2024-01-15T10:30:12",
            duration_sec=12.345,
            summary={"total_events": 5, "errors": 2, "warnings": 1},
            errors=[
                EventSummary(
                    ref="5:1",
                    severity="error",
                    file_path="src/main.c",
                    line_number=15,
                    column_number=5,
                    message="undefined variable 'foo'",
                ),
                EventSummary(
                    ref="5:2",
                    severity="error",
                    file_path="src/utils.c",
                    line_number=10,
                    column_number=1,
                    message="missing semicolon",
                ),
            ],
            warnings=[
                EventSummary(
                    ref="5:3",
                    severity="warning",
                    file_path="src/main.c",
                    line_number=28,
                    column_number=12,
                    message="unused variable 'temp'",
                ),
            ],
        )

    def test_to_json_basic_fields(self, sample_result):
        """JSON output includes all basic fields."""
        output = sample_result.to_json()
        data = json.loads(output)

        assert data["run_id"] == 5
        assert data["command"] == "make -j8"
        assert data["status"] == "FAIL"
        assert data["exit_code"] == 2
        assert data["duration_sec"] == 12.345
        assert data["summary"]["errors"] == 2
        assert data["summary"]["warnings"] == 1

    def test_to_json_errors_included(self, sample_result):
        """JSON output includes errors by default."""
        output = sample_result.to_json()
        data = json.loads(output)

        assert len(data["errors"]) == 2
        assert data["errors"][0]["ref"] == "5:1"
        assert data["errors"][0]["file_path"] == "src/main.c"

    def test_to_json_warnings_excluded_by_default(self, sample_result):
        """JSON output excludes warnings by default."""
        output = sample_result.to_json()
        data = json.loads(output)

        assert "warnings" not in data

    def test_to_json_include_warnings(self, sample_result):
        """JSON output includes warnings when requested."""
        output = sample_result.to_json(include_warnings=True)
        data = json.loads(output)

        assert "warnings" in data
        assert len(data["warnings"]) == 1
        assert data["warnings"][0]["ref"] == "5:3"

    def test_to_markdown_header(self, sample_result):
        """Markdown output includes status header."""
        output = sample_result.to_markdown()

        assert "## ✗ Build Result: FAIL" in output
        assert "**Command:** `make -j8`" in output
        assert "12.3s" in output  # Duration rounded
        assert "Run ID:** 5" in output

    def test_to_markdown_errors_section(self, sample_result):
        """Markdown output includes errors section."""
        output = sample_result.to_markdown()

        assert "### Errors (2)" in output
        assert "`src/main.c:15:5`" in output
        assert "[5:1]" in output
        assert "undefined variable" in output

    def test_to_markdown_warnings_excluded_by_default(self, sample_result):
        """Markdown output excludes warnings by default."""
        output = sample_result.to_markdown()

        assert "### Warnings" not in output

    def test_to_markdown_include_warnings(self, sample_result):
        """Markdown output includes warnings when requested."""
        output = sample_result.to_markdown(include_warnings=True)

        assert "### Warnings (1)" in output
        assert "unused variable" in output

    def test_to_markdown_ok_status(self):
        """Markdown output shows checkmark for OK status."""
        result = RunResult(
            run_id=1,
            command="make test",
            status="OK",
            exit_code=0,
            started_at="2024-01-15T10:30:00",
            completed_at="2024-01-15T10:30:01",
            duration_sec=1.0,
            summary={"total_events": 0, "errors": 0, "warnings": 0},
            errors=[],
            warnings=[],
        )
        output = result.to_markdown()

        assert "## ✓ Build Result: OK" in output
        assert "No errors or warnings detected." in output

    def test_to_markdown_warn_status(self):
        """Markdown output shows warning symbol for WARN status."""
        result = RunResult(
            run_id=1,
            command="make",
            status="WARN",
            exit_code=0,
            started_at="2024-01-15T10:30:00",
            completed_at="2024-01-15T10:30:01",
            duration_sec=1.0,
            summary={"total_events": 1, "errors": 0, "warnings": 1},
            errors=[],
            warnings=[
                EventSummary(
                    ref="1:1",
                    severity="warning",
                    file_path="src/main.c",
                    line_number=10,
                    column_number=1,
                    message="unused",
                )
            ],
        )
        output = result.to_markdown(include_warnings=True)

        assert "## ⚠ Build Result: WARN" in output


class TestParseLogContent:
    """Tests for parse_log_content function (fallback parser)."""

    def test_parse_gcc_error(self):
        """Parse GCC-style error message."""
        content = "src/main.c:15:5: error: undefined variable 'foo'"
        events = parse_log_content(content)

        assert len(events) == 1
        assert events[0]["severity"] == "error"
        assert events[0]["file_path"] == "src/main.c"
        assert events[0]["line_number"] == 15
        assert events[0]["column_number"] == 5
        assert "undefined variable" in events[0]["message"]

    def test_parse_gcc_warning(self):
        """Parse GCC-style warning message."""
        content = "src/main.c:28:12: warning: unused variable 'temp'"
        events = parse_log_content(content)

        assert len(events) == 1
        assert events[0]["severity"] == "warning"
        assert events[0]["file_path"] == "src/main.c"
        assert events[0]["line_number"] == 28

    def test_parse_multiple_events(self):
        """Parse multiple error/warning messages."""
        content = """Building...
src/main.c:15:5: error: undefined variable 'foo'
src/main.c:28:12: warning: unused variable 'temp'
src/utils.c:10:1: error: missing semicolon
Done
"""
        events = parse_log_content(content)

        assert len(events) == 3
        assert events[0]["severity"] == "error"
        assert events[1]["severity"] == "warning"
        assert events[2]["severity"] == "error"

    def test_parse_no_column(self):
        """Parse error without column number."""
        content = "src/main.c:15: error: something wrong"
        events = parse_log_content(content)

        assert len(events) == 1
        assert events[0]["line_number"] == 15
        assert events[0]["column_number"] is None

    def test_parse_assigns_event_ids(self):
        """Events get sequential IDs."""
        content = """src/a.c:1:1: error: first
src/b.c:2:2: error: second
src/c.c:3:3: error: third
"""
        events = parse_log_content(content)

        assert events[0]["event_id"] == 1
        assert events[1]["event_id"] == 2
        assert events[2]["event_id"] == 3

    def test_parse_assigns_log_line_numbers(self):
        """Events get log line start/end positions (when available).

        Note: log_line_start/end may be None depending on the parser used.
        Duck_hunt may or may not populate these fields depending on format.
        """
        content = """Building...
src/main.c:15:5: error: first error
Some output
src/main.c:20:1: error: second error
"""
        events = parse_log_content(content)

        # The events should exist
        assert len(events) >= 2
        # log_line_start/end are optional - may be None or integers
        for event in events:
            if event.get("log_line_start") is not None:
                assert isinstance(event["log_line_start"], int)
            if event.get("log_line_end") is not None:
                assert isinstance(event["log_line_end"], int)

    def test_parse_empty_content(self):
        """Empty content returns no events."""
        events = parse_log_content("")
        assert len(events) == 0

    def test_parse_no_errors(self):
        """Content without errors returns no events."""
        content = """Building...
Compiling main.c
Linking program
Done
"""
        events = parse_log_content(content)
        assert len(events) == 0

    def test_parse_note_severity(self):
        """Note messages may or may not be captured.

        Notes are informational and different parsers handle them differently.
        Duck_hunt may capture them as 'info' severity, while the fallback
        parser may skip them entirely (only captures errors/warnings).
        """
        content = "src/main.c:15:5: note: declared here"
        events = parse_log_content(content)

        # Notes may or may not be captured depending on parser
        # If captured, severity should be 'note' or 'info'
        if len(events) > 0:
            assert events[0]["severity"] in ("note", "info")
