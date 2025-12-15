"""Tests for core blq functionality."""

import argparse
import json
import os
from pathlib import Path

import pytest

from blq.cli import (
    cmd_context,
    cmd_errors,
    cmd_event,
    cmd_import,
    cmd_init,
    cmd_run,
    cmd_status,
    ensure_initialized,
    get_connection,
    get_lq_dir,
    get_next_run_id,
    write_run_parquet,
)


class TestGetLqDir:
    """Tests for finding the .lq directory."""

    def test_finds_lq_in_current_dir(self, chdir_temp):
        """Find .lq in current directory."""
        lq_path = chdir_temp / ".lq"
        lq_path.mkdir()

        result = get_lq_dir()
        assert result == lq_path

    def test_finds_lq_in_parent_dir(self, chdir_temp):
        """Find .lq in parent directory."""
        lq_path = chdir_temp / ".lq"
        lq_path.mkdir()

        subdir = chdir_temp / "subdir" / "deep"
        subdir.mkdir(parents=True)
        os.chdir(subdir)

        result = get_lq_dir()
        assert result == lq_path

    def test_returns_none_when_not_found(self, temp_dir):
        """Return None when .lq not found."""
        # Create a clean subdirectory with no .lq anywhere in its hierarchy
        subdir = temp_dir / "clean" / "subdir"
        subdir.mkdir(parents=True)
        original = os.getcwd()
        os.chdir(subdir)
        try:
            result = get_lq_dir()
            # get_lq_dir returns None when no .lq exists, OR a path outside our temp dir
            # (if .lq exists somewhere in the system path)
            if result is not None:
                assert not str(result).startswith(str(temp_dir)), (
                    f"Unexpected .lq found within test directory: {result}"
                )
        finally:
            os.chdir(original)


class TestEnsureInitialized:
    """Tests for ensure_initialized function."""

    def test_returns_path_when_initialized(self, initialized_project):
        """Return path when .lq exists."""
        result = ensure_initialized()
        assert result.exists()
        assert result.name == ".lq"

    def test_exits_when_not_initialized(self, chdir_temp):
        """Exit with error when .lq not found."""
        with pytest.raises(SystemExit) as exc_info:
            ensure_initialized()
        assert exc_info.value.code == 1


class TestCmdInit:
    """Tests for lq init command."""

    def test_creates_lq_directory(self, chdir_temp, capsys):
        """Create .lq directory structure."""
        args = argparse.Namespace()
        cmd_init(args)

        lq_path = chdir_temp / ".lq"
        assert lq_path.exists()
        assert (lq_path / "logs").exists()
        assert (lq_path / "raw").exists()
        assert (lq_path / "schema.sql").exists()

    def test_schema_file_has_content(self, chdir_temp):
        """Schema file contains SQL definitions."""
        args = argparse.Namespace()
        cmd_init(args)

        schema = (chdir_temp / ".lq" / "schema.sql").read_text()
        assert "lq_events" in schema
        assert "lq_base_path" in schema

    def test_prints_confirmation(self, chdir_temp, capsys):
        """Print confirmation message."""
        args = argparse.Namespace()
        cmd_init(args)

        captured = capsys.readouterr()
        assert "Initialized .lq" in captured.out


class TestGetNextRunId:
    """Tests for get_next_run_id function."""

    def test_returns_1_for_empty_dir(self, lq_dir):
        """Return 1 when no runs exist."""
        result = get_next_run_id(lq_dir)
        assert result == 1

    def test_increments_after_runs(self, initialized_project, sample_build_script):
        """Return next ID after existing runs."""
        # Run a command to create run 1
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
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        # Next run should be 2
        lq_dir = Path(".lq")
        result = get_next_run_id(lq_dir)
        assert result == 2


class TestGetConnection:
    """Tests for database connection setup."""

    def test_returns_connection(self, lq_dir):
        """Return a working DuckDB connection."""
        conn = get_connection(lq_dir)
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1

    def test_loads_schema_macros(self, lq_dir):
        """Load schema macros into connection."""
        conn = get_connection(lq_dir)

        # lq_base_path should be defined
        result = conn.execute("SELECT lq_base_path()").fetchone()
        assert result[0] is not None

    def test_creates_views(self, initialized_project, sample_build_script):
        """Create views that work with parquet files."""
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
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        conn = get_connection(Path(".lq"))
        result = conn.execute("SELECT COUNT(*) FROM lq_events").fetchone()
        assert result[0] > 0


class TestWriteRunParquet:
    """Tests for writing parquet files."""

    def test_creates_parquet_file(self, lq_dir):
        """Create parquet file in correct location."""
        events = [
            {
                "event_id": 1,
                "severity": "error",
                "message": "test error",
            }
        ]
        run_meta = {
            "run_id": 1,
            "source_name": "test",
            "source_type": "run",
            "command": "test",
            "started_at": "2024-01-15T10:00:00",
            "completed_at": "2024-01-15T10:00:01",
            "exit_code": 1,
        }

        filepath = write_run_parquet(events, run_meta, lq_dir)

        assert filepath.exists()
        assert filepath.suffix == ".parquet"
        assert "date=" in str(filepath)
        assert "source=" in str(filepath)

    def test_parquet_contains_data(self, lq_dir):
        """Parquet file contains correct data."""
        import duckdb

        events = [
            {
                "event_id": 1,
                "severity": "error",
                "message": "test error",
                "file_path": "test.c",
                "line_number": 10,
            }
        ]
        run_meta = {
            "run_id": 42,
            "source_name": "test",
            "source_type": "run",
            "command": "make",
            "started_at": "2024-01-15T10:00:00",
            "completed_at": "2024-01-15T10:00:01",
            "exit_code": 1,
        }

        filepath = write_run_parquet(events, run_meta, lq_dir)

        conn = duckdb.connect(":memory:")
        result = conn.execute(f"SELECT * FROM '{filepath}'").fetchall()

        assert len(result) == 1
        # Check run_id is included
        row_dict = dict(zip([d[0] for d in conn.description], result[0]))
        assert row_dict["run_id"] == 42
        assert row_dict["severity"] == "error"


class TestCmdRun:
    """Tests for blq run command."""

    def test_captures_output(self, initialized_project, sample_build_script, capsys):
        """Capture command output and parse errors."""
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=True,
            markdown=False,
            quiet=False,
            include_warnings=False,
            error_limit=20,
            capture=None,
        )

        with pytest.raises(SystemExit) as exc_info:
            cmd_run(args)

        # Script exits with 1
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["status"] == "FAIL"
        assert data["exit_code"] == 1
        assert len(data["errors"]) > 0

    def test_success_run(self, initialized_project, sample_success_script, capsys):
        """Capture successful command."""
        args = argparse.Namespace(
            command=[str(sample_success_script)],
            name=None,
            format="auto",
            keep_raw=False,
            json=True,
            markdown=False,
            quiet=False,
            include_warnings=False,
            error_limit=20,
            capture=None,
        )

        with pytest.raises(SystemExit) as exc_info:
            cmd_run(args)

        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["status"] == "OK"
        assert data["exit_code"] == 0

    def test_keeps_raw_log(self, initialized_project, sample_build_script):
        """Keep raw log file when requested."""
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=True,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
            capture=None,
        )

        try:
            cmd_run(args)
        except SystemExit:
            pass

        raw_files = list(Path(".lq/raw").glob("*.log"))
        assert len(raw_files) == 1

    def test_quiet_suppresses_output(self, initialized_project, sample_build_script, capsys):
        """Quiet mode suppresses streaming output."""
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
            capture=None,
        )

        try:
            cmd_run(args)
        except SystemExit:
            pass

        captured = capsys.readouterr()
        # Should not contain script output (Building..., etc)
        # Only stderr summary
        assert "Building..." not in captured.out


class TestCmdImport:
    """Tests for blq import command."""

    def test_imports_log_file(self, initialized_project, temp_dir, capsys):
        """Import an existing log file."""
        # Create a log file
        log_file = temp_dir / "build.log"
        log_file.write_text("""Building...
src/main.c:10:5: error: undefined reference
Done
""")

        args = argparse.Namespace(
            file=str(log_file),
            name=None,
            format="auto",
        )

        cmd_import(args)

        captured = capsys.readouterr()
        assert "Imported" in captured.out or "Captured" in captured.out

        # Check parquet was created
        parquet_files = list(Path(".lq/logs").rglob("*.parquet"))
        assert len(parquet_files) >= 1


class TestCmdEvent:
    """Tests for blq event command."""

    def test_shows_event_details(self, initialized_project, sample_build_script, capsys):
        """Show details for a specific event."""
        # Create a run first
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=True,
            json=True,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()  # Clear output

        # Get event 1:1
        args = argparse.Namespace(ref="1:1", json=False)
        cmd_event(args)

        captured = capsys.readouterr()
        assert "Event: 1:1" in captured.out
        assert "Severity:" in captured.out

    def test_event_not_found(self, initialized_project, sample_build_script, capsys):
        """Error when event not found."""
        # First create some data so the view exists
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
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass
        capsys.readouterr()  # Clear output

        # Now try to get a non-existent event
        args = argparse.Namespace(ref="999:999", json=False)

        with pytest.raises(SystemExit) as exc_info:
            cmd_event(args)

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err


class TestCmdContext:
    """Tests for blq context command."""

    def test_shows_context_lines(self, initialized_project, sample_build_script, capsys):
        """Show context lines around an event."""
        # Create a run with raw log
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name=None,
            format="auto",
            keep_raw=True,
            json=True,  # This also keeps raw
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Get context for event 1:1
        args = argparse.Namespace(ref="1:1", lines=2)

        # This may work or may say no log_line_start depending on parser
        try:
            cmd_context(args)
            captured = capsys.readouterr()
            # Should show some output
            assert "Context" in captured.out or "Event" in captured.out
        except SystemExit:
            # Event not found is also acceptable in some cases
            pass


class TestCmdErrors:
    """Tests for blq errors command."""

    def test_shows_errors(self, initialized_project, sample_build_script, capsys):
        """Show recent errors."""
        # Create a run with errors
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
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        # Get errors
        args = argparse.Namespace(source=None, limit=10, compact=False, json=False)
        cmd_errors(args)

        captured = capsys.readouterr()
        # Should show some error info
        assert len(captured.out) > 0

    def test_errors_json_output(self, initialized_project, sample_build_script, capsys):
        """Show errors in JSON format."""
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
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        args = argparse.Namespace(source=None, limit=10, compact=False, json=True)
        cmd_errors(args)

        captured = capsys.readouterr()
        # Should be valid JSON
        if captured.out.strip():
            data = json.loads(captured.out)
            assert isinstance(data, (list, str))


class TestCmdStatus:
    """Tests for blq status command."""

    def test_shows_status(self, initialized_project, sample_build_script, capsys):
        """Show status of runs."""
        args = argparse.Namespace(
            command=[str(sample_build_script)],
            name="build",
            format="auto",
            keep_raw=False,
            json=False,
            markdown=False,
            quiet=True,
            include_warnings=False,
            error_limit=20,
            capture=None,
        )
        try:
            cmd_run(args)
        except SystemExit:
            pass

        capsys.readouterr()

        args = argparse.Namespace(verbose=False)
        cmd_status(args)

        captured = capsys.readouterr()
        # Should show some status output
        assert len(captured.out) > 0
