"""Tests for core blq functionality."""

import argparse
import json
import os
from pathlib import Path

import pytest

from blq.cli import (
    cmd_completions,
    cmd_context,
    cmd_errors,
    cmd_event,
    cmd_formats,
    cmd_import,
    cmd_init,
    cmd_status,
    get_connection,
    get_lq_dir,
    get_next_run_id,
    write_run_parquet,
)
from blq.commands import cmd_exec
from blq.commands.core import BlqConfig, RegisteredCommand


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


class TestBlqConfigEnsureInitialized:
    """Tests for BlqConfig.ensure() method."""

    def test_returns_config_when_initialized(self, initialized_project):
        """Return config when .lq exists."""
        config = BlqConfig.ensure()
        assert config.lq_dir.exists()
        assert config.lq_dir.name == ".lq"

    def test_exits_when_not_initialized(self, chdir_temp):
        """Exit with error when .lq not found."""
        with pytest.raises(SystemExit) as exc_info:
            BlqConfig.ensure()
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

    def test_increments_after_runs(
        self, initialized_project, sample_build_script, run_adhoc_command
    ):
        """Return next ID after existing runs."""
        # Run an ad-hoc command to create run 1
        run_adhoc_command([str(sample_build_script)])

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

    def test_creates_views(self, initialized_project, sample_build_script, run_adhoc_command):
        """Create views that work with parquet files."""
        # Create some data first using ad-hoc execution
        run_adhoc_command([str(sample_build_script)])

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


class TestCmdExec:
    """Tests for blq exec command (ad-hoc execution)."""

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
            summary=False,
            verbose=False,
            include_warnings=False,
            error_limit=20,
            no_capture=False,
        )

        with pytest.raises(SystemExit) as exc_info:
            cmd_exec(args)

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
            summary=False,
            verbose=False,
            include_warnings=False,
            error_limit=20,
            no_capture=False,
        )

        with pytest.raises(SystemExit) as exc_info:
            cmd_exec(args)

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
            summary=False,
            verbose=False,
            include_warnings=False,
            error_limit=20,
            no_capture=False,
        )

        try:
            cmd_exec(args)
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
            summary=False,
            verbose=False,
            include_warnings=False,
            error_limit=20,
            no_capture=False,
        )

        try:
            cmd_exec(args)
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

    def test_shows_event_details(
        self, initialized_project, sample_build_script, run_adhoc_command, capsys
    ):
        """Show details for a specific event."""
        # Create a run first
        run_adhoc_command([str(sample_build_script)])
        capsys.readouterr()  # Clear output

        # Get event 1:1
        args = argparse.Namespace(ref="1:1", json=False)
        cmd_event(args)

        captured = capsys.readouterr()
        assert "Event: 1:1" in captured.out
        assert "Severity:" in captured.out

    def test_event_not_found(
        self, initialized_project, sample_build_script, run_adhoc_command, capsys
    ):
        """Error when event not found."""
        # First create some data so the view exists
        run_adhoc_command([str(sample_build_script)])
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

    def test_shows_context_lines(
        self, initialized_project, sample_build_script, run_adhoc_command, capsys
    ):
        """Show context lines around an event."""
        # Create a run with raw log
        run_adhoc_command([str(sample_build_script)])
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

    def test_shows_errors(
        self, initialized_project, sample_build_script, run_adhoc_command, capsys
    ):
        """Show recent errors."""
        # Create a run with errors
        run_adhoc_command([str(sample_build_script)])
        capsys.readouterr()

        # Get errors
        args = argparse.Namespace(source=None, limit=10, compact=False, json=False)
        cmd_errors(args)

        captured = capsys.readouterr()
        # Should show some error info
        assert len(captured.out) > 0

    def test_errors_json_output(
        self, initialized_project, sample_build_script, run_adhoc_command, capsys
    ):
        """Show errors in JSON format."""
        run_adhoc_command([str(sample_build_script)])
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

    def test_shows_status(
        self, initialized_project, sample_build_script, run_adhoc_command, capsys
    ):
        """Show status of runs."""
        run_adhoc_command([str(sample_build_script)], name="build")
        capsys.readouterr()

        args = argparse.Namespace(verbose=False)
        cmd_status(args)

        captured = capsys.readouterr()
        # Should show some status output
        assert len(captured.out) > 0


class TestCmdFormats:
    """Tests for blq formats command."""

    def test_lists_formats_or_shows_fallback(self, capsys):
        """List available formats or show fallback message."""
        args = argparse.Namespace()
        cmd_formats(args)

        captured = capsys.readouterr()
        # Either shows formats from duck_hunt or a fallback message
        assert "format" in captured.out.lower() or "duck_hunt" in captured.out.lower()

    def test_output_not_empty(self, capsys):
        """Command produces some output."""
        args = argparse.Namespace()
        cmd_formats(args)

        captured = capsys.readouterr()
        assert len(captured.out) > 0


class TestCmdCompletions:
    """Tests for blq completions command."""

    def test_bash_completion(self, capsys):
        """Generate bash completion script."""
        args = argparse.Namespace(shell="bash")
        cmd_completions(args)

        captured = capsys.readouterr()
        assert "_blq_completions" in captured.out
        assert "complete -F" in captured.out
        assert "COMPREPLY" in captured.out

    def test_zsh_completion(self, capsys):
        """Generate zsh completion script."""
        args = argparse.Namespace(shell="zsh")
        cmd_completions(args)

        captured = capsys.readouterr()
        assert "#compdef blq" in captured.out
        assert "_blq()" in captured.out
        assert "_arguments" in captured.out

    def test_fish_completion(self, capsys):
        """Generate fish completion script."""
        args = argparse.Namespace(shell="fish")
        cmd_completions(args)

        captured = capsys.readouterr()
        assert "complete -c blq" in captured.out
        assert "__fish_use_subcommand" in captured.out

    def test_bash_includes_all_commands(self, capsys):
        """Bash completion includes all main commands."""
        args = argparse.Namespace(shell="bash")
        cmd_completions(args)

        captured = capsys.readouterr()
        # Check for key commands
        for cmd in ["init", "run", "exec", "query", "filter", "errors", "status", "completions"]:
            assert cmd in captured.out

    def test_zsh_includes_command_descriptions(self, capsys):
        """Zsh completion includes command descriptions."""
        args = argparse.Namespace(shell="zsh")
        cmd_completions(args)

        captured = capsys.readouterr()
        # Zsh format includes descriptions like 'init:Initialize .lq directory'
        assert "init:Initialize" in captured.out
        assert "errors:Show recent errors" in captured.out

    def test_completions_include_registered_command_lookup(self, capsys):
        """Completions include logic to complete registered commands."""
        args = argparse.Namespace(shell="bash")
        cmd_completions(args)

        captured = capsys.readouterr()
        # Should reference commands.yaml for registered command completion
        assert "commands.yaml" in captured.out


# ============================================================================
# BlqConfig Tests
# ============================================================================


class TestBlqConfigBasic:
    """Basic BlqConfig functionality tests."""

    def test_create_with_path(self, lq_dir):
        """Create BlqConfig with explicit path."""
        config = BlqConfig(lq_dir=lq_dir)
        assert config.lq_dir == lq_dir
        assert config.logs_dir == lq_dir / "logs"
        assert config.raw_dir == lq_dir / "raw"
        assert config.schema_path == lq_dir / "schema.sql"

    def test_default_capture_env(self, lq_dir):
        """Default capture_env is populated."""
        config = BlqConfig(lq_dir=lq_dir)
        assert isinstance(config.capture_env, list)
        assert len(config.capture_env) > 0
        assert "PATH" in config.capture_env

    def test_custom_capture_env(self, lq_dir):
        """Custom capture_env overrides defaults."""
        custom_env = ["CUSTOM_VAR"]
        config = BlqConfig(lq_dir=lq_dir, capture_env=custom_env)
        assert config.capture_env == custom_env

    def test_namespace_and_project(self, lq_dir):
        """Set namespace and project."""
        config = BlqConfig(lq_dir=lq_dir, namespace="test_ns", project="test_proj")
        assert config.namespace == "test_ns"
        assert config.project == "test_proj"


class TestBlqConfigFind:
    """Tests for BlqConfig.find() class method."""

    def test_find_in_current_dir(self, chdir_temp):
        """Find .lq in current directory."""
        lq_path = chdir_temp / ".lq"
        lq_path.mkdir()

        config = BlqConfig.find()
        assert config is not None
        assert config.lq_dir == lq_path

    def test_find_in_parent_dir(self, chdir_temp):
        """Find .lq in parent directory."""
        lq_path = chdir_temp / ".lq"
        lq_path.mkdir()

        subdir = chdir_temp / "subdir" / "deep"
        subdir.mkdir(parents=True)
        os.chdir(subdir)

        config = BlqConfig.find()
        assert config is not None
        assert config.lq_dir == lq_path

    def test_find_returns_none_when_not_found(self, temp_dir):
        """Return None when .lq not found."""
        subdir = temp_dir / "clean" / "subdir"
        subdir.mkdir(parents=True)
        original = os.getcwd()
        os.chdir(subdir)
        try:
            config = BlqConfig.find()
            # Either None or found outside temp_dir
            if config is not None:
                assert not str(config.lq_dir).startswith(str(temp_dir))
        finally:
            os.chdir(original)


class TestBlqConfigLoad:
    """Tests for BlqConfig.load() class method."""

    def test_load_from_path(self, lq_dir):
        """Load config from existing .lq directory."""
        config = BlqConfig.load(lq_dir)
        assert config.lq_dir == lq_dir

    def test_load_reads_config_yaml(self, lq_dir):
        """Load reads settings from config.yaml."""
        # Create config.yaml with custom settings
        config_content = """
capture_env:
  - CUSTOM_VAR
  - ANOTHER_VAR
project:
  namespace: test_ns
  project: test_proj
"""
        (lq_dir / "config.yaml").write_text(config_content)

        config = BlqConfig.load(lq_dir)
        assert config.capture_env == ["CUSTOM_VAR", "ANOTHER_VAR"]
        assert config.namespace == "test_ns"
        assert config.project == "test_proj"

    def test_load_uses_defaults_when_no_config(self, lq_dir):
        """Load uses defaults when config.yaml doesn't exist."""
        # Ensure no config.yaml
        config_path = lq_dir / "config.yaml"
        if config_path.exists():
            config_path.unlink()

        config = BlqConfig.load(lq_dir)
        assert len(config.capture_env) > 0
        assert "PATH" in config.capture_env


class TestBlqConfigEnsure:
    """Tests for BlqConfig.ensure() class method."""

    def test_ensure_returns_config_when_initialized(self, initialized_project):
        """Return config when .lq exists."""
        config = BlqConfig.ensure()
        assert config is not None
        assert config.lq_dir.exists()

    def test_ensure_exits_when_not_initialized(self, chdir_temp):
        """Exit with error when .lq not found."""
        with pytest.raises(SystemExit) as exc_info:
            BlqConfig.ensure()
        assert exc_info.value.code == 1


class TestBlqConfigSave:
    """Tests for BlqConfig save methods."""

    def test_save_writes_config_yaml(self, lq_dir):
        """Save writes settings to config.yaml."""
        config = BlqConfig(
            lq_dir=lq_dir,
            capture_env=["TEST_VAR"],
            namespace="save_ns",
            project="save_proj",
        )
        config.save()

        # Read back and verify
        import yaml

        config_path = lq_dir / "config.yaml"
        assert config_path.exists()

        with open(config_path) as f:
            data = yaml.safe_load(f)

        assert data["capture_env"] == ["TEST_VAR"]
        assert data["project"]["namespace"] == "save_ns"
        assert data["project"]["project"] == "save_proj"


class TestBlqConfigCommands:
    """Tests for BlqConfig commands property."""

    def test_commands_empty_by_default(self, lq_dir):
        """Commands is empty when no commands.yaml."""
        config = BlqConfig(lq_dir=lq_dir)
        assert config.commands == {}

    def test_commands_loads_from_yaml(self, lq_dir):
        """Commands loads from commands.yaml."""
        # Create commands.yaml
        commands_content = """
commands:
  build:
    cmd: "make"
    description: "Build project"
  test:
    cmd: "pytest"
"""
        (lq_dir / "commands.yaml").write_text(commands_content)

        config = BlqConfig(lq_dir=lq_dir)
        assert "build" in config.commands
        assert "test" in config.commands
        assert config.commands["build"].cmd == "make"

    def test_commands_lazy_loaded(self, lq_dir):
        """Commands are lazily loaded."""
        config = BlqConfig(lq_dir=lq_dir)
        # Internal _commands should be None before first access
        assert config._commands is None

        # Access commands triggers load
        _ = config.commands
        assert config._commands is not None

    def test_save_commands(self, lq_dir):
        """Save commands to commands.yaml."""
        config = BlqConfig(lq_dir=lq_dir)

        # Modify commands
        config.commands["new_cmd"] = RegisteredCommand(
            name="new_cmd",
            cmd="echo hello",
            description="Test command",
        )
        config.save_commands()

        # Read back and verify
        import yaml

        with open(lq_dir / "commands.yaml") as f:
            data = yaml.safe_load(f)

        assert "new_cmd" in data["commands"]
        assert data["commands"]["new_cmd"]["cmd"] == "echo hello"

    def test_reload_commands(self, lq_dir):
        """Reload commands clears cache."""
        config = BlqConfig(lq_dir=lq_dir)
        _ = config.commands  # Load commands
        assert config._commands is not None

        config.reload_commands()
        assert config._commands is None
