"""Shared fixtures for blq tests."""

import os
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    tmp = tempfile.mkdtemp()
    yield Path(tmp)
    shutil.rmtree(tmp)


@pytest.fixture
def lq_dir(temp_dir):
    """Create an initialized .lq directory."""
    lq_path = temp_dir / ".lq"
    lq_path.mkdir()
    (lq_path / "logs").mkdir()
    (lq_path / "raw").mkdir()

    # Copy schema.sql from the package
    from importlib import resources

    schema_content = resources.files("blq").joinpath("schema.sql").read_text()
    (lq_path / "schema.sql").write_text(schema_content)

    return lq_path


@pytest.fixture
def chdir_temp(temp_dir):
    """Change to temp directory and restore after test."""
    original = os.getcwd()
    os.chdir(temp_dir)
    yield temp_dir
    os.chdir(original)


@pytest.fixture
def initialized_project(chdir_temp):
    """A project directory with blq initialized."""
    import argparse

    from blq.cli import cmd_init

    args = argparse.Namespace()
    cmd_init(args)

    return chdir_temp


@pytest.fixture
def sample_build_script(temp_dir):
    """Create a sample build script that produces errors."""
    script = temp_dir / "build.sh"
    script.write_text("""#!/bin/bash
echo "Building..."
echo "src/main.c:15:5: error: undefined variable 'foo'"
echo "src/main.c:28:12: warning: unused variable 'temp'"
echo "src/utils.c:10:1: error: missing semicolon"
echo "Done"
exit 1
""")
    script.chmod(0o755)
    return script


@pytest.fixture
def sample_success_script(temp_dir):
    """Create a sample script that succeeds."""
    script = temp_dir / "success.sh"
    script.write_text("""#!/bin/bash
echo "All tests passed!"
exit 0
""")
    script.chmod(0o755)
    return script


def _run_adhoc_command(command, name=None, format="auto", quiet=True, json_output=False):
    """Helper to run an ad-hoc command using cmd_exec.

    Use this instead of cmd_run when you just need to generate test data
    without registering the command.
    """
    import argparse

    from blq.commands import cmd_exec

    args = argparse.Namespace(
        command=command if isinstance(command, list) else [command],
        name=name,
        format=format,
        keep_raw=False,
        json=json_output,
        markdown=False,
        quiet=quiet,
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


@pytest.fixture
def run_adhoc_command():
    """Fixture that provides a helper to run ad-hoc commands."""
    return _run_adhoc_command
