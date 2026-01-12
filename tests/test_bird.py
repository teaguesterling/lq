"""
Tests for BIRD (Buffer and Invocation Record Database) integration.

Tests the BIRD storage backend including:
- BirdStore initialization and schema creation
- Session management
- Invocation writing and querying
- Event writing
- Output/blob storage
- Integration with blq init --bird
- Integration with command execution
"""

from __future__ import annotations

import argparse
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from blq.bird import (
    BirdStore,
    EventRecord,
    InvocationRecord,
    OutputRecord,
    SessionRecord,
    write_bird_invocation,
)
from blq.commands.core import BlqConfig, RegisteredCommand, write_run_parquet
from blq.commands.init_cmd import cmd_init
from blq.commands.migrate import cmd_migrate, _migrate_parquet_to_bird


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def bird_store(temp_dir):
    """Create a BirdStore in a temporary directory."""
    lq_dir = temp_dir / ".lq"
    lq_dir.mkdir()
    (lq_dir / "blobs" / "content").mkdir(parents=True)

    store = BirdStore.open(lq_dir)
    yield store
    store.close()


@pytest.fixture
def bird_initialized_dir(temp_dir):
    """Initialize a directory with BIRD mode."""
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    try:
        args = argparse.Namespace()
        args.mcp = False
        args.detect = False
        args.detect_mode = "none"
        args.yes = False
        args.force = False
        args.bird = True
        args.namespace = "test"
        args.project = "bird-test"

        cmd_init(args)
        yield temp_dir
    finally:
        os.chdir(original_cwd)


class TestBirdStoreInit:
    """Tests for BirdStore initialization."""

    def test_open_creates_schema(self, temp_dir):
        """Opening a BirdStore creates the schema."""
        lq_dir = temp_dir / ".lq"
        lq_dir.mkdir()
        (lq_dir / "blobs" / "content").mkdir(parents=True)

        store = BirdStore.open(lq_dir)

        # Check tables exist
        tables = store.connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {t[0] for t in tables}

        assert "sessions" in table_names
        assert "invocations" in table_names
        assert "outputs" in table_names
        assert "events" in table_names
        assert "blob_registry" in table_names
        assert "blq_metadata" in table_names

        store.close()

    def test_open_idempotent(self, temp_dir):
        """Opening a BirdStore multiple times doesn't fail."""
        lq_dir = temp_dir / ".lq"
        lq_dir.mkdir()
        (lq_dir / "blobs" / "content").mkdir(parents=True)

        # First open
        store1 = BirdStore.open(lq_dir)
        store1.close()

        # Second open should work
        store2 = BirdStore.open(lq_dir)
        assert store2.invocation_count() == 0
        store2.close()

    def test_context_manager(self, temp_dir):
        """BirdStore works as context manager."""
        lq_dir = temp_dir / ".lq"
        lq_dir.mkdir()
        (lq_dir / "blobs" / "content").mkdir(parents=True)

        with BirdStore.open(lq_dir) as store:
            assert store.invocation_count() == 0


class TestSessionManagement:
    """Tests for session management."""

    def test_ensure_session_creates(self, bird_store):
        """ensure_session creates a new session."""
        bird_store.ensure_session(
            session_id="test-session",
            client_id="blq-test",
            invoker="blq",
            invoker_type="cli",
            cwd="/tmp/test",
        )

        result = bird_store.connection.execute(
            "SELECT * FROM sessions WHERE session_id = 'test-session'"
        ).fetchone()

        assert result is not None
        assert result[1] == "blq-test"  # client_id

    def test_ensure_session_idempotent(self, bird_store):
        """ensure_session doesn't duplicate sessions."""
        bird_store.ensure_session(
            session_id="test-session",
            client_id="blq-test",
            invoker="blq",
            invoker_type="cli",
        )
        bird_store.ensure_session(
            session_id="test-session",
            client_id="blq-test",
            invoker="blq",
            invoker_type="cli",
        )

        count = bird_store.connection.execute(
            "SELECT COUNT(*) FROM sessions WHERE session_id = 'test-session'"
        ).fetchone()[0]

        assert count == 1


class TestInvocationManagement:
    """Tests for invocation writing and querying."""

    def test_write_invocation(self, bird_store):
        """write_invocation stores an invocation."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="echo hello",
            cwd="/tmp",
            exit_code=0,
            client_id="blq-test",
        )

        inv_id = bird_store.write_invocation(inv)

        assert inv_id == inv.id
        assert bird_store.invocation_count() == 1

    def test_write_invocation_with_metadata(self, bird_store):
        """write_invocation stores all metadata fields."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="pytest tests/",
            cwd="/home/user/project",
            exit_code=1,
            client_id="blq-shell",
            duration_ms=5000,
            executable="/usr/bin/pytest",
            format_hint="pytest_text",
            hostname="testhost",
            username="testuser",
            source_name="test",
            source_type="run",
            environment={"PATH": "/usr/bin", "PYTHONPATH": "/home/user"},
            platform="Linux",
            arch="x86_64",
            git_commit="abc123",
            git_branch="main",
            git_dirty=True,
            ci={"provider": "github", "run_id": "12345"},
        )

        bird_store.write_invocation(inv)

        result = bird_store.connection.execute(
            "SELECT source_name, hostname, git_branch FROM invocations"
        ).fetchone()

        assert result[0] == "test"
        assert result[1] == "testhost"
        assert result[2] == "main"

    def test_recent_invocations(self, bird_store):
        """recent_invocations returns invocations in order."""
        for i in range(5):
            inv = InvocationRecord(
                id=str(uuid.uuid4()),
                session_id="test",
                cmd=f"command-{i}",
                cwd="/tmp",
                exit_code=0,
                client_id="blq-test",
            )
            bird_store.write_invocation(inv)

        recent = bird_store.recent_invocations(3)

        assert len(recent) == 3
        # Most recent first
        assert recent[0]["cmd"] == "command-4"

    def test_invocation_count(self, bird_store):
        """invocation_count returns correct count."""
        assert bird_store.invocation_count() == 0

        for i in range(3):
            inv = InvocationRecord(
                id=str(uuid.uuid4()),
                session_id="test",
                cmd=f"cmd-{i}",
                cwd="/tmp",
                exit_code=0,
                client_id="blq-test",
            )
            bird_store.write_invocation(inv)

        assert bird_store.invocation_count() == 3


class TestEventManagement:
    """Tests for event writing."""

    def test_write_events(self, bird_store):
        """write_events stores parsed events."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="make",
            cwd="/tmp",
            exit_code=1,
            client_id="blq-test",
        )
        bird_store.write_invocation(inv)

        events = [
            {
                "event_id": 0,
                "severity": "error",
                "file_path": "src/main.c",
                "line_number": 10,
                "message": "undefined reference to 'foo'",
                "tool_name": "gcc",
            },
            {
                "event_id": 1,
                "severity": "warning",
                "file_path": "src/util.c",
                "line_number": 25,
                "message": "unused variable 'x'",
                "tool_name": "gcc",
            },
        ]

        count = bird_store.write_events(
            inv.id, events, client_id="blq-test", format_used="gcc"
        )

        assert count == 2
        assert bird_store.event_count() == 2

    def test_write_events_empty(self, bird_store):
        """write_events handles empty event list."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="echo hello",
            cwd="/tmp",
            exit_code=0,
            client_id="blq-test",
        )
        bird_store.write_invocation(inv)

        count = bird_store.write_events(inv.id, [], client_id="blq-test")

        assert count == 0
        assert bird_store.event_count() == 0


class TestOutputManagement:
    """Tests for output/blob storage."""

    def test_write_output_inline(self, bird_store):
        """Small outputs are stored inline."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="echo hello",
            cwd="/tmp",
            exit_code=0,
            client_id="blq-test",
        )
        bird_store.write_invocation(inv)

        content = b"hello world\n"
        output = bird_store.write_output(inv.id, "combined", content)

        assert output.storage_type == "inline"
        assert output.storage_ref.startswith("data:")
        assert output.byte_length == len(content)

    def test_write_output_blob(self, bird_store):
        """Large outputs are stored as blobs."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="cat bigfile",
            cwd="/tmp",
            exit_code=0,
            client_id="blq-test",
        )
        bird_store.write_invocation(inv)

        # Create content larger than inline threshold (4KB)
        content = b"x" * 5000
        output = bird_store.write_output(inv.id, "combined", content)

        assert output.storage_type == "blob"
        assert output.storage_ref.startswith("file:")
        assert output.byte_length == len(content)

        # Check blob file exists
        blob_path = bird_store._blob_dir / output.storage_ref.replace("file:", "")
        assert blob_path.exists()

    def test_blob_deduplication(self, bird_store):
        """Identical content is deduplicated."""
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="cat bigfile",
            cwd="/tmp",
            exit_code=0,
            client_id="blq-test",
        )
        bird_store.write_invocation(inv)

        content = b"y" * 5000

        # Write same content twice
        output1 = bird_store.write_output(inv.id, "stdout", content)
        output2 = bird_store.write_output(inv.id, "stderr", content)

        # Same hash
        assert output1.content_hash == output2.content_hash


class TestWriteBirdInvocation:
    """Tests for the write_bird_invocation helper function."""

    def test_write_bird_invocation(self, temp_dir):
        """write_bird_invocation creates complete invocation."""
        lq_dir = temp_dir / ".lq"
        lq_dir.mkdir()
        (lq_dir / "blobs" / "content").mkdir(parents=True)

        # Initialize schema
        store = BirdStore.open(lq_dir)
        store.close()

        events = [
            {"severity": "error", "message": "test error", "file_path": "test.py"},
        ]
        run_meta = {
            "source_name": "test",
            "source_type": "run",
            "command": "pytest",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "exit_code": 1,
            "cwd": str(temp_dir),
            "hostname": "testhost",
        }

        inv_id, db_path = write_bird_invocation(events, run_meta, lq_dir)

        assert inv_id is not None
        assert db_path.exists()

        # Verify data was written
        store = BirdStore.open(lq_dir)
        assert store.invocation_count() == 1
        assert store.event_count() == 1
        store.close()

    def test_write_bird_invocation_with_output(self, temp_dir):
        """write_bird_invocation stores output when provided."""
        lq_dir = temp_dir / ".lq"
        lq_dir.mkdir()
        (lq_dir / "blobs" / "content").mkdir(parents=True)

        store = BirdStore.open(lq_dir)
        store.close()

        run_meta = {
            "source_name": "test",
            "source_type": "run",
            "command": "echo hello",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "exit_code": 0,
            "cwd": str(temp_dir),
        }
        output = b"hello world\n"

        inv_id, _ = write_bird_invocation([], run_meta, lq_dir, output=output)

        # Verify output was written
        store = BirdStore.open(lq_dir)
        result = store.connection.execute(
            "SELECT COUNT(*) FROM outputs WHERE invocation_id = ?", [inv_id]
        ).fetchone()
        assert result[0] == 1
        store.close()


class TestBirdInit:
    """Tests for blq init --bird."""

    def test_init_bird_creates_structure(self, bird_initialized_dir):
        """blq init --bird creates correct directory structure."""
        lq_dir = bird_initialized_dir / ".lq"

        assert lq_dir.exists()
        assert (lq_dir / "blq.duckdb").exists()
        assert (lq_dir / "blobs" / "content").exists()
        assert (lq_dir / "schema.sql").exists()
        assert (lq_dir / "config.yaml").exists()

    def test_init_bird_sets_storage_mode(self, bird_initialized_dir):
        """blq init --bird sets storage mode in config."""
        config = BlqConfig.load(bird_initialized_dir / ".lq")

        assert config.storage_mode == "bird"
        assert config.use_bird is True

    def test_init_bird_schema_works(self, bird_initialized_dir):
        """BIRD schema is functional after init."""
        lq_dir = bird_initialized_dir / ".lq"

        store = BirdStore.open(lq_dir)

        # Can write invocations
        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="echo test",
            cwd=str(bird_initialized_dir),
            exit_code=0,
            client_id="blq-test",
        )
        store.write_invocation(inv)

        assert store.invocation_count() == 1
        store.close()


class TestBirdCompatibilityViews:
    """Tests for backward compatibility views."""

    def test_blq_events_flat_view(self, bird_store):
        """blq_events_flat view provides v1-compatible schema."""
        # Create session, invocation, and events
        bird_store.ensure_session("test", "blq-test", "blq", "cli")

        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="make build",
            cwd="/tmp",
            exit_code=1,
            client_id="blq-test",
            source_name="build",
            source_type="run",
            hostname="testhost",
        )
        bird_store.write_invocation(inv)

        events = [
            {
                "severity": "error",
                "file_path": "src/main.c",
                "line_number": 10,
                "message": "error message",
            },
        ]
        bird_store.write_events(inv.id, events, client_id="blq-test")

        # Query through compatibility view
        result = bird_store.connection.execute(
            "SELECT source_name, severity, file_path, message FROM blq_events_flat"
        ).fetchone()

        assert result[0] == "build"
        assert result[1] == "error"
        assert result[2] == "src/main.c"

    def test_blq_load_events_macro(self, bird_store):
        """blq_load_events() macro works with BIRD schema."""
        bird_store.ensure_session("test", "blq-test", "blq", "cli")

        inv = InvocationRecord(
            id=str(uuid.uuid4()),
            session_id="test",
            cmd="pytest",
            cwd="/tmp",
            exit_code=0,
            client_id="blq-test",
            source_name="test",
        )
        bird_store.write_invocation(inv)

        # Use the macro
        result = bird_store.connection.execute(
            "SELECT COUNT(*) FROM blq_load_events()"
        ).fetchone()

        # Should return 0 events (invocation exists but no events)
        assert result[0] == 0


class TestSQLSplitting:
    """Tests for SQL statement splitting."""

    def test_split_simple_statements(self):
        """Simple statements are split correctly."""
        sql = "SELECT 1; SELECT 2; SELECT 3;"
        statements = BirdStore._split_sql_statements(sql)

        assert len(statements) == 3
        assert statements[0] == "SELECT 1"
        assert statements[1] == "SELECT 2"
        assert statements[2] == "SELECT 3"

    def test_split_with_comments(self):
        """Comments are preserved but don't break splitting."""
        sql = """
        -- This is a comment
        SELECT 1;
        /* Block comment */
        SELECT 2;
        """
        statements = BirdStore._split_sql_statements(sql)

        assert len(statements) == 2

    def test_split_complex_macro(self):
        """Complex macros with semicolons in subqueries are handled."""
        sql = """
        CREATE MACRO test() AS TABLE
        SELECT * FROM (SELECT 1 AS a UNION SELECT 2);

        SELECT 'done';
        """
        statements = BirdStore._split_sql_statements(sql)

        assert len(statements) == 2
        assert "CREATE MACRO" in statements[0]
        assert "SELECT 'done'" in statements[1]


@pytest.fixture
def parquet_initialized_dir(temp_dir):
    """Initialize a directory with parquet (v1) mode and some test data."""
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    try:
        # Initialize without BIRD mode (parquet)
        args = argparse.Namespace()
        args.mcp = False
        args.detect = False
        args.detect_mode = "none"
        args.yes = False
        args.force = False
        args.bird = False
        args.namespace = "test"
        args.project = "parquet-test"

        cmd_init(args)

        # Write some test parquet data
        lq_dir = temp_dir / ".lq"
        events = [
            {
                "event_id": 0,
                "severity": "error",
                "file_path": "src/main.c",
                "line_number": 10,
                "message": "undefined reference",
                "tool_name": "gcc",
            },
            {
                "event_id": 1,
                "severity": "warning",
                "file_path": "src/util.c",
                "line_number": 25,
                "message": "unused variable",
                "tool_name": "gcc",
            },
        ]
        run_meta = {
            "run_id": 1,
            "source_name": "build",
            "source_type": "run",
            "command": "make build",
            "started_at": "2024-01-15T10:00:00",
            "completed_at": "2024-01-15T10:01:00",
            "exit_code": 1,
            "cwd": str(temp_dir),
            "hostname": "testhost",
            "platform": "Linux",
            "arch": "x86_64",
        }

        write_run_parquet(events, run_meta, lq_dir)

        # Write a second run with no events
        run_meta2 = {
            "run_id": 2,
            "source_name": "test",
            "source_type": "run",
            "command": "pytest",
            "started_at": "2024-01-15T11:00:00",
            "completed_at": "2024-01-15T11:02:00",
            "exit_code": 0,
            "cwd": str(temp_dir),
            "hostname": "testhost",
        }
        write_run_parquet([{}], run_meta2, lq_dir)

        yield temp_dir
    finally:
        os.chdir(original_cwd)


class TestMigration:
    """Tests for parquet to BIRD migration."""

    def test_migrate_dry_run(self, parquet_initialized_dir):
        """Dry run shows what would be migrated."""
        config = BlqConfig.load(parquet_initialized_dir / ".lq")

        invocations, events = _migrate_parquet_to_bird(
            config, dry_run=True, verbose=False
        )

        assert invocations == 2  # Two runs
        assert events == 2  # Two events (error + warning)

        # Config should not be changed
        assert config.storage_mode == "parquet"

    def test_migrate_actual(self, parquet_initialized_dir):
        """Migration converts parquet data to BIRD."""
        config = BlqConfig.load(parquet_initialized_dir / ".lq")
        lq_dir = parquet_initialized_dir / ".lq"

        invocations, events = _migrate_parquet_to_bird(
            config, dry_run=False, verbose=False
        )

        assert invocations == 2
        assert events == 2

        # Verify BIRD data
        store = BirdStore.open(lq_dir)
        assert store.invocation_count() == 2
        assert store.event_count() == 2

        # Check invocation details
        recent = store.recent_invocations(10)
        cmds = {r["cmd"] for r in recent}
        assert "make build" in cmds
        assert "pytest" in cmds

        store.close()

    def test_migrate_preserves_metadata(self, parquet_initialized_dir):
        """Migration preserves all metadata fields."""
        config = BlqConfig.load(parquet_initialized_dir / ".lq")
        lq_dir = parquet_initialized_dir / ".lq"

        _migrate_parquet_to_bird(config, dry_run=False, verbose=False)

        store = BirdStore.open(lq_dir)

        # Check that metadata was preserved
        result = store.connection.execute("""
            SELECT hostname, platform, source_name
            FROM invocations
            WHERE cmd = 'make build'
        """).fetchone()

        assert result[0] == "testhost"
        assert result[1] == "Linux"
        assert result[2] == "build"

        store.close()

    def test_cmd_migrate_to_bird(self, parquet_initialized_dir):
        """blq migrate --to-bird command works."""
        os.chdir(parquet_initialized_dir)

        args = argparse.Namespace()
        args.to_bird = True
        args.dry_run = False
        args.keep_parquet = True
        args.force = False
        args.verbose = False

        cmd_migrate(args)

        # Config should now be BIRD mode
        config = BlqConfig.load(parquet_initialized_dir / ".lq")
        assert config.storage_mode == "bird"

    def test_migrate_no_data(self, temp_dir):
        """Migration handles empty directory gracefully."""
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        try:
            # Initialize without BIRD mode but don't add data
            args = argparse.Namespace()
            args.mcp = False
            args.detect = False
            args.detect_mode = "none"
            args.yes = False
            args.force = False
            args.bird = False
            args.namespace = "test"
            args.project = "empty-test"

            cmd_init(args)

            config = BlqConfig.load(temp_dir / ".lq")
            invocations, events = _migrate_parquet_to_bird(
                config, dry_run=False, verbose=False
            )

            # Should handle gracefully with no data
            assert invocations == 0
            assert events == 0
        finally:
            os.chdir(original_cwd)
