"""
BIRD (Buffer and Invocation Record Database) storage backend for blq.

This module implements the BIRD specification using DuckDB tables (single-writer mode).
All reads go through views, writes go directly to tables.

BIRD spec: https://github.com/teaguesterling/magic/blob/main/docs/bird_spec.md
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

# Schema version
BIRD_SCHEMA_VERSION = "2.0.0"

# Storage thresholds
DEFAULT_INLINE_THRESHOLD = 4096  # 4KB - outputs smaller than this are stored inline


@dataclass
class SessionRecord:
    """A BIRD session (invoker context)."""

    session_id: str
    client_id: str
    invoker: str
    invoker_type: str  # "cli", "mcp", "import", "capture"
    invoker_pid: int | None = None
    cwd: str | None = None
    registered_at: datetime = field(default_factory=datetime.now)
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))


@dataclass
class InvocationRecord:
    """A BIRD invocation (command execution)."""

    # Identity
    id: str  # UUID
    session_id: str

    # Command
    cmd: str
    cwd: str
    exit_code: int

    # Client
    client_id: str

    # Timing
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: int | None = None

    # Optional fields
    executable: str | None = None
    format_hint: str | None = None
    hostname: str | None = None
    username: str | None = None

    # blq-specific fields
    source_name: str | None = None
    source_type: str | None = None
    environment: dict[str, str] | None = None
    platform: str | None = None
    arch: str | None = None
    git_commit: str | None = None
    git_branch: str | None = None
    git_dirty: bool | None = None
    ci: dict[str, str] | None = None

    # Partitioning
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @classmethod
    def generate_id(cls) -> str:
        """Generate a new UUID for an invocation."""
        # TODO: Use UUIDv7 when available for time-ordered IDs
        return str(uuid.uuid4())


@dataclass
class OutputRecord:
    """A BIRD output (captured stdout/stderr)."""

    id: str  # UUID
    invocation_id: str
    stream: str  # 'stdout', 'stderr', 'combined'
    content_hash: str  # BLAKE3 hash
    byte_length: int
    storage_type: str  # 'inline' or 'blob'
    storage_ref: str  # data: URI or file: path
    content_type: str | None = None
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))


@dataclass
class EventRecord:
    """A BIRD event (parsed diagnostic)."""

    id: str  # UUID
    invocation_id: str
    event_index: int
    client_id: str

    # Classification
    severity: str | None = None
    event_type: str | None = None

    # Location
    file_path: str | None = None
    line_number: int | None = None
    column_number: int | None = None

    # Content
    message: str | None = None
    code: str | None = None
    rule: str | None = None

    # blq-specific
    tool_name: str | None = None
    category: str | None = None
    fingerprint: str | None = None
    log_line_start: int | None = None
    log_line_end: int | None = None
    context: str | None = None
    metadata: dict | None = None

    # Parsing metadata
    format_used: str | None = None
    hostname: str | None = None
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))


class BirdStore:
    """BIRD storage backend using DuckDB tables.

    This class manages BIRD-compatible storage in DuckDB mode (single-writer).
    It handles sessions, invocations, outputs, and events tables.

    Example:
        store = BirdStore.open(".lq/")

        # Register session (once per CLI invocation)
        store.ensure_session("test", "blq-shell", "blq", "cli")

        # Write invocation
        inv_id = store.write_invocation(record)

        # Write events
        store.write_events(inv_id, events)
    """

    def __init__(self, lq_dir: Path, conn: duckdb.DuckDBPyConnection):
        """Initialize BirdStore.

        Args:
            lq_dir: Path to .lq directory
            conn: Open DuckDB connection
        """
        self._lq_dir = lq_dir
        self._conn = conn
        self._blob_dir = lq_dir / "blobs" / "content"
        self._inline_threshold = DEFAULT_INLINE_THRESHOLD

    @classmethod
    def open(cls, lq_dir: Path | str) -> BirdStore:
        """Open or create a BirdStore.

        Args:
            lq_dir: Path to .lq directory

        Returns:
            BirdStore instance
        """
        lq_dir = Path(lq_dir)
        db_path = lq_dir / "blq.duckdb"

        # Open database
        conn = duckdb.connect(str(db_path))

        # Initialize schema if needed
        cls._ensure_schema(conn, lq_dir)

        return cls(lq_dir, conn)

    @classmethod
    def _ensure_schema(cls, conn: duckdb.DuckDBPyConnection, lq_dir: Path) -> None:
        """Ensure BIRD schema is initialized."""
        # Check if schema is already initialized
        try:
            result = conn.execute(
                "SELECT value FROM blq_metadata WHERE key = 'schema_version'"
            ).fetchone()
            if result:
                # Schema exists
                return
        except duckdb.Error:
            pass  # Table doesn't exist, need to create

        # Load schema from SQL file
        schema_path = Path(__file__).parent / "bird_schema.sql"
        if schema_path.exists():
            schema_sql = schema_path.read_text()
            # Execute statements using proper SQL splitting
            statements = cls._split_sql_statements(schema_sql)
            for stmt in statements:
                try:
                    conn.execute(stmt)
                except duckdb.Error as e:
                    # Log but continue - some statements may fail on re-init
                    if "already exists" not in str(e).lower():
                        pass  # Ignore

        # Create blob directory
        blob_dir = lq_dir / "blobs" / "content"
        blob_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _split_sql_statements(sql: str) -> list[str]:
        """Split SQL into individual statements, handling comments and semicolons.

        Simple parser that handles:
        - Line comments (--)
        - Block comments (/* */)
        - Semicolons inside comments

        Returns list of non-empty statements.
        """
        statements = []
        current = []
        in_line_comment = False
        in_block_comment = False
        i = 0

        while i < len(sql):
            c = sql[i]

            # Check for comment start
            if not in_line_comment and not in_block_comment:
                if c == "-" and i + 1 < len(sql) and sql[i + 1] == "-":
                    in_line_comment = True
                    current.append(c)
                    i += 1
                    current.append(sql[i])
                elif c == "/" and i + 1 < len(sql) and sql[i + 1] == "*":
                    in_block_comment = True
                    current.append(c)
                    i += 1
                    current.append(sql[i])
                elif c == ";":
                    # End of statement
                    stmt = "".join(current).strip()
                    if stmt:
                        statements.append(stmt)
                    current = []
                else:
                    current.append(c)
            elif in_line_comment:
                current.append(c)
                if c == "\n":
                    in_line_comment = False
            elif in_block_comment:
                current.append(c)
                if c == "*" and i + 1 < len(sql) and sql[i + 1] == "/":
                    i += 1
                    current.append(sql[i])
                    in_block_comment = False

            i += 1

        # Add final statement if any
        stmt = "".join(current).strip()
        if stmt:
            statements.append(stmt)

        return statements

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> BirdStore:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # =========================================================================
    # Session Management
    # =========================================================================

    def ensure_session(
        self,
        session_id: str,
        client_id: str,
        invoker: str,
        invoker_type: str,
        cwd: str | None = None,
    ) -> None:
        """Ensure a session exists, creating if needed.

        Args:
            session_id: Session identifier (e.g., source_name for CLI)
            client_id: Client identifier (e.g., "blq-shell")
            invoker: Invoker name (e.g., "blq")
            invoker_type: Invoker type ("cli", "mcp", "import", "capture")
            cwd: Initial working directory
        """
        # Check if session exists
        result = self._conn.execute(
            "SELECT 1 FROM sessions WHERE session_id = ?", [session_id]
        ).fetchone()

        if result:
            return  # Session already exists

        # Create session
        pid = os.getpid()
        now = datetime.now()
        date = now.strftime("%Y-%m-%d")

        self._conn.execute(
            """
            INSERT INTO sessions (session_id, client_id, invoker, invoker_pid,
                                  invoker_type, registered_at, cwd, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [session_id, client_id, invoker, pid, invoker_type, now, cwd, date],
        )

    # =========================================================================
    # Invocation Management
    # =========================================================================

    def write_invocation(self, record: InvocationRecord) -> str:
        """Write an invocation record.

        Args:
            record: Invocation record to write

        Returns:
            The invocation ID
        """
        self._conn.execute(
            """
            INSERT INTO invocations (
                id, session_id, timestamp, duration_ms, cwd, cmd, executable,
                exit_code, format_hint, client_id, hostname, username,
                source_name, source_type, environment, platform, arch,
                git_commit, git_branch, git_dirty, ci, date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record.id,
                record.session_id,
                record.timestamp,
                record.duration_ms,
                record.cwd,
                record.cmd,
                record.executable,
                record.exit_code,
                record.format_hint,
                record.client_id,
                record.hostname,
                record.username,
                record.source_name,
                record.source_type,
                json.dumps(record.environment) if record.environment else None,
                record.platform,
                record.arch,
                record.git_commit,
                record.git_branch,
                record.git_dirty,
                json.dumps(record.ci) if record.ci else None,
                record.date,
            ],
        )
        return record.id

    def get_next_run_number(self) -> int:
        """Get the next run number (for backward compatibility).

        Returns:
            Next sequential run number
        """
        result = self._conn.execute(
            "SELECT COUNT(*) FROM invocations"
        ).fetchone()
        return (result[0] if result else 0) + 1

    # =========================================================================
    # Output Management
    # =========================================================================

    def write_output(
        self,
        invocation_id: str,
        stream: str,
        content: bytes,
        content_type: str | None = None,
    ) -> OutputRecord:
        """Write output content, choosing inline or blob storage.

        Args:
            invocation_id: ID of the invocation
            stream: Stream name ('stdout', 'stderr', 'combined')
            content: Raw output bytes
            content_type: Optional MIME type

        Returns:
            OutputRecord with storage details
        """
        # Compute hash
        content_hash = hashlib.blake2b(content, digest_size=32).hexdigest()
        byte_length = len(content)

        # Determine storage type
        if byte_length < self._inline_threshold:
            # Inline storage as data: URI
            import base64

            b64 = base64.b64encode(content).decode("ascii")
            storage_type = "inline"
            storage_ref = f"data:application/octet-stream;base64,{b64}"
        else:
            # Blob storage
            storage_path = self._write_blob(content_hash, content)
            storage_type = "blob"
            storage_ref = f"file:{storage_path}"

        # Create record
        record = OutputRecord(
            id=str(uuid.uuid4()),
            invocation_id=invocation_id,
            stream=stream,
            content_hash=content_hash,
            byte_length=byte_length,
            storage_type=storage_type,
            storage_ref=storage_ref,
            content_type=content_type,
        )

        # Write to database
        self._conn.execute(
            """
            INSERT INTO outputs (
                id, invocation_id, stream, content_hash, byte_length,
                storage_type, storage_ref, content_type, date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record.id,
                record.invocation_id,
                record.stream,
                record.content_hash,
                record.byte_length,
                record.storage_type,
                record.storage_ref,
                record.content_type,
                record.date,
            ],
        )

        return record

    def _write_blob(self, content_hash: str, content: bytes) -> str:
        """Write content to blob storage.

        Args:
            content_hash: BLAKE2b hash of content
            content: Raw bytes

        Returns:
            Relative path to blob file
        """
        # Create subdirectory based on first 2 chars of hash
        subdir = content_hash[:2]
        blob_subdir = self._blob_dir / subdir
        blob_subdir.mkdir(parents=True, exist_ok=True)

        # Write blob file
        blob_path = blob_subdir / f"{content_hash}.bin"
        relative_path = f"{subdir}/{content_hash}.bin"

        # Atomic write with temp file
        temp_path = blob_subdir / f".tmp.{content_hash}.bin"
        try:
            temp_path.write_bytes(content)
            temp_path.rename(blob_path)
        except FileExistsError:
            # Another process wrote the same blob - that's fine
            temp_path.unlink(missing_ok=True)

        # Update blob registry
        self._register_blob(content_hash, len(content), relative_path)

        return relative_path

    def _register_blob(
        self, content_hash: str, byte_length: int, storage_path: str
    ) -> None:
        """Register or update blob in registry."""
        try:
            # Try insert
            self._conn.execute(
                """
                INSERT INTO blob_registry (content_hash, byte_length, storage_path)
                VALUES (?, ?, ?)
                """,
                [content_hash, byte_length, storage_path],
            )
        except duckdb.Error:
            # Already exists, update access time and ref count
            self._conn.execute(
                """
                UPDATE blob_registry
                SET last_accessed = CURRENT_TIMESTAMP, ref_count = ref_count + 1
                WHERE content_hash = ?
                """,
                [content_hash],
            )

    # =========================================================================
    # Event Management
    # =========================================================================

    def write_events(
        self,
        invocation_id: str,
        events: list[dict[str, Any]],
        client_id: str,
        format_used: str | None = None,
        hostname: str | None = None,
    ) -> int:
        """Write parsed events for an invocation.

        Args:
            invocation_id: ID of the invocation
            events: List of parsed event dicts
            client_id: Client identifier
            format_used: Parser format used
            hostname: Hostname (denormalized)

        Returns:
            Number of events written
        """
        if not events:
            return 0

        date = datetime.now().strftime("%Y-%m-%d")

        for idx, event in enumerate(events):
            event_id = str(uuid.uuid4())

            self._conn.execute(
                """
                INSERT INTO events (
                    id, invocation_id, event_index, client_id, hostname,
                    event_type, severity, file_path, line_number, column_number,
                    message, code, rule, tool_name, category, fingerprint,
                    log_line_start, log_line_end, context, metadata,
                    format_used, date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    event_id,
                    invocation_id,
                    event.get("event_id", idx),  # Use event_id if provided
                    client_id,
                    hostname,
                    event.get("event_type"),
                    event.get("severity"),
                    event.get("file_path"),
                    event.get("line_number"),
                    event.get("column_number"),
                    event.get("message"),
                    event.get("error_code") or event.get("code"),
                    event.get("rule"),
                    event.get("tool_name"),
                    event.get("category"),
                    event.get("fingerprint"),
                    event.get("log_line_start"),
                    event.get("log_line_end"),
                    event.get("context"),
                    json.dumps(event.get("metadata")) if event.get("metadata") else None,
                    format_used,
                    date,
                ],
            )

        return len(events)

    # =========================================================================
    # Query Helpers
    # =========================================================================

    def invocation_count(self) -> int:
        """Get total number of invocations."""
        result = self._conn.execute("SELECT COUNT(*) FROM invocations").fetchone()
        return result[0] if result else 0

    def event_count(self) -> int:
        """Get total number of events."""
        result = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()
        return result[0] if result else 0

    def recent_invocations(self, limit: int = 10) -> list[dict]:
        """Get recent invocations.

        Args:
            limit: Maximum number to return

        Returns:
            List of invocation dicts
        """
        result = self._conn.execute(
            """
            SELECT id, session_id, timestamp, duration_ms, cmd, exit_code,
                   source_name, source_type
            FROM invocations
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()

        columns = [
            "id", "session_id", "timestamp", "duration_ms", "cmd", "exit_code",
            "source_name", "source_type"
        ]
        return [dict(zip(columns, row)) for row in result]

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get the underlying DuckDB connection for direct queries."""
        return self._conn


def write_bird_invocation(
    events: list[dict[str, Any]],
    run_meta: dict[str, Any],
    lq_dir: Path,
    output: bytes | None = None,
) -> tuple[str, Path]:
    """Write a run to BIRD storage.

    This is the main entry point for writing invocations, replacing
    write_run_parquet() for BIRD-enabled projects.

    Args:
        events: Parsed events from the command output
        run_meta: Run metadata dict (same format as write_run_parquet)
        lq_dir: Path to .lq directory
        output: Optional raw output bytes to store

    Returns:
        Tuple of (invocation_id, db_path)
    """
    with BirdStore.open(lq_dir) as store:
        # Determine session and client IDs
        source_name = run_meta.get("source_name", "unknown")
        source_type = run_meta.get("source_type", "run")
        client_id = f"blq-{source_type}"

        # For CLI runs, session_id = source_name
        # For exec, session_id = "exec-{date}"
        if source_type == "run":
            session_id = source_name
        else:
            session_id = f"{source_type}-{datetime.now().strftime('%Y-%m-%d')}"

        # Ensure session exists
        store.ensure_session(
            session_id=session_id,
            client_id=client_id,
            invoker="blq",
            invoker_type="cli",
            cwd=run_meta.get("cwd"),
        )

        # Calculate duration
        started_at = run_meta.get("started_at")
        completed_at = run_meta.get("completed_at")
        duration_ms = None
        if started_at and completed_at:
            try:
                start = datetime.fromisoformat(started_at)
                end = datetime.fromisoformat(completed_at)
                duration_ms = int((end - start).total_seconds() * 1000)
            except (ValueError, TypeError):
                pass

        # Create invocation record
        invocation = InvocationRecord(
            id=InvocationRecord.generate_id(),
            session_id=session_id,
            cmd=run_meta.get("command", ""),
            cwd=run_meta.get("cwd", os.getcwd()),
            exit_code=run_meta.get("exit_code", 0),
            client_id=client_id,
            timestamp=datetime.now(),
            duration_ms=duration_ms,
            executable=run_meta.get("executable_path"),
            format_hint=run_meta.get("format_hint"),
            hostname=run_meta.get("hostname"),
            username=run_meta.get("username"),
            source_name=source_name,
            source_type=source_type,
            environment=run_meta.get("environment"),
            platform=run_meta.get("platform"),
            arch=run_meta.get("arch"),
            git_commit=run_meta.get("git_commit"),
            git_branch=run_meta.get("git_branch"),
            git_dirty=run_meta.get("git_dirty"),
            ci=run_meta.get("ci"),
        )

        # Write invocation
        inv_id = store.write_invocation(invocation)

        # Write output if provided
        if output is not None:
            store.write_output(inv_id, "combined", output)

        # Write events
        hostname = run_meta.get("hostname")
        store.write_events(
            inv_id,
            events,
            client_id=client_id,
            format_used=run_meta.get("format_hint"),
            hostname=hostname,
        )

        return inv_id, lq_dir / "blq.duckdb"
