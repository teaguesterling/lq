"""Tests for the LogQuery and LogStore API."""

import os
from pathlib import Path

import duckdb
import pytest

from blq.query import LogQuery, LogStore

# ============================================================================
# LogQuery Tests
# ============================================================================


class TestLogQueryBasic:
    """Basic LogQuery functionality tests."""

    @pytest.fixture
    def conn_with_data(self):
        """Create a connection with sample data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE events AS
            SELECT * FROM (VALUES
                (1, 'error', 'main.c', 10, 'undefined var foo'),
                (2, 'warning', 'utils.c', 20, 'unused var bar'),
                (3, 'error', 'main.c', 30, 'missing semicolon'),
                (4, 'info', 'test.c', 40, 'test passed'),
                (5, 'warning', 'main.c', 50, 'deprecated function')
            ) AS t(event_id, severity, file_path, line_number, message)
        """)
        return conn

    def test_from_table(self, conn_with_data):
        """Create LogQuery from table."""
        query = LogQuery.from_table(conn_with_data, "events")
        df = query.df()
        assert len(df) == 5
        assert "severity" in df.columns

    def test_from_sql(self, conn_with_data):
        """Create LogQuery from SQL."""
        query = LogQuery.from_sql(conn_with_data, "SELECT * FROM events WHERE event_id < 3")
        df = query.df()
        assert len(df) == 2

    def test_columns_property(self, conn_with_data):
        """Access column names."""
        query = LogQuery.from_table(conn_with_data, "events")
        assert "severity" in query.columns
        assert "file_path" in query.columns

    def test_count(self, conn_with_data):
        """Count rows."""
        query = LogQuery.from_table(conn_with_data, "events")
        assert query.count() == 5

    def test_exists(self, conn_with_data):
        """Check if rows exist."""
        query = LogQuery.from_table(conn_with_data, "events")
        assert query.filter(severity="error").exists() is True
        assert query.filter(severity="critical").exists() is False


class TestLogQueryFilter:
    """LogQuery filter functionality tests."""

    @pytest.fixture
    def query(self):
        """Create a LogQuery with sample data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE events AS
            SELECT * FROM (VALUES
                (1, 'error', 'main.c', 10, 'undefined var foo'),
                (2, 'warning', 'utils.c', 20, 'unused var bar'),
                (3, 'error', 'main.c', 30, 'missing semicolon'),
                (4, 'info', 'test.c', 40, 'test passed'),
                (5, 'warning', 'main.c', 50, 'deprecated function')
            ) AS t(event_id, severity, file_path, line_number, message)
        """)
        return LogQuery.from_table(conn, "events")

    def test_filter_exact_match(self, query):
        """Filter by exact value."""
        df = query.filter(severity="error").df()
        assert len(df) == 2
        assert all(df["severity"] == "error")

    def test_filter_list_values(self, query):
        """Filter by list of values (IN clause)."""
        df = query.filter(severity=["error", "warning"]).df()
        assert len(df) == 4
        assert set(df["severity"].unique()) == {"error", "warning"}

    def test_filter_like_pattern(self, query):
        """Filter with LIKE pattern."""
        df = query.filter(file_path="%main%").df()
        assert len(df) == 3
        assert all("main" in fp for fp in df["file_path"])

    def test_filter_sql_condition(self, query):
        """Filter with raw SQL condition."""
        df = query.filter("line_number > 25").df()
        assert len(df) == 3
        assert all(df["line_number"] > 25)

    def test_filter_multiple_conditions(self, query):
        """Filter with multiple conditions (AND)."""
        df = query.filter(severity="error", file_path="main.c").df()
        assert len(df) == 2

    def test_filter_chained(self, query):
        """Chain multiple filter calls."""
        df = query.filter(severity=["error", "warning"]).filter(file_path="%main%").df()
        assert len(df) == 3

    def test_exclude(self, query):
        """Exclude rows matching condition."""
        df = query.exclude(severity="info").df()
        assert len(df) == 4
        assert "info" not in df["severity"].values

    def test_where_raw_sql(self, query):
        """Use where() for raw SQL."""
        df = query.where("line_number BETWEEN 20 AND 40").df()
        assert len(df) == 3


class TestLogQueryProjection:
    """LogQuery select/order/limit tests."""

    @pytest.fixture
    def query(self):
        """Create a LogQuery with sample data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE events AS
            SELECT * FROM (VALUES
                (1, 'error', 'main.c', 10, 'msg1'),
                (2, 'warning', 'utils.c', 20, 'msg2'),
                (3, 'error', 'test.c', 30, 'msg3')
            ) AS t(event_id, severity, file_path, line_number, message)
        """)
        return LogQuery.from_table(conn, "events")

    def test_select_columns(self, query):
        """Select specific columns."""
        df = query.select("severity", "message").df()
        assert list(df.columns) == ["severity", "message"]

    def test_order_by(self, query):
        """Order results."""
        df = query.order_by("line_number").df()
        assert list(df["line_number"]) == [10, 20, 30]

    def test_order_by_desc(self, query):
        """Order results descending."""
        df = query.order_by("line_number", desc=True).df()
        assert list(df["line_number"]) == [30, 20, 10]

    def test_limit(self, query):
        """Limit results."""
        df = query.limit(2).df()
        assert len(df) == 2

    def test_combined_operations(self, query):
        """Combine filter, select, order, limit."""
        df = (
            query.filter(severity=["error", "warning"])
            .select("file_path", "message")
            .order_by("file_path")
            .limit(2)
            .df()
        )
        assert len(df) == 2
        assert list(df.columns) == ["file_path", "message"]


class TestLogQueryAggregation:
    """LogQuery aggregation tests."""

    @pytest.fixture
    def query(self):
        """Create a LogQuery with sample data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE events AS
            SELECT * FROM (VALUES
                (1, 'error', 'main.c', 10),
                (2, 'warning', 'main.c', 20),
                (3, 'error', 'utils.c', 30),
                (4, 'error', 'main.c', 40)
            ) AS t(event_id, severity, file_path, line_number)
        """)
        return LogQuery.from_table(conn, "events")

    def test_value_counts(self, query):
        """Count value occurrences."""
        df = query.value_counts("severity")
        assert len(df) == 2
        # Errors should be first (most common)
        assert df.iloc[0]["severity"] == "error"
        assert df.iloc[0]["count"] == 3

    def test_group_by_count(self, query):
        """Group by and count."""
        df = query.group_by("file_path").count()
        assert len(df) == 2

    def test_group_by_with_filter(self, query):
        """Group by with pre-filter."""
        df = query.filter(severity="error").group_by("file_path").count()
        # Only errors grouped by file
        assert len(df) == 2


class TestLogQueryExecution:
    """LogQuery execution method tests."""

    @pytest.fixture
    def query(self):
        """Create a LogQuery with sample data."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE events AS
            SELECT * FROM (VALUES
                (1, 'error', 'msg1'),
                (2, 'warning', 'msg2')
            ) AS t(event_id, severity, message)
        """)
        return LogQuery.from_table(conn, "events")

    def test_fetchall(self, query):
        """Fetch all as tuples."""
        rows = query.fetchall()
        assert len(rows) == 2
        assert isinstance(rows[0], tuple)

    def test_fetchone(self, query):
        """Fetch single row."""
        row = query.filter(event_id=1).fetchone()
        assert row is not None
        assert row[0] == 1

    def test_fetchone_no_match(self, query):
        """Fetch returns None when no match."""
        row = query.filter(event_id=999).fetchone()
        assert row is None


# ============================================================================
# LogStore Tests
# ============================================================================


class TestLogStore:
    """LogStore functionality tests."""

    def test_open_finds_lq_dir(self, initialized_project):
        """Open finds .lq in current directory."""
        store = LogStore.open()
        assert store.path.name == ".lq"
        assert store.path.exists()

    def test_open_explicit_path(self, initialized_project):
        """Open with explicit path."""
        store = LogStore.open(Path(".lq"))
        assert store.path.exists()

    def test_open_not_found(self, temp_dir):
        """Open raises when .lq not found."""
        original = os.getcwd()
        try:
            os.chdir(temp_dir)
            with pytest.raises(FileNotFoundError):
                LogStore.open()
        finally:
            os.chdir(original)

    def test_events_returns_query(self, initialized_project, sample_build_script, run_adhoc_command):
        """events() returns a LogQuery."""
        # Create some data
        run_adhoc_command([str(sample_build_script)])

        store = LogStore.open()
        query = store.events()
        assert isinstance(query, LogQuery)
        assert query.count() > 0

    def test_errors_convenience(self, initialized_project, sample_build_script, run_adhoc_command):
        """errors() returns filtered query."""
        run_adhoc_command([str(sample_build_script)])

        store = LogStore.open()
        errors = store.errors().df()
        assert all(errors["severity"] == "error")

    def test_warnings_convenience(self, initialized_project, sample_build_script, run_adhoc_command):
        """warnings() returns filtered query."""
        run_adhoc_command([str(sample_build_script)])

        store = LogStore.open()
        warnings = store.warnings().df()
        assert all(warnings["severity"] == "warning")

    def test_latest_run(self, initialized_project, sample_build_script, run_adhoc_command):
        """latest_run() returns the latest run ID."""
        run_adhoc_command([str(sample_build_script)])

        store = LogStore.open()
        run_id = store.latest_run()
        assert run_id == 1

    def test_run_filter(self, initialized_project, sample_build_script, run_adhoc_command):
        """run() filters by run_id."""
        run_adhoc_command([str(sample_build_script)])

        store = LogStore.open()
        events = store.run(1).df()
        assert all(events["run_id"] == 1)

    def test_has_data(self, initialized_project, sample_build_script, run_adhoc_command):
        """has_data() checks for parquet files."""
        store = LogStore.open()
        assert store.has_data() is False

        run_adhoc_command([str(sample_build_script)])

        assert store.has_data() is True

    def test_connection_property(self, initialized_project):
        """connection property returns DuckDB connection."""
        store = LogStore.open()
        conn = store.connection
        assert isinstance(conn, duckdb.DuckDBPyConnection)


class TestLogStoreChaining:
    """Test chaining queries from LogStore."""

    def test_full_query_chain(self, initialized_project, sample_build_script, run_adhoc_command):
        """Full query chain from store."""
        run_adhoc_command([str(sample_build_script)])

        store = LogStore.open()
        result = (
            store.events()
            .filter(severity="error")
            .select("file_path", "message")
            .order_by("file_path")
            .limit(5)
            .df()
        )

        assert len(result) <= 5
        assert list(result.columns) == ["file_path", "message"]
