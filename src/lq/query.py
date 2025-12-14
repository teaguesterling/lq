"""
Pythonic query API for lq.

This module provides a fluent interface for querying log data,
built on top of DuckDB's relational API.

Example usage:
    from lq.query import LogQuery, LogStore

    # Query stored events
    store = LogStore.open()
    results = (
        store.events()
        .filter(severity="error")
        .filter(file_path="%main%")  # LIKE pattern
        .select("file_path", "line_number", "message")
        .order_by("line_number")
        .limit(10)
        .df()
    )

    # Query a log file directly
    results = (
        LogQuery.from_file("build.log")
        .filter(severity=["error", "warning"])  # IN clause
        .df()
    )
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import duckdb
import pandas as pd


class LogQuery:
    """Fluent query builder for log data.

    Wraps a DuckDB relation and provides a Pythonic interface
    for filtering, selecting, ordering, and retrieving results.

    Operations are deferred and executed when a terminal method
    (df(), count(), fetchall(), etc.) is called.
    """

    def __init__(self, rel: duckdb.DuckDBPyRelation, conn: duckdb.DuckDBPyConnection):
        """Initialize with a DuckDB relation.

        Args:
            rel: The underlying DuckDB relation
            conn: The connection (kept for potential future operations)
        """
        self._rel = rel
        self._conn = conn
        self._filters: list[str] = []
        self._select_cols: list[str] | None = None
        self._order_cols: list[str] | None = None
        self._limit_n: int | None = None

    @classmethod
    def from_relation(
        cls,
        rel: duckdb.DuckDBPyRelation,
        conn: duckdb.DuckDBPyConnection,
    ) -> LogQuery:
        """Create a LogQuery from an existing relation."""
        return cls(rel, conn)

    @classmethod
    def from_sql(
        cls,
        conn: duckdb.DuckDBPyConnection,
        sql: str,
        params: Sequence[Any] | None = None,
    ) -> LogQuery:
        """Create a LogQuery from a SQL query.

        Args:
            conn: DuckDB connection
            sql: SQL query string
            params: Optional query parameters

        Returns:
            LogQuery wrapping the query result
        """
        if params:
            rel = conn.sql(sql, params=params)
        else:
            rel = conn.sql(sql)
        return cls(rel, conn)

    @classmethod
    def from_table(cls, conn: duckdb.DuckDBPyConnection, table_name: str) -> LogQuery:
        """Create a LogQuery from a table or view.

        Args:
            conn: DuckDB connection
            table_name: Name of table or view

        Returns:
            LogQuery wrapping the table
        """
        return cls(conn.table(table_name), conn)

    @classmethod
    def from_parquet(
        cls,
        path: str | Path,
        conn: duckdb.DuckDBPyConnection | None = None,
        hive_partitioning: bool = True,
    ) -> LogQuery:
        """Create a LogQuery from parquet file(s).

        Args:
            path: Path to parquet file or glob pattern
            conn: DuckDB connection (creates new if None)
            hive_partitioning: Enable Hive-style partitioning

        Returns:
            LogQuery wrapping the parquet data
        """
        if conn is None:
            conn = duckdb.connect(":memory:")

        path_str = str(path)
        rel = conn.read_parquet(path_str, hive_partitioning=hive_partitioning)
        return cls(rel, conn)

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        format: str = "auto",
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> LogQuery:
        """Create a LogQuery from a log file using duck_hunt.

        Args:
            path: Path to log file
            format: Log format hint (default: "auto")
            conn: DuckDB connection (creates new if None)

        Returns:
            LogQuery wrapping the parsed log data

        Raises:
            duckdb.Error: If duck_hunt extension not available
        """
        if conn is None:
            conn = duckdb.connect(":memory:")

        # Load duck_hunt extension
        try:
            conn.execute("LOAD duck_hunt")
        except duckdb.Error:
            raise duckdb.Error(
                "duck_hunt extension required for parsing log files. "
                "Run 'lq init' to install required extensions."
            )

        path_str = str(Path(path).resolve())
        rel = conn.sql(
            "SELECT * FROM read_duck_hunt_log($1, $2)",
            params=[path_str, format],
        )
        return cls(rel, conn)

    # -------------------------------------------------------------------------
    # Filter methods
    # -------------------------------------------------------------------------

    def filter(self, _condition: str | None = None, **kwargs: Any) -> LogQuery:
        """Filter rows by conditions.

        Can be called with a raw SQL condition string or keyword arguments.

        Keyword argument patterns:
            column=value         -> column = 'value'
            column=[v1, v2]      -> column IN ('v1', 'v2')
            column="%pattern%"   -> column ILIKE '%pattern%'

        Args:
            _condition: Raw SQL WHERE condition (optional)
            **kwargs: Column=value filter conditions

        Returns:
            Self for chaining

        Examples:
            .filter(severity="error")
            .filter(severity=["error", "warning"])
            .filter(file_path="%main%")
            .filter("line_number > 100")
        """
        if _condition:
            self._filters.append(_condition)

        for col, val in kwargs.items():
            condition = self._build_condition(col, val)
            self._filters.append(condition)

        return self

    def _build_condition(self, column: str, value: Any) -> str:
        """Build a SQL condition from a column and value."""
        if value is None:
            return f"{column} IS NULL"

        if isinstance(value, (list, tuple)):
            # IN clause for multiple values
            if not value:
                return "FALSE"  # Empty list matches nothing
            quoted = ", ".join(f"'{v}'" for v in value)
            return f"{column} IN ({quoted})"

        if isinstance(value, str):
            # Check for LIKE patterns
            if value.startswith("%") or value.endswith("%"):
                return f"{column} ILIKE '{value}'"
            # Check for NOT pattern
            if value.startswith("!"):
                return f"{column} != '{value[1:]}'"
            # Regular equality
            return f"{column} = '{value}'"

        if isinstance(value, bool):
            return f"{column} = {str(value).upper()}"

        if isinstance(value, (int, float)):
            return f"{column} = {value}"

        # Default to string comparison
        return f"{column} = '{value}'"

    def exclude(self, **kwargs: Any) -> LogQuery:
        """Exclude rows matching conditions (NOT filter).

        Args:
            **kwargs: Column=value conditions to exclude

        Returns:
            Self for chaining

        Example:
            .exclude(severity="info")  # NOT severity = 'info'
        """
        for col, val in kwargs.items():
            condition = self._build_condition(col, val)
            self._filters.append(f"NOT ({condition})")
        return self

    def where(self, condition: str) -> LogQuery:
        """Add a raw SQL WHERE condition.

        Args:
            condition: SQL condition string

        Returns:
            Self for chaining
        """
        self._filters.append(condition)
        return self

    # -------------------------------------------------------------------------
    # Projection methods
    # -------------------------------------------------------------------------

    def select(self, *columns: str) -> LogQuery:
        """Select specific columns.

        Args:
            *columns: Column names to select

        Returns:
            Self for chaining

        Example:
            .select("file_path", "line_number", "message")
        """
        self._select_cols = list(columns)
        return self

    def order_by(self, *columns: str, desc: bool = False) -> LogQuery:
        """Order results by columns.

        Args:
            *columns: Column names to order by
            desc: If True, order descending

        Returns:
            Self for chaining

        Example:
            .order_by("line_number")
            .order_by("severity", "line_number", desc=True)
        """
        if desc:
            self._order_cols = [f"{col} DESC" for col in columns]
        else:
            self._order_cols = list(columns)
        return self

    def limit(self, n: int) -> LogQuery:
        """Limit number of results.

        Args:
            n: Maximum number of rows to return

        Returns:
            Self for chaining
        """
        self._limit_n = n
        return self

    # -------------------------------------------------------------------------
    # Execution methods
    # -------------------------------------------------------------------------

    def _build(self) -> duckdb.DuckDBPyRelation:
        """Build the final relation with all deferred operations."""
        rel = self._rel

        # Apply filters first
        if self._filters:
            combined = " AND ".join(f"({f})" for f in self._filters)
            rel = rel.filter(combined)

        # Order before select (in case order column not in select)
        if self._order_cols:
            rel = rel.order(", ".join(self._order_cols))

        # Select columns
        if self._select_cols:
            rel = rel.select(", ".join(self._select_cols))

        # Limit last
        if self._limit_n is not None:
            rel = rel.limit(self._limit_n)

        return rel

    def df(self) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame."""
        return self._build().df()

    def fetchall(self) -> list[tuple]:
        """Execute query and return all rows as list of tuples."""
        return self._build().fetchall()

    def fetchone(self) -> tuple | None:
        """Execute query and return first row."""
        return self._build().fetchone()

    def count(self) -> int:
        """Return count of matching rows."""
        rel = self._rel
        if self._filters:
            combined = " AND ".join(f"({f})" for f in self._filters)
            rel = rel.filter(combined)
        result = rel.aggregate("COUNT(*) as cnt").fetchone()
        return result[0] if result else 0

    def exists(self) -> bool:
        """Check if any rows match the query."""
        return self.count() > 0

    # -------------------------------------------------------------------------
    # Inspection methods
    # -------------------------------------------------------------------------

    @property
    def columns(self) -> list[str]:
        """Get list of column names."""
        return self._rel.columns

    @property
    def dtypes(self) -> list[str]:
        """Get list of column types."""
        return self._rel.dtypes

    def describe(self) -> pd.DataFrame:
        """Get statistical description of the data."""
        return self._rel.describe().df()

    def show(self, n: int = 10) -> None:
        """Print first n rows to stdout."""
        self._build().limit(n).show()

    def explain(self) -> str:
        """Get the query execution plan."""
        return self._build().explain()

    # -------------------------------------------------------------------------
    # Aggregation methods
    # -------------------------------------------------------------------------

    def group_by(self, *columns: str) -> LogQueryGrouped:
        """Group by columns for aggregation.

        Args:
            *columns: Column names to group by

        Returns:
            LogQueryGrouped for aggregation operations
        """
        return LogQueryGrouped(self, list(columns))

    def value_counts(self, column: str) -> pd.DataFrame:
        """Count occurrences of each value in a column.

        Args:
            column: Column to count values for

        Returns:
            DataFrame with value counts
        """
        rel = self._rel
        if self._filters:
            combined = " AND ".join(f"({f})" for f in self._filters)
            rel = rel.filter(combined)
        return rel.aggregate(f"{column}, COUNT(*) as count").order("count DESC").df()


class LogQueryGrouped:
    """Grouped query for aggregation operations."""

    def __init__(self, query: LogQuery, group_cols: list[str]):
        self._query = query
        self._group_cols = group_cols

    def count(self) -> pd.DataFrame:
        """Count rows in each group."""
        return self._aggregate("COUNT(*) as count")

    def sum(self, column: str) -> pd.DataFrame:
        """Sum values in each group."""
        return self._aggregate(f"SUM({column}) as sum")

    def avg(self, column: str) -> pd.DataFrame:
        """Average values in each group."""
        return self._aggregate(f"AVG({column}) as avg")

    def min(self, column: str) -> pd.DataFrame:
        """Minimum value in each group."""
        return self._aggregate(f"MIN({column}) as min")

    def max(self, column: str) -> pd.DataFrame:
        """Maximum value in each group."""
        return self._aggregate(f"MAX({column}) as max")

    def agg(self, **aggregations: str) -> pd.DataFrame:
        """Custom aggregations.

        Args:
            **aggregations: name=expression pairs

        Example:
            .agg(total="SUM(amount)", avg_amount="AVG(amount)")
        """
        agg_exprs = [f"{expr} as {name}" for name, expr in aggregations.items()]
        return self._aggregate(", ".join(agg_exprs))

    def _aggregate(self, agg_expr: str) -> pd.DataFrame:
        """Execute aggregation."""
        rel = self._query._rel
        if self._query._filters:
            combined = " AND ".join(f"({f})" for f in self._query._filters)
            rel = rel.filter(combined)

        group_expr = ", ".join(self._group_cols)
        return rel.aggregate(f"{group_expr}, {agg_expr}").df()


class LogStore:
    """Manages the .lq log repository.

    Provides access to stored events and metadata about runs.

    Example:
        store = LogStore.open()  # Find .lq in current/parent dirs
        store = LogStore("/path/to/.lq")  # Explicit path

        # Query events
        errors = store.events().filter(severity="error").df()

        # Get run info
        runs = store.runs()
        latest = store.latest_run()
    """

    def __init__(
        self,
        lq_dir: Path,
        conn: duckdb.DuckDBPyConnection | None = None,
    ):
        """Initialize LogStore.

        Args:
            lq_dir: Path to .lq directory
            conn: Optional existing connection
        """
        self._lq_dir = Path(lq_dir)
        self._logs_dir = self._lq_dir / "logs"
        self._conn = conn or duckdb.connect(":memory:")
        self._schema_loaded = False

    @classmethod
    def open(cls, path: Path | str | None = None) -> LogStore:
        """Open a LogStore, finding .lq directory if not specified.

        Args:
            path: Optional path to .lq directory

        Returns:
            LogStore instance

        Raises:
            FileNotFoundError: If .lq directory not found
        """
        if path is not None:
            lq_dir = Path(path)
        else:
            lq_dir = cls._find_lq_dir()

        if not lq_dir.exists():
            raise FileNotFoundError(f".lq directory not found: {lq_dir}")

        return cls(lq_dir)

    @classmethod
    def _find_lq_dir(cls) -> Path:
        """Find .lq directory in current or parent directories."""
        cwd = Path.cwd()
        for p in [cwd, *list(cwd.parents)]:
            lq_path = p / ".lq"
            if lq_path.exists():
                return lq_path
        raise FileNotFoundError(
            ".lq directory not found. Run 'lq init' to initialize."
        )

    def _ensure_schema(self) -> None:
        """Load schema if not already loaded."""
        if self._schema_loaded:
            return

        # Set up lq_base_path macro
        logs_path = self._logs_dir.resolve()
        self._conn.execute(f"CREATE OR REPLACE MACRO lq_base_path() AS '{logs_path}'")

        # Load schema file
        schema_path = self._lq_dir / "schema.sql"
        if schema_path.exists():
            schema_sql = schema_path.read_text()
            for stmt in schema_sql.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                # Skip lq_base_path definition (we set it above)
                if "lq_base_path()" in stmt and "CREATE" in stmt.upper() and "MACRO" in stmt.upper():
                    continue
                # Skip pure comments
                lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
                if not lines:
                    continue
                try:
                    self._conn.execute(stmt)
                except duckdb.Error:
                    pass  # Ignore schema errors

        self._schema_loaded = True

    @property
    def path(self) -> Path:
        """Path to .lq directory."""
        return self._lq_dir

    @property
    def logs_path(self) -> Path:
        """Path to logs directory."""
        return self._logs_dir

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get the underlying DuckDB connection."""
        self._ensure_schema()
        return self._conn

    def events(self) -> LogQuery:
        """Query stored events.

        Returns:
            LogQuery for filtering/selecting events
        """
        self._ensure_schema()
        return LogQuery.from_table(self._conn, "lq_events")

    def errors(self) -> LogQuery:
        """Query stored errors (convenience method).

        Returns:
            LogQuery filtered to severity='error'
        """
        return self.events().filter(severity="error")

    def warnings(self) -> LogQuery:
        """Query stored warnings (convenience method).

        Returns:
            LogQuery filtered to severity='warning'
        """
        return self.events().filter(severity="warning")

    def runs(self) -> pd.DataFrame:
        """Get summary of all runs.

        Returns:
            DataFrame with run_id, source_name, started_at, metadata, etc.
        """
        self._ensure_schema()
        return self._conn.sql("""
            SELECT DISTINCT
                run_id,
                source_name,
                source_type,
                command,
                started_at,
                completed_at,
                exit_code,
                cwd,
                executable_path,
                hostname,
                platform,
                arch,
                git_commit,
                git_branch,
                git_dirty,
                ci
            FROM lq_events
            ORDER BY run_id DESC
        """).df()

    def run(self, run_id: int) -> LogQuery:
        """Query events from a specific run.

        Args:
            run_id: The run ID to query

        Returns:
            LogQuery filtered to the specified run
        """
        return self.events().filter(run_id=run_id)

    def latest_run(self) -> int | None:
        """Get the latest run ID.

        Returns:
            Latest run_id or None if no runs
        """
        self._ensure_schema()
        result = self._conn.sql(
            "SELECT MAX(run_id) FROM lq_events"
        ).fetchone()
        return result[0] if result and result[0] is not None else None

    def event(self, run_id: int, event_id: int) -> dict[str, Any] | None:
        """Get a specific event by reference.

        Args:
            run_id: Run ID
            event_id: Event ID within the run

        Returns:
            Event as dict or None if not found
        """
        result = (
            self.events()
            .filter(run_id=run_id, event_id=event_id)
            .fetchone()
        )
        if result is None:
            return None

        columns = self.events().columns
        return dict(zip(columns, result))

    def has_data(self) -> bool:
        """Check if the store has any data."""
        if not self._logs_dir.exists():
            return False
        return any(self._logs_dir.rglob("*.parquet"))
