"""
Query and filter commands for blq CLI.

Handles querying logs, filtering, SQL queries, and interactive shell.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from blq.commands.core import (
    SCHEMA_FILE,
    ensure_initialized,
    get_store_for_args,
)
from blq.query import LogQuery, LogStore


def format_query_output(
    df: pd.DataFrame,
    output_format: str = "table",
    limit: int | None = None,
) -> str:
    """Format query results for output.

    Args:
        df: DataFrame with query results
        output_format: One of 'table', 'json', 'csv', 'markdown'
        limit: Max rows to output (None for all)

    Returns:
        Formatted string output
    """
    if limit is not None and limit > 0:
        df = df.head(limit)

    if output_format == "json":
        return df.to_json(orient="records", indent=2)
    elif output_format == "csv":
        return df.to_csv(index=False)
    elif output_format == "markdown":
        return df.to_markdown(index=False)
    else:  # table
        return df.to_string(index=False)


def query_source(
    source: str | Path | None,
    select: str | None = None,
    where: str | None = None,
    order: str | None = None,
    lq_dir: Path | None = None,
    log_format: str = "auto",
) -> pd.DataFrame:
    """Query a log file directly or the stored lq_events.

    Uses the LogQuery API for cleaner query building.

    Args:
        source: Path to log file(s) or None to query stored data
        select: Columns to select (comma-separated) or None for all
        where: SQL WHERE clause (without WHERE keyword)
        order: SQL ORDER BY clause (without ORDER BY keyword)
        lq_dir: Path to .lq directory (for stored data queries)
        log_format: Log format hint for duck_hunt (default: auto)

    Returns:
        DataFrame with query results
    """
    if source:
        # Query file(s) directly using duck_hunt
        source_path = Path(source)
        if not source_path.exists() and "*" not in str(source_path):
            raise FileNotFoundError(f"File not found: {source}")

        try:
            query = LogQuery.from_file(source_path, format=log_format)
        except duckdb.Error:
            print(
                "Error: duck_hunt extension required for querying files directly.", file=sys.stderr
            )
            print("Run 'blq init' to install required extensions.", file=sys.stderr)
            print(f"Or import the file first: blq import {source}", file=sys.stderr)
            raise
    else:
        # Query stored data
        if lq_dir is None:
            lq_dir = ensure_initialized()
        store = LogStore(lq_dir)
        query = store.events()

    # Apply query modifiers
    if where:
        query = query.filter(where)
    if order:
        query = query.order_by(*[col.strip() for col in order.split(",")])
    if select:
        query = query.select(*[col.strip() for col in select.split(",")])

    return query.df()


def parse_filter_expression(expr: str, ignore_case: bool = False) -> str:
    """Parse a simple filter expression into SQL WHERE clause.

    Supports:
        key=value      -> key = 'value'
        key=v1,v2      -> key IN ('v1', 'v2')
        key~pattern    -> key ILIKE '%pattern%'
        key!=value     -> key != 'value'

    Args:
        expr: Filter expression like "severity=error" or "file_path~main"
        ignore_case: If True, use ILIKE for = comparisons too

    Returns:
        SQL WHERE clause fragment
    """
    # Handle ~ (LIKE/contains)
    if "~" in expr:
        key, value = expr.split("~", 1)
        return f"{key.strip()} ILIKE '%{value.strip()}%'"

    # Handle !=
    if "!=" in expr:
        key, value = expr.split("!=", 1)
        return f"{key.strip()} != '{value.strip()}'"

    # Handle = (exact match or IN for comma-separated)
    if "=" in expr:
        key, value = expr.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Check for comma-separated values (OR)
        if "," in value:
            values = [v.strip() for v in value.split(",")]
            quoted = ", ".join(f"'{v}'" for v in values)
            return f"{key} IN ({quoted})"

        # Single value
        if ignore_case:
            return f"LOWER({key}) = LOWER('{value}')"
        return f"{key} = '{value}'"

    raise ValueError(
        f"Invalid filter expression: {expr}. Use key=value, key~pattern, or key!=value"
    )


def cmd_query(args: argparse.Namespace) -> None:
    """Query log files or stored events."""
    # Determine source (file or stored data)
    source = args.files[0] if args.files else None

    # Support multiple files via glob pattern
    if source and "*" in source:
        # Let DuckDB handle the glob
        pass
    elif source and not Path(source).exists():
        print(f"Error: File not found: {source}", file=sys.stderr)
        sys.exit(1)

    # Get lq_dir for stored queries
    lq_dir = None
    if not source:
        lq_dir = ensure_initialized()

    try:
        df = query_source(
            source=source,
            select=args.select,
            where=args.filter,
            order=args.order,
            lq_dir=lq_dir,
            log_format=args.log_format,
        )

        # Determine output format
        if args.json:
            output_format = "json"
        elif args.csv:
            output_format = "csv"
        elif args.markdown:
            output_format = "markdown"
        else:
            output_format = "table"

        output = format_query_output(df, output_format, args.limit)
        print(output)

    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_filter(args: argparse.Namespace) -> None:
    """Filter log files or stored events with simple syntax."""
    # Separate filter expressions from file paths
    # Expressions contain =, ~, or !=
    expressions = []
    files = []
    for arg in args.args:
        if "=" in arg or "~" in arg:
            expressions.append(arg)
        else:
            files.append(arg)

    # Determine source (file or stored data)
    source = files[0] if files else None

    if source and not Path(source).exists() and "*" not in source:
        print(f"Error: File not found: {source}", file=sys.stderr)
        sys.exit(1)

    # Parse filter expressions into SQL WHERE clause
    where_clauses = []
    for expr in expressions:
        try:
            clause = parse_filter_expression(expr, ignore_case=args.ignore_case)
            where_clauses.append(clause)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    where = " AND ".join(where_clauses) if where_clauses else None

    # Invert the filter if -v flag
    if args.invert and where:
        where = f"NOT ({where})"

    # Get lq_dir for stored queries
    lq_dir = None
    if not source:
        lq_dir = ensure_initialized()

    try:
        df = query_source(
            source=source,
            select=None,  # filter always returns all columns
            where=where,
            order=None,
            lq_dir=lq_dir,
            log_format=args.log_format,
        )

        # Count mode
        if args.count:
            print(len(df))
            return

        # Determine output format
        if args.json:
            output_format = "json"
        elif args.csv:
            output_format = "csv"
        elif args.markdown:
            output_format = "markdown"
        else:
            output_format = "table"

        output = format_query_output(df, output_format, args.limit)
        print(output)

    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sql(args: argparse.Namespace) -> None:
    """Run arbitrary SQL."""
    sql = " ".join(args.query)
    try:
        store = get_store_for_args(args)
        result = store.connection.execute(sql).fetchdf()
        print(result.to_string(index=False))
    except duckdb.Error as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_shell(args: argparse.Namespace) -> None:
    """Start interactive DuckDB shell."""
    lq_dir = ensure_initialized()

    # Create init file
    init_sql = """
.prompt 'blq> '
LOAD duck_hunt;
"""
    schema_path = lq_dir / SCHEMA_FILE
    if schema_path.exists():
        init_sql += f".read '{schema_path}'\n"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(init_sql)
        init_file_name = f.name

    try:
        subprocess.run(["duckdb", "-init", init_file_name])
    finally:
        Path(init_file_name).unlink(missing_ok=True)
