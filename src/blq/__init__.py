"""
blq - Log Query

Capture and query build/test logs with DuckDB.

Example usage:
    from blq import LogStore, LogQuery

    # Query stored events
    store = LogStore.open()
    errors = store.errors().filter(file_path="%main%").df()

    # Query a log file directly
    events = LogQuery.from_file("build.log").filter(severity="error").df()
"""

__version__ = "0.1.0"

from blq.query import LogQuery, LogQueryGrouped, LogStore

__all__ = ["LogQuery", "LogStore", "LogQueryGrouped", "__version__"]
