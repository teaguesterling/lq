"""
blq commands module.

This module provides modular command implementations for the blq CLI.
"""

from blq.commands.events import cmd_context, cmd_event
from blq.commands.execution import cmd_capture, cmd_import, cmd_run
from blq.commands.init_cmd import cmd_init
from blq.commands.management import (
    cmd_errors,
    cmd_history,
    cmd_prune,
    cmd_status,
    cmd_summary,
    cmd_warnings,
)
from blq.commands.query_cmd import cmd_filter, cmd_query, cmd_shell, cmd_sql
from blq.commands.registry import cmd_commands, cmd_register, cmd_unregister
from blq.commands.serve_cmd import cmd_serve
from blq.commands.sync_cmd import cmd_sync

__all__ = [
    # Init
    "cmd_init",
    # Execution
    "cmd_run",
    "cmd_import",
    "cmd_capture",
    # Query
    "cmd_query",
    "cmd_filter",
    "cmd_sql",
    "cmd_shell",
    # Management
    "cmd_status",
    "cmd_errors",
    "cmd_warnings",
    "cmd_summary",
    "cmd_history",
    "cmd_prune",
    # Events
    "cmd_event",
    "cmd_context",
    # Registry
    "cmd_commands",
    "cmd_register",
    "cmd_unregister",
    # Sync
    "cmd_sync",
    # Serve
    "cmd_serve",
]
