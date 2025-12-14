"""
lq commands module.

This module provides modular command implementations for the lq CLI.
"""

from lq.commands.events import cmd_context, cmd_event
from lq.commands.execution import cmd_capture, cmd_import, cmd_run
from lq.commands.init_cmd import cmd_init
from lq.commands.management import (
    cmd_errors,
    cmd_history,
    cmd_prune,
    cmd_status,
    cmd_summary,
    cmd_warnings,
)
from lq.commands.query_cmd import cmd_filter, cmd_query, cmd_shell, cmd_sql
from lq.commands.registry import cmd_commands, cmd_register, cmd_unregister
from lq.commands.serve_cmd import cmd_serve
from lq.commands.sync_cmd import cmd_sync

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
