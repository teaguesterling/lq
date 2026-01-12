"""
blq CLI - Build Log Query command-line interface.

Usage:
    blq init [--mcp]                  Initialize .lq directory
    blq run <command>                 Run registered command (alias: r)
    blq exec <command>                Execute ad-hoc command (alias: e)
    blq import <file> [--name NAME]   Import existing log file
    blq capture [--name NAME]         Capture from stdin
    blq status                        Show status of all sources
    blq errors [--source S] [-n N]    Show recent errors
    blq warnings [--source S] [-n N]  Show recent warnings
    blq summary                       Aggregate summary
    blq sql <query>                   Run arbitrary SQL
    blq shell                         Interactive SQL shell
    blq history [-n N]                Show run history
    blq prune [--older-than DAYS]     Remove old logs
    blq event <ref>                   Show event details by reference (e.g., 5:3)
    blq query [options] [file...]     Query log files or stored events (alias: q)
    blq filter [expr...] [file...]    Filter with simple syntax (alias: f)
    blq sync [destination]            Sync logs to central location
    blq serve [--transport T]         Start MCP server for AI agents

Query examples:
    blq q build.log                           # all events from file
    blq q -s file_path,message build.log      # select columns
    blq q -f "severity='error'" build.log     # filter with SQL WHERE
    blq q -f "severity='error'"               # query stored events

Filter examples:
    blq f severity=error build.log            # filter by exact match
    blq f severity=error,warning build.log    # OR within field
    blq f file_path~main build.log            # contains (LIKE)
    blq f severity!=info build.log            # not equal
    blq f -v severity=error build.log         # invert (grep -v style)
    blq f -c severity=error build.log         # count matches only
"""

from __future__ import annotations

import argparse
import logging
import sys
from importlib.metadata import version as get_version

from blq.commands import (
    cmd_capture,
    cmd_ci_check,
    cmd_ci_comment,
    cmd_commands,
    cmd_completions,
    cmd_context,
    cmd_errors,
    cmd_event,
    cmd_exec,
    cmd_filter,
    cmd_formats,
    cmd_history,
    cmd_hooks_add,
    cmd_hooks_install,
    cmd_hooks_list,
    cmd_hooks_remove,
    cmd_hooks_run,
    cmd_hooks_status,
    cmd_import,
    cmd_init,
    cmd_migrate,
    cmd_prune,
    cmd_query,
    cmd_register,
    cmd_report,
    cmd_run,
    cmd_serve,
    cmd_shell,
    cmd_sql,
    cmd_status,
    cmd_summary,
    cmd_sync,
    cmd_unregister,
    cmd_warnings,
    cmd_watch,
)
from blq.commands.core import (
    GLOBAL_PROJECTS_PATH,
    # Re-export commonly used items for backward compatibility
    BlqConfig,
    ConnectionFactory,
    EventRef,
    EventSummary,
    RegisteredCommand,
    RunResult,
    capture_ci_info,
    capture_environment,
    capture_git_info,
    find_executable,
    get_connection,
    get_lq_dir,
    get_next_run_id,
    parse_log_content,
    write_run_parquet,
)
from blq.commands.query_cmd import format_query_output, parse_filter_expression, query_source

# Re-export for backward compatibility
__all__ = [
    "main",
    # Commands
    "cmd_capture",
    "cmd_ci_check",
    "cmd_ci_comment",
    "cmd_commands",
    "cmd_report",
    "cmd_completions",
    "cmd_context",
    "cmd_errors",
    "cmd_event",
    "cmd_exec",
    "cmd_filter",
    "cmd_formats",
    "cmd_history",
    "cmd_import",
    "cmd_init",
    "cmd_prune",
    "cmd_query",
    "cmd_register",
    "cmd_run",
    "cmd_serve",
    "cmd_shell",
    "cmd_sql",
    "cmd_status",
    "cmd_summary",
    "cmd_sync",
    "cmd_unregister",
    "cmd_warnings",
    "cmd_watch",
    # Core types and utilities
    "BlqConfig",
    "ConnectionFactory",
    "EventRef",
    "EventSummary",
    "RegisteredCommand",
    "RunResult",
    "capture_ci_info",
    "capture_environment",
    "capture_git_info",
    "find_executable",
    "format_query_output",
    "get_connection",
    "get_lq_dir",
    "get_next_run_id",
    "parse_filter_expression",
    "parse_log_content",
    "query_source",
    "write_run_parquet",
]


def _setup_logging() -> None:
    """Configure the lq logger with stderr handler."""
    lq_logger = logging.getLogger("blq-cli")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    lq_logger.addHandler(handler)
    # Default level is WARNING (quiet), changed by --summary or --verbose
    lq_logger.setLevel(logging.WARNING)


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="blq - Build Log Query CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {get_version('blq-cli')}",
    )

    # Global flags
    parser.add_argument(
        "-F",
        "--log-format",
        default="auto",
        help="Log format for parsing (default: auto). Use 'blq formats' to list available formats.",
    )
    parser.add_argument(
        "-g",
        "--global",
        action="store_true",
        dest="global_",
        help="Query global store (~/.lq/projects/) instead of local .lq",
    )
    parser.add_argument(
        "-d",
        "--database",
        metavar="PATH",
        help="Query custom database path (local or remote, e.g., s3://bucket/lq/)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # init
    p_init = subparsers.add_parser("init", help="Initialize .lq directory")
    p_init.add_argument(
        "--mcp", "-m", action="store_true", help="Create .mcp.json for MCP server discovery"
    )
    p_init.add_argument("--project", "-p", help="Project name (overrides auto-detection)")
    p_init.add_argument("--namespace", "-n", help="Project namespace (overrides auto-detection)")
    p_init.add_argument(
        "--detect", "-d", action="store_true", help="Auto-detect and register build/test commands"
    )
    p_init.add_argument(
        "--detect-mode",
        choices=["none", "simple", "inspect", "auto"],
        default="auto",
        help="Detection mode: none, simple (build files), inspect (parse CI/Makefiles), auto",
    )
    p_init.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Non-interactive mode (auto-confirm detected commands)",
    )
    p_init.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Reinitialize config files (schema, config) without deleting data",
    )
    p_init.add_argument(
        "--bird",
        action="store_true",
        help="Use BIRD storage mode (DuckDB tables instead of parquet files)",
    )
    p_init.set_defaults(func=cmd_init)

    # run
    p_run = subparsers.add_parser("run", aliases=["r"], help="Run command and capture output")
    p_run.add_argument("command", nargs="+", help="Command to run")
    p_run.add_argument("--name", "-n", help="Source name (default: command name)")
    p_run.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_run.add_argument("--keep-raw", "-r", action="store_true", help="Keep raw output file")
    p_run.add_argument("--json", "-j", action="store_true", help="Output structured JSON result")
    p_run.add_argument("--markdown", "-m", action="store_true", help="Output markdown summary")
    p_run.add_argument("--quiet", "-q", action="store_true", help="Suppress streaming output")
    p_run.add_argument(
        "--summary", "-s", action="store_true", help="Show brief summary (errors/warnings count)"
    )
    p_run.add_argument("--verbose", "-v", action="store_true", help="Show all blq status messages")
    p_run.add_argument(
        "--include-warnings",
        "-w",
        action="store_true",
        help="Include warnings in structured output",
    )
    p_run.add_argument(
        "--error-limit", type=int, default=20, help="Max errors/warnings in output (default: 20)"
    )
    p_run.add_argument(
        "--register",
        "-R",
        action="store_true",
        help="Register command if not already registered",
    )
    p_run.add_argument(
        "--positional-args",
        "-a",
        type=int,
        default=None,
        metavar="N",
        help="Use exactly N positional args for placeholders (rest are passthrough)",
    )
    p_run.set_defaults(func=cmd_run)
    # Capture control: runtime flags override command config
    capture_group = p_run.add_mutually_exclusive_group()
    capture_group.add_argument(
        "--capture",
        "-C",
        action="store_true",
        dest="capture",
        default=None,
        help="Force log capture (override command config)",
    )
    capture_group.add_argument(
        "--no-capture",
        "-N",
        action="store_false",
        dest="capture",
        help="Skip log capture, just run command",
    )

    # exec - ad-hoc command execution (never uses registry)
    p_exec = subparsers.add_parser(
        "exec", aliases=["e"], help="Execute ad-hoc command and capture output"
    )
    p_exec.add_argument("command", nargs="+", help="Command to execute")
    p_exec.add_argument("--name", "-n", help="Source name (default: command name)")
    p_exec.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_exec.add_argument("--keep-raw", "-r", action="store_true", help="Keep raw output file")
    p_exec.add_argument("--json", "-j", action="store_true", help="Output structured JSON result")
    p_exec.add_argument("--markdown", "-m", action="store_true", help="Output markdown summary")
    p_exec.add_argument("--quiet", "-q", action="store_true", help="Suppress streaming output")
    p_exec.add_argument(
        "--summary", "-s", action="store_true", help="Show brief summary (errors/warnings count)"
    )
    p_exec.add_argument("--verbose", "-v", action="store_true", help="Show all blq status messages")
    p_exec.add_argument(
        "--include-warnings",
        "-w",
        action="store_true",
        help="Include warnings in structured output",
    )
    p_exec.add_argument(
        "--error-limit", type=int, default=20, help="Max errors/warnings in output (default: 20)"
    )
    p_exec.add_argument(
        "--no-capture",
        "-N",
        action="store_true",
        help="Skip log capture, just run command",
    )
    p_exec.set_defaults(func=cmd_exec)

    # import
    p_import = subparsers.add_parser("import", help="Import existing log file")
    p_import.add_argument("file", help="Log file to import")
    p_import.add_argument("--name", "-n", help="Source name (default: filename)")
    p_import.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_import.set_defaults(func=cmd_import)

    # capture
    p_capture = subparsers.add_parser("capture", help="Capture from stdin")
    p_capture.add_argument("--name", "-n", default="stdin", help="Source name")
    p_capture.add_argument("--format", "-f", default="auto", help="Parse format hint")
    p_capture.set_defaults(func=cmd_capture)

    # status
    p_status = subparsers.add_parser("status", help="Show status of all sources")
    p_status.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    p_status.set_defaults(func=cmd_status)

    # errors
    p_errors = subparsers.add_parser("errors", help="Show recent errors")
    p_errors.add_argument("--source", "-s", help="Filter by source")
    p_errors.add_argument("--limit", "-n", type=int, default=10, help="Max results")
    p_errors.add_argument("--compact", "-c", action="store_true", help="Compact format")
    p_errors.add_argument("--json", "-j", action="store_true", help="JSON output")
    p_errors.set_defaults(func=cmd_errors)

    # warnings
    p_warnings = subparsers.add_parser("warnings", help="Show recent warnings")
    p_warnings.add_argument("--source", "-s", help="Filter by source")
    p_warnings.add_argument("--limit", "-n", type=int, default=10, help="Max results")
    p_warnings.set_defaults(func=cmd_warnings)

    # summary
    p_summary = subparsers.add_parser("summary", help="Aggregate summary")
    p_summary.add_argument("--latest", "-l", action="store_true", help="Latest run only")
    p_summary.set_defaults(func=cmd_summary)

    # history
    p_history = subparsers.add_parser("history", help="Show run history")
    p_history.add_argument("--limit", "-n", type=int, default=20, help="Max results")
    p_history.set_defaults(func=cmd_history)

    # sql
    p_sql = subparsers.add_parser("sql", help="Run arbitrary SQL")
    p_sql.add_argument("query", nargs="+", help="SQL query")
    p_sql.set_defaults(func=cmd_sql)

    # shell
    p_shell = subparsers.add_parser("shell", help="Interactive SQL shell")
    p_shell.set_defaults(func=cmd_shell)

    # prune
    p_prune = subparsers.add_parser("prune", help="Remove old logs")
    p_prune.add_argument("--older-than", "-d", type=int, default=30, help="Days to keep")
    p_prune.add_argument("--dry-run", action="store_true", help="Show what would be removed")
    p_prune.set_defaults(func=cmd_prune)

    # formats
    p_formats = subparsers.add_parser("formats", help="List available log formats")
    p_formats.set_defaults(func=cmd_formats)

    # completions
    p_completions = subparsers.add_parser("completions", help="Generate shell completion scripts")
    p_completions.add_argument(
        "shell",
        choices=["bash", "zsh", "fish"],
        help="Shell type (bash, zsh, or fish)",
    )
    p_completions.set_defaults(func=cmd_completions)

    # event
    p_event = subparsers.add_parser("event", help="Show event details by reference")
    p_event.add_argument("ref", help="Event reference (e.g., 5:3 for run 5, event 3)")
    p_event.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_event.set_defaults(func=cmd_event)

    # context
    p_context = subparsers.add_parser("context", help="Show context lines around an event")
    p_context.add_argument("ref", help="Event reference (e.g., 5:3)")
    p_context.add_argument(
        "--lines", "-n", type=int, default=3, help="Context lines before/after (default: 3)"
    )
    p_context.set_defaults(func=cmd_context)

    # commands
    p_commands = subparsers.add_parser("commands", help="List registered commands")
    p_commands.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_commands.set_defaults(func=cmd_commands)

    # register
    p_register = subparsers.add_parser("register", help="Register a command")
    p_register.add_argument("name", help="Command name (e.g., 'build', 'test')")
    p_register.add_argument("cmd", nargs="+", help="Command to run")
    p_register.add_argument("--description", "-d", help="Command description")
    p_register.add_argument(
        "--timeout", "-t", type=int, default=300, help="Timeout in seconds (default: 300)"
    )
    p_register.add_argument("--format", "-f", default="auto", help="Log format hint")
    p_register.add_argument(
        "--no-capture", "-N", action="store_true", help="Don't capture logs by default"
    )
    p_register.add_argument("--force", action="store_true", help="Overwrite existing command")
    p_register.set_defaults(func=cmd_register)

    # unregister
    p_unregister = subparsers.add_parser("unregister", help="Remove a registered command")
    p_unregister.add_argument("name", help="Command name to remove")
    p_unregister.set_defaults(func=cmd_unregister)

    # sync
    p_sync = subparsers.add_parser("sync", help="Sync project logs to central location")
    p_sync.add_argument(
        "destination", nargs="?", help="Destination path", default=GLOBAL_PROJECTS_PATH
    )
    p_sync.add_argument(
        "--soft", "-s", action="store_true", default=True, help="Create symlink (default)"
    )
    p_sync.add_argument("--hard", "-H", action="store_true", help="Copy files instead of symlink")
    p_sync.add_argument("--force", "-f", action="store_true", help="Replace existing sync target")
    p_sync.add_argument(
        "--dry-run", "-n", action="store_true", help="Show what would be done without doing it"
    )
    p_sync.add_argument("--status", action="store_true", help="Show current sync status")
    p_sync.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    p_sync.set_defaults(func=cmd_sync)

    # migrate
    p_migrate = subparsers.add_parser("migrate", help="Migrate data between storage formats")
    p_migrate.add_argument(
        "--to-bird",
        action="store_true",
        help="Migrate parquet data to BIRD storage format",
    )
    p_migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    p_migrate.add_argument(
        "--keep-parquet",
        action="store_true",
        help="Keep parquet files after migration",
    )
    p_migrate.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force migration even if already using BIRD mode",
    )
    p_migrate.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed progress",
    )
    p_migrate.set_defaults(func=cmd_migrate)

    # query (with alias 'q')
    p_query = subparsers.add_parser("query", aliases=["q"], help="Query log files or stored events")
    p_query.add_argument("files", nargs="*", help="Log file(s) to query (omit for stored data)")
    p_query.add_argument("-s", "--select", help="Columns to select (comma-separated)")
    p_query.add_argument("-f", "--filter", help="SQL WHERE clause")
    p_query.add_argument("-o", "--order", help="SQL ORDER BY clause")
    p_query.add_argument("-n", "--limit", type=int, help="Max rows to return")
    p_query.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_query.add_argument("--csv", action="store_true", help="Output as CSV")
    p_query.add_argument("--markdown", "--md", action="store_true", help="Output as Markdown table")
    p_query.set_defaults(func=cmd_query)

    # filter (with alias 'f')
    p_filter = subparsers.add_parser(
        "filter", aliases=["f"], help="Filter log files with simple syntax"
    )
    p_filter.add_argument("args", nargs="*", help="Filter expressions and/or file(s)")
    p_filter.add_argument("-v", "--invert", action="store_true", help="Invert match (like grep -v)")
    p_filter.add_argument("-c", "--count", action="store_true", help="Only print count of matches")
    p_filter.add_argument(
        "-i", "--ignore-case", action="store_true", help="Case insensitive matching"
    )
    p_filter.add_argument("-n", "--limit", type=int, help="Max rows to return")
    p_filter.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    p_filter.add_argument("--csv", action="store_true", help="Output as CSV")
    p_filter.add_argument(
        "--markdown", "--md", action="store_true", help="Output as Markdown table"
    )
    p_filter.set_defaults(func=cmd_filter)

    # serve (MCP server)
    p_serve = subparsers.add_parser("serve", help="Start MCP server for AI agent integration")
    p_serve.add_argument(
        "--transport",
        "-t",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport type (default: stdio)",
    )
    p_serve.add_argument(
        "--port", "-p", type=int, default=8080, help="Port for SSE transport (default: 8080)"
    )
    p_serve.set_defaults(func=cmd_serve)

    # =========================================================================
    # Hooks commands
    # =========================================================================

    p_hooks_install = subparsers.add_parser(
        "hooks-install", help="Install git pre-commit hook"
    )
    p_hooks_install.add_argument(
        "--force", "-f", action="store_true", help="Overwrite existing hook"
    )
    p_hooks_install.set_defaults(func=cmd_hooks_install)

    p_hooks_remove = subparsers.add_parser(
        "hooks-remove", help="Remove git pre-commit hook"
    )
    p_hooks_remove.set_defaults(func=cmd_hooks_remove)

    p_hooks_status = subparsers.add_parser(
        "hooks-status", help="Show git hook status"
    )
    p_hooks_status.set_defaults(func=cmd_hooks_status)

    p_hooks_run = subparsers.add_parser(
        "hooks-run", help="Run pre-commit hook commands (called by git hook)"
    )
    p_hooks_run.set_defaults(func=cmd_hooks_run)

    p_hooks_add = subparsers.add_parser(
        "hooks-add", help="Add a command to pre-commit hook"
    )
    p_hooks_add.add_argument("command", help="Command name to add")
    p_hooks_add.set_defaults(func=cmd_hooks_add)

    p_hooks_list = subparsers.add_parser(
        "hooks-list", help="List commands in pre-commit hook"
    )
    p_hooks_list.set_defaults(func=cmd_hooks_list)

    # =========================================================================
    # Watch command
    # =========================================================================

    p_watch = subparsers.add_parser(
        "watch", aliases=["w"], help="Watch for changes and re-run commands"
    )
    p_watch.add_argument(
        "commands", nargs="*", help="Commands to run (default: all registered)"
    )
    p_watch.add_argument(
        "--include", "-i", action="append", default=[],
        help="Glob patterns to watch (can be repeated)"
    )
    p_watch.add_argument(
        "--exclude", "-e", action="append", default=[],
        help="Glob patterns to ignore (can be repeated)"
    )
    p_watch.add_argument(
        "--debounce", "-d", type=int, default=None,
        help="Debounce delay in milliseconds (default: 500)"
    )
    p_watch.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress command output, show only status"
    )
    p_watch.add_argument(
        "--clear", "-c", action="store_true",
        help="Clear screen between runs"
    )
    p_watch.add_argument(
        "--once", action="store_true",
        help="Run once on startup then exit (useful for testing)"
    )
    p_watch.set_defaults(func=cmd_watch)

    # =========================================================================
    # CI commands
    # =========================================================================

    p_ci = subparsers.add_parser("ci", help="CI integration commands")
    ci_subparsers = p_ci.add_subparsers(dest="ci_command", help="CI subcommand")

    # ci check
    p_ci_check = ci_subparsers.add_parser(
        "check", help="Check for new errors vs baseline"
    )
    p_ci_check.add_argument(
        "--baseline", "-b",
        help="Baseline (run ID, branch name, or commit SHA)"
    )
    p_ci_check.add_argument(
        "--fail-on-any", action="store_true",
        help="Fail if any errors (no baseline comparison)"
    )
    p_ci_check.add_argument(
        "--json", "-j", action="store_true",
        help="Output as JSON"
    )
    p_ci_check.set_defaults(func=cmd_ci_check)

    # ci comment
    p_ci_comment = ci_subparsers.add_parser(
        "comment", help="Post error summary as PR comment"
    )
    p_ci_comment.add_argument(
        "--update", "-u", action="store_true",
        help="Update existing comment instead of creating new"
    )
    p_ci_comment.add_argument(
        "--diff", "-d", action="store_true",
        help="Include diff vs baseline"
    )
    p_ci_comment.add_argument(
        "--baseline", "-b",
        help="Baseline for diff (run ID, branch, or commit)"
    )
    p_ci_comment.set_defaults(func=cmd_ci_comment)

    def ci_help(args: argparse.Namespace) -> None:
        """Show help for ci command."""
        p_ci.print_help()

    p_ci.set_defaults(func=ci_help)

    # =========================================================================
    # Report command
    # =========================================================================

    p_report = subparsers.add_parser(
        "report", help="Generate markdown report of build/test results"
    )
    p_report.add_argument(
        "--run", "-r", type=int,
        help="Run ID to report on (default: latest)"
    )
    p_report.add_argument(
        "--baseline", "-b",
        help="Baseline for comparison (run ID or branch name)"
    )
    p_report.add_argument(
        "--output", "-o",
        help="Output file (default: stdout)"
    )
    p_report.add_argument(
        "--warnings", "-w", action="store_true",
        help="Include warning details"
    )
    p_report.add_argument(
        "--summary-only", "-s", action="store_true",
        help="Summary only, no individual error details"
    )
    p_report.add_argument(
        "--error-limit", "-n", type=int, default=20,
        help="Max errors to include (default: 20)"
    )
    p_report.add_argument(
        "--file-limit", "-f", type=int, default=10,
        help="Max files in breakdown (default: 10)"
    )
    p_report.set_defaults(func=cmd_report)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
