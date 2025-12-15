"""
Serve command for blq CLI.

Handles starting the MCP server for AI agent integration.
"""

from __future__ import annotations

import argparse
import sys

from blq.commands.core import get_lq_dir


def cmd_serve(args: argparse.Namespace) -> None:
    """Start the MCP server for AI agent integration."""
    try:
        from blq.serve import serve
    except ImportError:
        print("Error: MCP dependencies not installed.", file=sys.stderr)
        print("Install with: pip install blq-cli[mcp]", file=sys.stderr)
        sys.exit(1)

    # Ensure we're in an initialized directory
    lq_dir = get_lq_dir()
    if lq_dir is None:
        print("Warning: No .lq directory found. Some features may not work.", file=sys.stderr)

    serve(transport=args.transport, port=args.port)
