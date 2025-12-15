"""
Command registry commands for blq CLI.

Handles listing, registering, and unregistering commands.
"""

from __future__ import annotations

import argparse
import json
import sys

from blq.commands.core import (
    RegisteredCommand,
    ensure_initialized,
    load_commands,
    save_commands,
)


def cmd_commands(args: argparse.Namespace) -> None:
    """List registered commands."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    if not commands:
        print("No commands registered.")
        print("Use 'blq register <name> <command>' to register a command.")
        return

    if args.json:
        data = {name: cmd.to_dict() for name, cmd in commands.items()}
        print(json.dumps(data, indent=2))
    else:
        print(f"{'Name':<15} {'Command':<40} {'Capture':<8} Description")
        print("-" * 80)
        for name, cmd in commands.items():
            cmd_display = cmd.cmd[:37] + "..." if len(cmd.cmd) > 40 else cmd.cmd
            capture_str = "yes" if cmd.capture else "no"
            print(f"{name:<15} {cmd_display:<40} {capture_str:<8} {cmd.description}")


def cmd_register(args: argparse.Namespace) -> None:
    """Register a new command."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    name = args.name
    cmd_str = " ".join(args.cmd)

    if name in commands and not args.force:
        print(f"Command '{name}' already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)

    capture = not getattr(args, "no_capture", False)
    commands[name] = RegisteredCommand(
        name=name,
        cmd=cmd_str,
        description=args.description or "",
        timeout=args.timeout,
        format=args.format,
        capture=capture,
    )

    save_commands(lq_dir, commands)
    capture_note = " (no capture)" if not capture else ""
    print(f"Registered command '{name}': {cmd_str}{capture_note}")


def cmd_unregister(args: argparse.Namespace) -> None:
    """Remove a registered command."""
    lq_dir = ensure_initialized()
    commands = load_commands(lq_dir)

    if args.name not in commands:
        print(f"Command '{args.name}' not found.", file=sys.stderr)
        sys.exit(1)

    del commands[args.name]
    save_commands(lq_dir, commands)
    print(f"Unregistered command '{args.name}'")
