"""
Git hooks integration for blq.

Provides commands to install/remove git pre-commit hooks that
automatically capture build/test output.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from blq.commands.core import BlqConfig

# Marker to identify blq-managed hooks
HOOK_MARKER = "# blq-managed-hook"

# Pre-commit hook script template
PRECOMMIT_HOOK_TEMPLATE = f'''#!/bin/sh
{HOOK_MARKER}
# blq pre-commit hook - auto-generated
# To remove: blq hooks-remove

# Run configured pre-commit commands
blq hooks-run

# Always exit 0 (non-blocking mode)
# Future: exit with error count if block_on_new_errors is enabled
exit 0
'''


def _find_git_dir() -> Path | None:
    """Find .git directory from cwd or parents.

    Returns:
        Path to .git directory, or None if not in a git repository.
    """
    cwd = Path.cwd()
    for p in [cwd, *list(cwd.parents)]:
        git_dir = p / ".git"
        if git_dir.is_dir():
            return git_dir
    return None


def _is_blq_hook(hook_path: Path) -> bool:
    """Check if a hook file was created by blq.

    Args:
        hook_path: Path to the hook file.

    Returns:
        True if the hook contains our marker.
    """
    if not hook_path.exists():
        return False
    try:
        content = hook_path.read_text()
        return HOOK_MARKER in content
    except (OSError, UnicodeDecodeError):
        return False


def _get_precommit_commands(config: BlqConfig) -> list[str]:
    """Get list of commands configured for pre-commit hook.

    Args:
        config: BlqConfig instance.

    Returns:
        List of command names to run.
    """
    hooks_config = config.hooks_config
    if not hooks_config:
        return []
    precommit = hooks_config.get("pre-commit", [])
    if isinstance(precommit, list):
        return precommit
    return []


def cmd_hooks_install(args: argparse.Namespace) -> None:
    """Install git pre-commit hook."""
    config = BlqConfig.ensure()

    # Find git directory
    git_dir = _find_git_dir()
    if git_dir is None:
        print("Error: Not in a git repository.", file=sys.stderr)
        sys.exit(1)

    hooks_dir = git_dir / "hooks"
    hooks_dir.mkdir(exist_ok=True)
    hook_path = hooks_dir / "pre-commit"

    # Check if hook exists
    if hook_path.exists():
        if _is_blq_hook(hook_path):
            if not getattr(args, "force", False):
                print("blq pre-commit hook already installed.")
                print("Use --force to reinstall.")
                return
        else:
            if not getattr(args, "force", False):
                print("Error: Pre-commit hook exists but was not created by blq.", file=sys.stderr)
                print("Use --force to overwrite (existing hook will be lost).", file=sys.stderr)
                sys.exit(1)
            print("Warning: Overwriting existing pre-commit hook.")

    # Write hook script
    hook_path.write_text(PRECOMMIT_HOOK_TEMPLATE)
    hook_path.chmod(0o755)

    # Show status
    commands = _get_precommit_commands(config)
    print(f"Installed pre-commit hook at {hook_path}")
    if commands:
        print(f"Configured commands: {', '.join(commands)}")
    else:
        print("No commands configured yet.")
        print("Add commands to .lq/config.yaml:")
        print("  hooks:")
        print("    pre-commit:")
        print("      - lint")
        print("      - test")


def cmd_hooks_remove(args: argparse.Namespace) -> None:
    """Remove git pre-commit hook."""
    git_dir = _find_git_dir()
    if git_dir is None:
        print("Error: Not in a git repository.", file=sys.stderr)
        sys.exit(1)

    hook_path = git_dir / "hooks" / "pre-commit"

    if not hook_path.exists():
        print("No pre-commit hook installed.")
        return

    if not _is_blq_hook(hook_path):
        print("Error: Pre-commit hook was not created by blq.", file=sys.stderr)
        print("Remove manually if needed:", hook_path, file=sys.stderr)
        sys.exit(1)

    hook_path.unlink()
    print("Removed pre-commit hook.")


def cmd_hooks_status(args: argparse.Namespace) -> None:
    """Show git hook status."""
    config = BlqConfig.find()

    git_dir = _find_git_dir()
    if git_dir is None:
        print("Not in a git repository.")
        return

    hook_path = git_dir / "hooks" / "pre-commit"

    print("Pre-commit hook:")
    if hook_path.exists():
        if _is_blq_hook(hook_path):
            print("  Status: installed (blq-managed)")
            print(f"  Location: {hook_path}")
        else:
            print("  Status: installed (not blq-managed)")
            print(f"  Location: {hook_path}")
    else:
        print("  Status: not installed")

    print()
    print("Configured commands:")
    if config:
        commands = _get_precommit_commands(config)
        if commands:
            for cmd in commands:
                # Check if command is registered
                if cmd in config.commands:
                    reg_cmd = config.commands[cmd]
                    print(f"  - {cmd}: {reg_cmd.cmd}")
                else:
                    print(f"  - {cmd}: (not registered)")
        else:
            print("  (none)")
    else:
        print("  (blq not initialized)")


def cmd_hooks_run(args: argparse.Namespace) -> None:
    """Run pre-commit hook commands.

    This is called by the git hook script. It runs all configured
    commands and displays a summary.
    """
    config = BlqConfig.find()
    if config is None:
        # Silently exit if not in a blq project
        return

    commands = _get_precommit_commands(config)
    if not commands:
        return

    print("blq: Running pre-commit checks...")
    print()

    results: list[tuple[str, bool, int]] = []  # (name, success, error_count)

    for cmd_name in commands:
        if cmd_name not in config.commands:
            print(f"  {cmd_name}: (not registered, skipping)")
            continue

        # Run the command via blq run
        result = subprocess.run(
            ["blq", "run", "--quiet", "--json", cmd_name],
            capture_output=True,
            text=True,
        )

        # Parse result
        try:
            import json

            data = json.loads(result.stdout) if result.stdout.strip() else {}
            status = data.get("status", "FAIL" if result.returncode != 0 else "OK")
            error_count = len(data.get("errors", []))
            success = status == "OK"
        except json.JSONDecodeError:
            success = result.returncode == 0
            error_count = 0 if success else 1
            status = "OK" if success else "FAIL"

        results.append((cmd_name, success, error_count))

        # Print status
        if success:
            print(f"  {cmd_name}: OK")
        else:
            print(f"  {cmd_name}: FAIL ({error_count} errors)")

    print()

    # Summary
    failed = [r for r in results if not r[1]]
    if failed:
        total_errors = sum(r[2] for r in failed)
        print(f"Pre-commit: {len(failed)} command(s) failed, {total_errors} error(s)")
        print("Run 'blq errors' to see details.")
    else:
        print("Pre-commit: all checks passed")


def cmd_hooks_add(args: argparse.Namespace) -> None:
    """Add a command to the pre-commit hook."""
    config = BlqConfig.ensure()

    cmd_name = args.command

    # Verify command is registered
    if cmd_name not in config.commands:
        print(f"Warning: '{cmd_name}' is not a registered command.", file=sys.stderr)
        print("Register it first with: blq register {cmd_name} \"<command>\"", file=sys.stderr)

    # Load current hooks config
    hooks_config = config.hooks_config.copy() if config.hooks_config else {}
    precommit = hooks_config.get("pre-commit", [])
    if not isinstance(precommit, list):
        precommit = []

    if cmd_name in precommit:
        print(f"'{cmd_name}' is already in pre-commit hooks.")
        return

    precommit.append(cmd_name)
    hooks_config["pre-commit"] = precommit
    config._hooks_config = hooks_config
    config.save()

    print(f"Added '{cmd_name}' to pre-commit hooks.")


def cmd_hooks_list(args: argparse.Namespace) -> None:
    """List commands configured for pre-commit hook."""
    config = BlqConfig.find()
    if config is None:
        return

    commands = _get_precommit_commands(config)
    for cmd in commands:
        print(cmd)
