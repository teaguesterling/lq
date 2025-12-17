"""Tests for command argument parameterization."""

import pytest

from blq.commands.core import (
    RegisteredCommand,
    expand_command,
    format_command_help,
    parse_placeholders,
)
from blq.commands.execution import _parse_command_args


class TestParsePlaceholders:
    """Tests for parse_placeholders function."""

    def test_no_placeholders(self):
        """Commands without placeholders return empty list."""
        result = parse_placeholders("echo hello")
        assert result == []

    def test_keyword_only_required(self):
        """Test {name} - keyword-only, required."""
        result = parse_placeholders("kubectl apply -f {file}")
        assert len(result) == 1
        assert result[0].name == "file"
        assert result[0].default is None
        assert result[0].positional is False

    def test_keyword_only_optional(self):
        """Test {name=default} - keyword-only, optional."""
        result = parse_placeholders("make -j{jobs=4}")
        assert len(result) == 1
        assert result[0].name == "jobs"
        assert result[0].default == "4"
        assert result[0].positional is False

    def test_positional_required(self):
        """Test {name:} - positional-able, required."""
        result = parse_placeholders("deploy {file:}")
        assert len(result) == 1
        assert result[0].name == "file"
        assert result[0].default is None
        assert result[0].positional is True

    def test_positional_optional(self):
        """Test {name:=default} - positional-able, optional."""
        result = parse_placeholders("pytest {path:=tests/}")
        assert len(result) == 1
        assert result[0].name == "path"
        assert result[0].default == "tests/"
        assert result[0].positional is True

    def test_empty_default(self):
        """Test empty default value."""
        result = parse_placeholders("cmd {arg=}")
        assert len(result) == 1
        assert result[0].default == ""

    def test_multiple_placeholders(self):
        """Test multiple placeholders in order."""
        result = parse_placeholders("kubectl apply -f {file:} -n {namespace:=default}")
        assert len(result) == 2
        assert result[0].name == "file"
        assert result[0].positional is True
        assert result[0].default is None
        assert result[1].name == "namespace"
        assert result[1].positional is True
        assert result[1].default == "default"

    def test_mixed_placeholder_types(self):
        """Test mix of keyword-only and positional placeholders."""
        result = parse_placeholders("make -j{jobs=4} {target:=all}")
        assert len(result) == 2
        assert result[0].name == "jobs"
        assert result[0].positional is False
        assert result[1].name == "target"
        assert result[1].positional is True


class TestExpandCommand:
    """Tests for expand_command function."""

    def test_no_placeholders(self):
        """Command without placeholders passes through."""
        result = expand_command("echo hello", {}, [])
        assert result == "echo hello"

    def test_keyword_only_with_named_arg(self):
        """Keyword-only placeholder filled by named arg."""
        result = expand_command("make -j{jobs=4}", {"jobs": "8"}, [])
        assert result == "make -j8"

    def test_keyword_only_with_default(self):
        """Keyword-only placeholder uses default when not provided."""
        result = expand_command("make -j{jobs=4}", {}, [])
        assert result == "make -j4"

    def test_positional_with_positional_arg(self):
        """Positional placeholder filled by positional arg."""
        result = expand_command("pytest {path:=tests/}", {}, ["unit/"])
        assert result == "pytest unit/"

    def test_positional_with_named_arg(self):
        """Positional placeholder can also be filled by named arg."""
        result = expand_command("pytest {path:=tests/}", {"path": "unit/"}, [])
        assert result == "pytest unit/"

    def test_positional_with_default(self):
        """Positional placeholder uses default when not provided."""
        result = expand_command("pytest {path:=tests/}", {}, [])
        assert result == "pytest tests/"

    def test_multiple_positional_args(self):
        """Multiple positional args fill placeholders in order."""
        result = expand_command(
            "kubectl apply -f {file:} -n {namespace:=default}",
            {},
            ["manifest.yaml", "prod"],
        )
        assert result == "kubectl apply -f manifest.yaml -n prod"

    def test_mixed_named_and_positional(self):
        """Named args take precedence, positional fills remaining."""
        result = expand_command(
            "kubectl apply -f {file:} -n {namespace:=default}",
            {"namespace": "staging"},
            ["manifest.yaml"],
        )
        assert result == "kubectl apply -f manifest.yaml -n staging"

    def test_extra_args_appended(self):
        """Extra args are appended to command."""
        result = expand_command("pytest {path:=tests/}", {}, ["unit/"], ["--verbose", "-x"])
        assert result == "pytest unit/ --verbose -x"

    def test_extra_positional_args_become_passthrough(self):
        """Positional args beyond placeholders become passthrough."""
        result = expand_command("pytest {path:=tests/}", {}, ["unit/", "--verbose", "-x"])
        assert result == "pytest unit/ --verbose -x"

    def test_required_missing_raises(self):
        """Missing required arg raises ValueError."""
        with pytest.raises(ValueError, match="Missing required argument 'file'"):
            expand_command("kubectl apply -f {file}", {}, [])

    def test_unknown_named_arg_raises(self):
        """Unknown named arg raises ValueError."""
        with pytest.raises(ValueError, match="Unknown argument 'unknown'"):
            expand_command("make -j{jobs=4}", {"unknown": "value"}, [])

    def test_keyword_only_not_filled_positionally(self):
        """Keyword-only placeholders are not filled by positional args."""
        result = expand_command("make -j{jobs=4} {target=all}", {}, ["clean"])
        # "clean" should be passthrough, not fill {jobs}
        assert result == "make -j4 all clean"


class TestParseCommandArgs:
    """Tests for _parse_command_args helper function."""

    def test_empty_args(self):
        """Empty args returns empty collections."""
        named, positional, extra = _parse_command_args([])
        assert named == {}
        assert positional == []
        assert extra == []

    def test_named_args(self):
        """Named args (key=value) are parsed correctly."""
        named, positional, extra = _parse_command_args(["jobs=8", "target=clean"])
        assert named == {"jobs": "8", "target": "clean"}
        assert positional == []
        assert extra == []

    def test_positional_args(self):
        """Positional args (no =) are collected."""
        named, positional, extra = _parse_command_args(["unit/", "integration/"])
        assert named == {}
        assert positional == ["unit/", "integration/"]
        assert extra == []

    def test_mixed_args(self):
        """Mixed named and positional args are separated."""
        named, positional, extra = _parse_command_args(["unit/", "jobs=8", "integration/"])
        assert named == {"jobs": "8"}
        assert positional == ["unit/", "integration/"]
        assert extra == []

    def test_separator_splits_extra(self):
        """:: separator splits extra args."""
        named, positional, extra = _parse_command_args(["unit/", "::", "--verbose", "-x"])
        assert named == {}
        assert positional == ["unit/"]
        assert extra == ["--verbose", "-x"]

    def test_separator_with_named_args(self):
        """:: works with named args too."""
        named, positional, extra = _parse_command_args(["jobs=8", "::", "--dry-run"])
        assert named == {"jobs": "8"}
        assert positional == []
        assert extra == ["--dry-run"]

    def test_positional_limit(self):
        """Positional limit restricts placeholder args."""
        named, positional, extra = _parse_command_args(
            ["unit/", "integration/", "--verbose"],
            positional_limit=1,
        )
        assert named == {}
        assert positional == ["unit/"]
        assert extra == ["integration/", "--verbose"]

    def test_positional_limit_zero(self):
        """Positional limit of 0 sends all to extra."""
        named, positional, extra = _parse_command_args(
            ["unit/", "--verbose"],
            positional_limit=0,
        )
        assert named == {}
        assert positional == []
        assert extra == ["unit/", "--verbose"]

    def test_flag_like_args_are_positional(self):
        """Args starting with - are treated as positional."""
        named, positional, extra = _parse_command_args(["--verbose", "-x"])
        assert named == {}
        assert positional == ["--verbose", "-x"]
        assert extra == []

    def test_value_with_equals(self):
        """Values containing = are handled correctly."""
        named, positional, extra = _parse_command_args(["filter=name=foo"])
        assert named == {"filter": "name=foo"}


class TestFormatCommandHelp:
    """Tests for format_command_help function."""

    def test_simple_command(self):
        """Simple command without placeholders."""
        cmd = RegisteredCommand(name="build", cmd="make", description="Build the project")
        result = format_command_help(cmd)
        assert "build: make" in result
        assert "Build the project" in result

    def test_command_with_placeholders(self):
        """Command with placeholders shows argument info."""
        cmd = RegisteredCommand(
            name="deploy",
            cmd="kubectl apply -f {file:} -n {namespace:=default}",
            description="Deploy to Kubernetes",
        )
        result = format_command_help(cmd)
        assert "deploy:" in result
        assert "file" in result
        assert "required" in result
        assert "namespace" in result
        assert "default: default" in result
