import pytest
from click.testing import CliRunner

from tpch_runner.commands import power_commands


@pytest.mark.parametrize(
    "visible_command",
    [
        "compare",
        "delete",
        "draw",
        "list",
        "multi",
        "show",
        "validate",
        "update",
    ],
)
def test_visible_command(visible_command):
    """Test visible commands in help message"""
    runner = CliRunner()
    result = runner.invoke(power_commands.cli, ["--help"])
    print(result.output)
    assert result.exit_code == 0
    assert visible_command in result.output
