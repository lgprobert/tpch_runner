from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tpch_runner.commands import base_commands


@pytest.mark.parametrize(
    "visible_command",
    ["db", "generate", "power", "result", "run", "version"],
)
def test_visible_command(visible_command):
    """Test visible commands in help message"""
    runner = CliRunner()
    result = runner.invoke(base_commands.cli, ["--help"])
    print(result.output)
    assert result.exit_code == 0
    assert visible_command in result.output


def test_verbose_enabled(caplog):
    """Test that '-v' enables verbose logging."""
    runner = CliRunner()
    result = runner.invoke(base_commands.cli, ["-v", "version"])

    assert result.exit_code == 0
    log_messages = "\n".join(record.message for record in caplog.records)
    assert "Verbose mode is on" in log_messages


def test_verbose_disabled(caplog):
    """Test that verbose mode is off by default."""
    runner = CliRunner()
    result = runner.invoke(base_commands.cli, ["version"])

    assert result.exit_code == 0
    log_messages = "\n".join(record.message for record in caplog.records)
    assert "Verbose mode is on" not in log_messages


def test_version():
    runner = CliRunner()
    result = runner.invoke(base_commands.version)
    assert result.exit_code == 0
    assert "TPC-H Runner v1.0" in result.output


@pytest.mark.parametrize(
    "scale, table, mock_return, mock_log, expected_exit, expected_output",
    [
        ("1", "all", (True, None), None, 0, "succeeds, output: done"),
        (
            "2",
            "orders",
            (False, "generation fails"),
            "Data generation failed.",
            1,
            "dbgen fails, error:",
        ),
        (
            "3",
            "region",
            (False, None),
            "Data generation failed.",
            1,
            "dbgen fails, error: n/a",
        ),
        (
            "4",
            "invalid_table",
            (False, None),
            "Invalid table invalid_table",
            1,
            "",
        ),
    ],
)
def test_generate(
    scale, table, mock_return, mock_log, expected_exit, expected_output, caplog
):
    runner = CliRunner()

    with patch(
        "tpch_runner.commands.base_commands.data_gen_batch", return_value=mock_return
    ):
        result = runner.invoke(base_commands.generate, ["-s", scale, "-t", table])

    assert result.exit_code == expected_exit

    if mock_log:
        log_messages = "\n".join(record.message for record in caplog.records)
        assert mock_log in log_messages

        assert expected_output in result.output
